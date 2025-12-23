/**
 * ADDI Scraper Microservice - Node.js/Fastify
 * Despliega en EasyPanel como servicio Node
 *
 * Este microservicio extrae transacciones de ADDI y las devuelve a GigaMovil.
 * Versión mejorada con: reintentos, validación, webhooks, métricas y rate limiting.
 */

import Fastify from 'fastify';
import cors from '@fastify/cors';
import puppeteer from 'puppeteer';

const fastify = Fastify({ logger: true });

// CORS
await fastify.register(cors, { origin: '*' });

// ============================================================
// CONFIGURACIÓN
// ============================================================
const CONFIG = {
    maxRetries: 3,
    retryBaseDelayMs: 1000,
    requestTimeoutMs: 300000, // 5 minutos máximo por scraping
    rateLimitWindowMs: 60000, // 1 minuto
    rateLimitMaxRequests: 10, // 10 requests por minuto
    browserPoolSize: 1, // Browsers en pool (1 para servidores pequeños)
    pageTimeoutMs: 30000
};

// ============================================================
// RATE LIMITER
// ============================================================
class RateLimiter {
    constructor(windowMs, maxRequests) {
        this.windowMs = windowMs;
        this.maxRequests = maxRequests;
        this.requests = new Map();
    }

    isAllowed(clientId) {
        const now = Date.now();
        const clientRequests = this.requests.get(clientId) || [];

        // Limpiar requests antiguos
        const validRequests = clientRequests.filter(time => now - time < this.windowMs);

        if (validRequests.length >= this.maxRequests) {
            return { allowed: false, retryAfter: Math.ceil((validRequests[0] + this.windowMs - now) / 1000) };
        }

        validRequests.push(now);
        this.requests.set(clientId, validRequests);
        return { allowed: true, remaining: this.maxRequests - validRequests.length };
    }

    cleanup() {
        const now = Date.now();
        for (const [clientId, requests] of this.requests.entries()) {
            const valid = requests.filter(time => now - time < this.windowMs);
            if (valid.length === 0) {
                this.requests.delete(clientId);
            } else {
                this.requests.set(clientId, valid);
            }
        }
    }
}

const rateLimiter = new RateLimiter(CONFIG.rateLimitWindowMs, CONFIG.rateLimitMaxRequests);

// Limpiar rate limiter cada minuto
setInterval(() => rateLimiter.cleanup(), 60000);

// ============================================================
// MÉTRICAS
// ============================================================
const metrics = {
    totalRequests: 0,
    successfulScrapes: 0,
    failedScrapes: 0,
    totalTransactionsExtracted: 0,
    averageExecutionTimeMs: 0,
    executionTimes: [],
    lastErrors: [],
    webhooksSent: 0,
    webhooksFailed: 0,
    startTime: Date.now()
};

function updateMetrics(executionTimeMs, success, transactionsCount, error = null) {
    metrics.totalRequests++;

    if (success) {
        metrics.successfulScrapes++;
        metrics.totalTransactionsExtracted += transactionsCount;
    } else {
        metrics.failedScrapes++;
        if (error) {
            metrics.lastErrors.push({
                timestamp: new Date().toISOString(),
                error: error.substring(0, 500)
            });
            // Mantener solo últimos 20 errores
            if (metrics.lastErrors.length > 20) {
                metrics.lastErrors.shift();
            }
        }
    }

    // Calcular promedio de tiempo de ejecución (últimos 50)
    metrics.executionTimes.push(executionTimeMs);
    if (metrics.executionTimes.length > 50) {
        metrics.executionTimes.shift();
    }
    metrics.averageExecutionTimeMs = Math.round(
        metrics.executionTimes.reduce((a, b) => a + b, 0) / metrics.executionTimes.length
    );
}

// ============================================================
// WEBHOOK SERVICE
// ============================================================
async function sendWebhook(url, data, retries = 3) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 10000);

            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data),
                signal: controller.signal
            });

            clearTimeout(timeout);

            if (response.ok) {
                metrics.webhooksSent++;
                return { success: true, status: response.status };
            }

            if (attempt === retries) {
                metrics.webhooksFailed++;
                return { success: false, status: response.status, error: 'Max retries reached' };
            }
        } catch (e) {
            if (attempt === retries) {
                metrics.webhooksFailed++;
                return { success: false, error: e.message };
            }
            await new Promise(r => setTimeout(r, 1000 * attempt));
        }
    }
}

// ============================================================
// VALIDACIÓN DE SCHEMAS
// ============================================================
const scrapeSchema = {
    body: {
        type: 'object',
        required: ['email', 'password'],
        properties: {
            email: { type: 'string', format: 'email', minLength: 5 },
            password: { type: 'string', minLength: 4 },
            status_filter: { type: 'string', enum: ['APPROVED', 'PENDING', 'REJECTED', 'CANCELLED', 'EXPIRED', null] },
            limit: { type: 'integer', minimum: 10, maximum: 500, default: 100 },
            webhook_url: { type: 'string', format: 'uri' },
            timeout_ms: { type: 'integer', minimum: 30000, maximum: 600000 }
        }
    }
};

// ============================================================
// ESTADO GLOBAL
// ============================================================
const scrapingStatus = {
    running: false,
    progress: 0,
    total: 0,
    extracted: 0,
    lastRun: null,
    error: null,
    errorsLog: [],
    jobId: null,
    canCancel: false
};

let cachedTransactions = [];
let currentAbortController = null;

// ============================================================
// CLASE ADDI SCRAPER MEJORADA
// ============================================================
class AddiScraper {
    constructor(email, password, statusFilter = null, options = {}) {
        this.email = email;
        this.password = password;
        this.statusFilter = statusFilter;
        this.browser = null;
        this.page = null;
        this.apiBase = 'https://ally-portal-external-api.addi.com/v1/transactions';
        this.totalRecords = null;
        this.errorsLog = [];
        this.abortController = options.abortController || null;
        this.maxRetries = options.maxRetries || CONFIG.maxRetries;
    }

    get isAborted() {
        return this.abortController?.signal?.aborted || false;
    }

    checkAbort() {
        if (this.isAborted) {
            throw new Error('Scraping cancelado por el usuario');
        }
    }

    async wait(ms) {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(resolve, ms);
            if (this.abortController) {
                this.abortController.signal.addEventListener('abort', () => {
                    clearTimeout(timeout);
                    reject(new Error('Scraping cancelado'));
                }, { once: true });
            }
        });
    }

    log(level, message, data = null) {
        const timestamp = new Date().toISOString();
        let logEntry = `[${timestamp}] [${level}] ${message}`;
        if (data) {
            logEntry += ` | Data: ${JSON.stringify(data)}`;
        }
        console.log(logEntry);

        if (level === 'ERROR') {
            this.errorsLog.push(logEntry);
        }
    }

    async withRetry(operation, operationName, maxRetries = this.maxRetries) {
        let lastError;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            this.checkAbort();

            try {
                return await operation();
            } catch (e) {
                lastError = e;
                this.log('WARN', `${operationName} falló (intento ${attempt}/${maxRetries}): ${e.message}`);

                if (attempt < maxRetries) {
                    const delay = CONFIG.retryBaseDelayMs * Math.pow(2, attempt - 1);
                    this.log('INFO', `Reintentando en ${delay}ms...`);
                    await this.wait(delay);
                }
            }
        }

        throw lastError;
    }

    async setupBrowser() {
        return this.withRetry(async () => {
            this.browser = await puppeteer.launch({
                headless: 'new',
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-plugins'
                ]
            });

            this.page = await this.browser.newPage();

            // Optimización: bloquear recursos innecesarios
            await this.page.setRequestInterception(true);
            this.page.on('request', (req) => {
                const resourceType = req.resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                    req.abort();
                } else {
                    req.continue();
                }
            });

            // Ocultar detección de automatización
            await this.page.evaluateOnNewDocument(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            });

            await this.page.setViewport({ width: 1920, height: 1080 });

            this.log('INFO', 'Browser inicializado');
            return true;
        }, 'setupBrowser', 2);
    }

    async closeModals() {
        const modalTexts = ['Omitir', 'Finalizar', 'Entendido', 'Listo', 'Cerrar', 'Siguiente'];

        for (let i = 0; i < 8; i++) {
            this.checkAbort();
            let found = false;
            try {
                const buttons = await this.page.$$('button');
                for (const btn of buttons) {
                    try {
                        const isVisible = await btn.isIntersectingViewport();
                        if (isVisible) {
                            const text = await btn.evaluate(el => el.textContent.trim());
                            if (modalTexts.includes(text)) {
                                await btn.click();
                                found = true;
                                await this.wait(1000);
                                break;
                            }
                        }
                    } catch { continue; }
                }
            } catch { break; }
            if (!found) break;
        }
    }

    async login() {
        return this.withRetry(async () => {
            this.log('INFO', `Iniciando login para: ${this.email}`);
            await this.page.goto('https://aliados.addi.com/', {
                waitUntil: 'networkidle2',
                timeout: CONFIG.pageTimeoutMs
            });

            await this.wait(3000);
            this.log('INFO', `URL actual: ${this.page.url()}`);

            // Verificar si ya estamos en auth
            const currentUrl = this.page.url();
            if (!currentUrl.includes('auth')) {
                this.log('INFO', 'Esperando redirección a auth...');
                try {
                    await this.page.waitForFunction(
                        () => window.location.href.includes('auth'),
                        { timeout: 15000 }
                    );
                } catch (e) {
                    this.log('INFO', 'No hubo redirección automática, continuando...');
                }
            }

            await this.wait(3000);
            this.checkAbort();

            // Buscar campo email
            const emailSelectors = [
                "input[name='email']",
                "input[type='email']",
                "input[id='email']",
                "input[placeholder*='email' i]",
                "input[placeholder*='correo' i]",
                ".auth0-lock-input[name='email']",
                "input.auth0-lock-input"
            ];

            let emailInput = null;
            for (const selector of emailSelectors) {
                try {
                    emailInput = await this.page.waitForSelector(selector, { visible: true, timeout: 5000 });
                    if (emailInput) {
                        this.log('INFO', `Campo email encontrado: ${selector}`);
                        break;
                    }
                } catch { continue; }
            }

            if (!emailInput) {
                throw new Error('No se encontró campo de email');
            }

            await emailInput.click();
            await this.wait(200);
            await emailInput.type(this.email, { delay: 50 });
            this.log('INFO', 'Email ingresado');

            // Buscar campo password
            const passSelectors = [
                "input[name='password']",
                "input[type='password']",
                "input[id='password']"
            ];

            let passInput = null;
            for (const selector of passSelectors) {
                try {
                    passInput = await this.page.waitForSelector(selector, { visible: true, timeout: 5000 });
                    if (passInput) {
                        this.log('INFO', `Campo password encontrado: ${selector}`);
                        break;
                    }
                } catch { continue; }
            }

            if (!passInput) {
                throw new Error('No se encontró campo de password');
            }

            await passInput.click();
            await this.wait(200);
            await passInput.type(this.password, { delay: 50 });
            this.log('INFO', 'Password ingresado');

            this.checkAbort();

            // Buscar botón submit
            const submitSelectors = [
                'button.auth0-lock-submit',
                'button[type="submit"]',
                'button[name="submit"]',
                'input[type="submit"]'
            ];

            let submitted = false;
            for (const selector of submitSelectors) {
                try {
                    const btn = await this.page.$(selector);
                    if (btn) {
                        await btn.click();
                        this.log('INFO', `Submit: ${selector}`);
                        submitted = true;
                        break;
                    }
                } catch { continue; }
            }

            if (!submitted) {
                await this.page.keyboard.press('Enter');
                this.log('INFO', 'Submit con Enter');
            }

            this.log('INFO', 'Formulario enviado, esperando respuesta...');
            await this.wait(5000);

            try {
                await this.page.waitForFunction(
                    () => window.location.href.includes('aliados.addi.com') &&
                          !window.location.href.includes('auth'),
                    { timeout: 25000 }
                );
            } catch (e) {
                this.log('ERROR', `Login no completado. URL: ${this.page.url()}`);
                throw new Error(`Login falló. URL final: ${this.page.url()}`);
            }

            await this.wait(2000);
            await this.closeModals();
            this.log('INFO', 'Login exitoso');
            return true;
        }, 'login', 2);
    }

    async fetchApiPage(limit, offset) {
        const url = `${this.apiBase}?limit=${limit}&offset=${offset}`;

        return this.withRetry(async () => {
            this.checkAbort();
            await this.page.goto(url, { waitUntil: 'networkidle2', timeout: CONFIG.pageTimeoutMs });
            await this.wait(1000);

            let text;
            try {
                text = await this.page.$eval('pre', el => el.textContent);
            } catch {
                text = await this.page.$eval('body', el => el.textContent);
            }

            if (!text || !text.trim()) {
                return { transactions: [], total: 0 };
            }

            const data = JSON.parse(text);
            this.log('INFO', `API offset=${offset}: OK`, {
                count: Array.isArray(data) ? data.length : (data.total || 'N/A')
            });
            return data;
        }, `fetchApiPage(offset=${offset})`, 3);
    }

    parseTransaction(tx) {
        let imei = null;
        const orderId = String(tx.orderId || '');
        const productDesc = String(tx.productDescription || '');

        if (orderId && /^\d+$/.test(orderId) && orderId.length >= 15) {
            imei = orderId;
        } else if (productDesc) {
            const imeiMatch = productDesc.match(/\b\d{15}\b/);
            if (imeiMatch) {
                imei = imeiMatch[0];
            }
        }

        return {
            transactionId: tx.transactionId || tx.id,
            status: tx.status,
            clientName: tx.clientName || tx.customerName,
            nationalIdNumber: tx.nationalIdNumber || tx.documentNumber,
            phoneNumber: tx.phoneNumber || tx.phone,
            storeUserEmail: tx.storeUserEmail || tx.email,
            amount: tx.amount || tx.loanAmount,
            downPayment: tx.downPayment || 0,
            installments: tx.installments || tx.term,
            installmentAmount: tx.installmentAmount || tx.monthlyPayment,
            orderId,
            imei,
            productDescription: productDesc,
            productName: tx.productName || productDesc,
            createdAt: tx.createdAt || tx.created_at,
            approvedAt: tx.approvedAt || tx.approved_at,
            firstPaymentDate: tx.firstPaymentDate,
            allyId: tx.allyId,
            storeId: tx.storeId,
            storeName: tx.storeName,
            loanId: tx.loanId,
            contractId: tx.contractId
        };
    }

    async extractAll(limit = 100) {
        const allTransactions = [];
        let offset = 0;

        this.log('INFO', `Iniciando extracción (limit=${limit}, filter=${this.statusFilter})`);

        // Primera página
        let data = await this.fetchApiPage(limit, 0);

        let transactions, total;
        if (Array.isArray(data)) {
            transactions = data;
            total = null;
        } else {
            total = data.total || data.count || data.totalCount;
            transactions = data.transactions || data.data || data.items || [];
        }

        if (total) {
            this.totalRecords = total;
            scrapingStatus.total = total;
            this.log('INFO', `Total registros en ADDI: ${total}`);
        }

        if (transactions.length > 0) {
            this.log('INFO', `Primera página: ${transactions.length} transacciones`);
            for (const tx of transactions) {
                const parsed = this.parseTransaction(tx);
                if (this.statusFilter) {
                    if (parsed.status === this.statusFilter) {
                        allTransactions.push(parsed);
                    }
                } else {
                    allTransactions.push(parsed);
                }
            }
            offset = limit;
        }

        scrapingStatus.extracted = allTransactions.length;

        // Páginas siguientes
        let consecutiveEmpty = 0;

        while (true) {
            this.checkAbort();

            if (this.totalRecords && offset >= this.totalRecords) {
                this.log('INFO', `Alcanzado total (${this.totalRecords})`);
                break;
            }

            data = await this.fetchApiPage(limit, offset);

            if (Array.isArray(data)) {
                transactions = data;
            } else {
                transactions = data.transactions || data.data || data.items || [];
            }

            if (transactions.length === 0) {
                consecutiveEmpty++;
                this.log('INFO', `Página vacía #${consecutiveEmpty} offset=${offset}`);
                if (consecutiveEmpty >= 2) {
                    this.log('INFO', '2 páginas vacías, terminando');
                    break;
                }
                offset += limit;
                continue;
            }

            consecutiveEmpty = 0;
            this.log('INFO', `Página offset=${offset}: ${transactions.length} transacciones`);

            for (const tx of transactions) {
                const parsed = this.parseTransaction(tx);
                if (this.statusFilter) {
                    if (parsed.status === this.statusFilter) {
                        allTransactions.push(parsed);
                    }
                } else {
                    allTransactions.push(parsed);
                }
            }

            scrapingStatus.extracted = allTransactions.length;

            if (this.totalRecords) {
                scrapingStatus.progress = Math.floor((offset / this.totalRecords) * 100);
            }

            if (transactions.length < limit) {
                this.log('INFO', `Última página (${transactions.length} < ${limit})`);
                break;
            }

            offset += limit;
            await this.wait(300);
        }

        scrapingStatus.progress = 100;
        this.log('INFO', `Extracción completada: ${allTransactions.length} transacciones`);

        return allTransactions;
    }

    async run(limit = 100) {
        try {
            if (!await this.setupBrowser()) {
                return [[], this.errorsLog];
            }

            if (!await this.login()) {
                return [[], this.errorsLog];
            }

            const transactions = await this.extractAll(limit);
            return [transactions, this.errorsLog];

        } catch (e) {
            this.log('ERROR', `Error general: ${e.message}`);
            return [[], this.errorsLog];
        } finally {
            if (this.browser) {
                try {
                    await this.browser.close();
                } catch { }
            }
        }
    }
}

// ============================================================
// MIDDLEWARE RATE LIMITING
// ============================================================
fastify.addHook('preHandler', async (request, reply) => {
    // Solo aplicar a endpoints de scraping
    if (request.url.startsWith('/scrape')) {
        const clientId = request.ip || 'unknown';
        const result = rateLimiter.isAllowed(clientId);

        if (!result.allowed) {
            reply.code(429).send({
                error: 'Rate limit exceeded',
                retry_after_seconds: result.retryAfter,
                message: `Demasiadas solicitudes. Intenta de nuevo en ${result.retryAfter} segundos.`
            });
            return;
        }

        reply.header('X-RateLimit-Remaining', result.remaining);
    }
});

// ============================================================
// ENDPOINTS
// ============================================================

fastify.get('/', async () => ({
    service: 'ADDI Scraper API for GigaMovil',
    version: '2.0.0-nodejs',
    status: 'running',
    runtime: 'Node.js',
    features: ['reintentos', 'webhooks', 'métricas', 'rate-limiting', 'cancelación'],
    endpoints: {
        'GET /health': 'Estado del servicio',
        'GET /metrics': 'Métricas y estadísticas',
        'POST /scrape': 'Scraping síncrono (espera resultado)',
        'POST /scrape/async': 'Scraping en background',
        'GET /scrape/status': 'Estado del scraping async',
        'POST /scrape/cancel': 'Cancelar scraping en progreso',
        'GET /transactions': 'Transacciones en cache',
        'GET /transactions/export': 'Exportar transacciones (JSON/CSV)',
        'GET /debug/last-errors': 'Últimos errores'
    }
}));

fastify.get('/health', async () => ({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    runtime: 'Node.js + Puppeteer',
    version: '2.0.0',
    uptime_seconds: Math.round((Date.now() - metrics.startTime) / 1000),
    scrapingRunning: scrapingStatus.running,
    cachedTransactions: cachedTransactions.length,
    lastRun: scrapingStatus.lastRun,
    lastError: scrapingStatus.error
}));

fastify.get('/metrics', async () => ({
    uptime_seconds: Math.round((Date.now() - metrics.startTime) / 1000),
    total_requests: metrics.totalRequests,
    successful_scrapes: metrics.successfulScrapes,
    failed_scrapes: metrics.failedScrapes,
    success_rate: metrics.totalRequests > 0
        ? Math.round((metrics.successfulScrapes / metrics.totalRequests) * 100)
        : 0,
    total_transactions_extracted: metrics.totalTransactionsExtracted,
    average_execution_time_ms: metrics.averageExecutionTimeMs,
    webhooks_sent: metrics.webhooksSent,
    webhooks_failed: metrics.webhooksFailed,
    last_errors: metrics.lastErrors.slice(-5),
    cached_transactions: cachedTransactions.length,
    scraping_running: scrapingStatus.running
}));

fastify.post('/scrape', { schema: scrapeSchema }, async (request) => {
    const {
        email,
        password,
        status_filter: statusFilter,
        limit = 100,
        webhook_url: webhookUrl,
        timeout_ms: timeoutMs = CONFIG.requestTimeoutMs
    } = request.body;

    if (scrapingStatus.running) {
        return {
            success: false,
            total: 0,
            transactions: [],
            error: 'Ya hay un scraping en progreso. Usa POST /scrape/cancel para cancelar.',
            job_id: scrapingStatus.jobId
        };
    }

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
    currentAbortController = new AbortController();

    scrapingStatus.running = true;
    scrapingStatus.jobId = jobId;
    scrapingStatus.canCancel = true;
    scrapingStatus.error = null;
    scrapingStatus.errorsLog = [];
    scrapingStatus.progress = 0;
    scrapingStatus.extracted = 0;

    const startTime = Date.now();

    // Timeout global
    const timeoutId = setTimeout(() => {
        if (currentAbortController) {
            currentAbortController.abort();
        }
    }, timeoutMs);

    try {
        const scraper = new AddiScraper(email, password, statusFilter, {
            abortController: currentAbortController,
            maxRetries: CONFIG.maxRetries
        });

        const [transactions, errors] = await scraper.run(limit);

        clearTimeout(timeoutId);

        cachedTransactions = transactions;
        scrapingStatus.lastRun = new Date().toISOString();
        scrapingStatus.errorsLog = errors;
        scrapingStatus.extracted = transactions.length;

        const executionTime = Date.now() - startTime;
        const success = transactions.length > 0 || errors.length === 0;

        if (!success && errors.length > 0) {
            scrapingStatus.error = errors[errors.length - 1];
        }

        // Actualizar métricas
        updateMetrics(executionTime, success, transactions.length, success ? null : errors[errors.length - 1]);

        const result = {
            success,
            job_id: jobId,
            total: transactions.length,
            filter: statusFilter,
            transactions,
            error: errors.length > 0 && transactions.length === 0 ? errors[errors.length - 1] : null,
            errors_log: errors,
            execution_time_ms: executionTime,
            execution_time: Math.round(executionTime / 10) / 100
        };

        // Enviar webhook si está configurado
        if (webhookUrl) {
            setImmediate(async () => {
                await sendWebhook(webhookUrl, {
                    event: 'scrape_completed',
                    job_id: jobId,
                    success,
                    total: transactions.length,
                    execution_time_ms: executionTime,
                    timestamp: new Date().toISOString()
                });
            });
        }

        return result;

    } catch (e) {
        clearTimeout(timeoutId);
        const executionTime = Date.now() - startTime;
        const errorMsg = e.message.includes('cancelado')
            ? 'Scraping cancelado por el usuario'
            : `Error fatal: ${e.message}`;

        scrapingStatus.error = errorMsg;
        updateMetrics(executionTime, false, 0, errorMsg);

        // Webhook de error
        if (webhookUrl) {
            setImmediate(async () => {
                await sendWebhook(webhookUrl, {
                    event: 'scrape_failed',
                    job_id: jobId,
                    error: errorMsg,
                    timestamp: new Date().toISOString()
                });
            });
        }

        return {
            success: false,
            job_id: jobId,
            total: 0,
            transactions: [],
            error: errorMsg,
            errors_log: [errorMsg],
            execution_time_ms: executionTime
        };
    } finally {
        scrapingStatus.running = false;
        scrapingStatus.canCancel = false;
        currentAbortController = null;
    }
});

fastify.post('/scrape/async', { schema: scrapeSchema }, async (request) => {
    const {
        email,
        password,
        status_filter: statusFilter,
        limit = 100,
        webhook_url: webhookUrl
    } = request.body;

    if (scrapingStatus.running) {
        return {
            success: false,
            error: 'Ya hay un scraping en progreso',
            job_id: scrapingStatus.jobId
        };
    }

    const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
    currentAbortController = new AbortController();

    scrapingStatus.running = true;
    scrapingStatus.jobId = jobId;
    scrapingStatus.canCancel = true;
    scrapingStatus.error = null;
    scrapingStatus.progress = 0;

    // Ejecutar en background
    setImmediate(async () => {
        const startTime = Date.now();

        try {
            const scraper = new AddiScraper(email, password, statusFilter, {
                abortController: currentAbortController,
                maxRetries: CONFIG.maxRetries
            });

            const [transactions, errors] = await scraper.run(limit);

            cachedTransactions = transactions;
            scrapingStatus.lastRun = new Date().toISOString();
            scrapingStatus.errorsLog = errors;
            scrapingStatus.extracted = transactions.length;

            const executionTime = Date.now() - startTime;
            const success = transactions.length > 0 || errors.length === 0;

            if (errors.length > 0 && transactions.length === 0) {
                scrapingStatus.error = errors[errors.length - 1];
            }

            updateMetrics(executionTime, success, transactions.length, success ? null : errors[errors.length - 1]);

            if (webhookUrl) {
                await sendWebhook(webhookUrl, {
                    event: success ? 'scrape_completed' : 'scrape_failed',
                    job_id: jobId,
                    success,
                    total: transactions.length,
                    execution_time_ms: executionTime,
                    timestamp: new Date().toISOString()
                });
            }
        } catch (e) {
            const executionTime = Date.now() - startTime;
            scrapingStatus.error = e.message;
            updateMetrics(executionTime, false, 0, e.message);

            if (webhookUrl) {
                await sendWebhook(webhookUrl, {
                    event: 'scrape_failed',
                    job_id: jobId,
                    error: e.message,
                    timestamp: new Date().toISOString()
                });
            }
        } finally {
            scrapingStatus.running = false;
            scrapingStatus.canCancel = false;
            currentAbortController = null;
        }
    });

    return {
        success: true,
        job_id: jobId,
        message: 'Scraping iniciado en background',
        check_status: '/scrape/status',
        webhook_configured: !!webhookUrl
    };
});

fastify.post('/scrape/cancel', async () => {
    if (!scrapingStatus.running) {
        return {
            success: false,
            error: 'No hay scraping en progreso'
        };
    }

    if (!scrapingStatus.canCancel || !currentAbortController) {
        return {
            success: false,
            error: 'El scraping actual no puede ser cancelado'
        };
    }

    currentAbortController.abort();

    return {
        success: true,
        message: 'Señal de cancelación enviada',
        job_id: scrapingStatus.jobId
    };
});

fastify.get('/scrape/status', async () => ({
    ...scrapingStatus,
    cached_count: cachedTransactions.length
}));

fastify.get('/transactions', async (request) => {
    let { status, limit, offset = 0 } = request.query;
    let transactions = cachedTransactions;

    if (status) {
        transactions = transactions.filter(tx => tx.status === status);
    }

    const total = transactions.length;

    if (offset) {
        transactions = transactions.slice(parseInt(offset));
    }

    if (limit) {
        transactions = transactions.slice(0, parseInt(limit));
    }

    return {
        success: true,
        total,
        returned: transactions.length,
        offset: parseInt(offset) || 0,
        last_run: scrapingStatus.lastRun,
        transactions
    };
});

fastify.get('/transactions/export', async (request, reply) => {
    const { format = 'json', status } = request.query;
    let transactions = cachedTransactions;

    if (status) {
        transactions = transactions.filter(tx => tx.status === status);
    }

    if (format === 'csv') {
        if (transactions.length === 0) {
            return reply.type('text/csv').send('No hay transacciones');
        }

        const headers = Object.keys(transactions[0]);
        const csv = [
            headers.join(','),
            ...transactions.map(tx =>
                headers.map(h => {
                    const val = tx[h];
                    if (val === null || val === undefined) return '';
                    const str = String(val);
                    return str.includes(',') || str.includes('"')
                        ? `"${str.replace(/"/g, '""')}"`
                        : str;
                }).join(',')
            )
        ].join('\n');

        reply.header('Content-Disposition', 'attachment; filename=transactions.csv');
        return reply.type('text/csv').send(csv);
    }

    return {
        success: true,
        total: transactions.length,
        exported_at: new Date().toISOString(),
        transactions
    };
});

fastify.get('/debug/last-errors', async () => ({
    running: scrapingStatus.running,
    lastRun: scrapingStatus.lastRun,
    error: scrapingStatus.error,
    errorsLog: scrapingStatus.errorsLog,
    extracted: scrapingStatus.extracted,
    total: scrapingStatus.total,
    metrics_errors: metrics.lastErrors
}));

// ============================================================
// INICIAR SERVIDOR
// ============================================================
const port = process.env.PORT || 8000;
try {
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`🚀 ADDI Scraper API v2.0.0 running on port ${port}`);
    console.log(`📊 Features: reintentos, webhooks, métricas, rate-limiting, cancelación`);
} catch (err) {
    fastify.log.error(err);
    process.exit(1);
}
