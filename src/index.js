/**
 * ADDI Scraper Microservice - Node.js/Fastify
 * Despliega en EasyPanel como servicio Node
 * 
 * Este microservicio extrae transacciones de ADDI y las devuelve a GigaMovil.
 * Incluye logging detallado de errores para debug.
 */

import Fastify from 'fastify';
import cors from '@fastify/cors';
import puppeteer from 'puppeteer';

const fastify = Fastify({ logger: true });

// CORS
await fastify.register(cors, { origin: '*' });

// Estado global
const scrapingStatus = {
    running: false,
    progress: 0,
    total: 0,
    extracted: 0,
    lastRun: null,
    error: null,
    errorsLog: []
};

let cachedTransactions = [];

/**
 * Clase AddiScraper - maneja todo el proceso de scraping
 */
class AddiScraper {
    constructor(email, password, statusFilter = null) {
        this.email = email;
        this.password = password;
        this.statusFilter = statusFilter;
        this.browser = null;
        this.page = null;
        this.apiBase = 'https://ally-portal-external-api.addi.com/v1/transactions';
        this.totalRecords = null;
        this.errorsLog = [];
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

    async setupBrowser() {
        try {
            this.browser = await puppeteer.launch({
                headless: 'new',
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--disable-blink-features=AutomationControlled'
                ]
            });

            this.page = await this.browser.newPage();
            
            // Ocultar detección de automatización
            await this.page.evaluateOnNewDocument(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            });

            await this.page.setViewport({ width: 1920, height: 1080 });
            
            this.log('INFO', 'Browser inicializado');
            return true;
        } catch (e) {
            this.log('ERROR', `Error inicializando browser: ${e.message}`);
            return false;
        }
    }

    async typeSlowly(selector, text) {
        await this.page.click(selector);
        await this.page.waitForTimeout(150);
        
        // Limpiar campo
        await this.page.evaluate((sel) => {
            document.querySelector(sel).value = '';
        }, selector);

        // Escribir caracter por caracter
        for (const char of text) {
            await this.page.type(selector, char, { delay: 30 });
        }
    }

    async closeModals() {
        const modalTexts = ['Omitir', 'Finalizar', 'Entendido', 'Listo', 'Cerrar', 'Siguiente'];
        
        for (let i = 0; i < 8; i++) {
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
                                await this.page.waitForTimeout(1000);
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
        try {
            this.log('INFO', `Iniciando login para: ${this.email}`);
            await this.page.goto('https://aliados.addi.com/', { waitUntil: 'networkidle2' });

            // Esperar redirección a auth
            await this.page.waitForFunction(
                () => window.location.href.includes('auth.addi.com'),
                { timeout: 20000 }
            );
            this.log('INFO', 'Redirigido a auth.addi.com');

            await this.page.waitForTimeout(4000);
            await this.page.waitForSelector('.auth0-lock-widget', { timeout: 15000 });
            await this.page.waitForTimeout(1000);

            // Email
            await this.page.waitForSelector("input[name='email']", { visible: true });
            await this.typeSlowly("input[name='email']", this.email);
            this.log('INFO', 'Email ingresado');

            // Password
            await this.page.waitForSelector("input[name='password']", { visible: true });
            await this.typeSlowly("input[name='password']", this.password);
            this.log('INFO', 'Password ingresado');

            await this.page.waitForTimeout(300);
            await this.page.click('button.auth0-lock-submit');
            this.log('INFO', 'Formulario enviado');

            // Esperar login exitoso
            await this.page.waitForFunction(
                () => window.location.href.includes('aliados.addi.com') && 
                      !window.location.href.includes('auth'),
                { timeout: 30000 }
            );

            await this.page.waitForTimeout(2000);
            await this.closeModals();
            this.log('INFO', 'Login exitoso');
            return true;

        } catch (e) {
            this.log('ERROR', `Error en login: ${e.message}`);
            return false;
        }
    }

    async fetchApiPage(limit, offset) {
        const url = `${this.apiBase}?limit=${limit}&offset=${offset}`;

        try {
            await this.page.goto(url, { waitUntil: 'networkidle2' });
            await this.page.waitForTimeout(1500);

            // Extraer JSON
            let text;
            try {
                text = await this.page.$eval('pre', el => el.textContent);
            } catch {
                text = await this.page.$eval('body', el => el.textContent);
            }

            if (!text || !text.trim()) {
                this.log('INFO', `Respuesta vacía offset=${offset}`);
                return { transactions: [], total: 0 };
            }

            const data = JSON.parse(text);
            this.log('INFO', `API offset=${offset}: respuesta recibida`, {
                type: Array.isArray(data) ? 'array' : 'object',
                keys: Array.isArray(data) ? 'list' : Object.keys(data),
                count: Array.isArray(data) ? data.length : (data.total || 'N/A')
            });
            return data;

        } catch (e) {
            this.log('ERROR', `Error fetch API offset=${offset}: ${e.message}`);
            return { transactions: [], total: 0 };
        }
    }

    parseTransaction(tx) {
        // Extraer IMEI
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

        if (imei) {
            this.log('INFO', `IMEI encontrado: ${imei}`, { cliente: tx.clientName || 'N/A' });
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
        } else {
            this.log('INFO', 'Primera página vacía o sin transacciones');
        }

        scrapingStatus.extracted = allTransactions.length;

        // Páginas siguientes
        let consecutiveEmpty = 0;

        while (true) {
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
            await new Promise(r => setTimeout(r, 300));
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
// ENDPOINTS
// ============================================================

fastify.get('/', async () => ({
    service: 'ADDI Scraper API for GigaMovil',
    version: '1.2.0-nodejs',
    status: 'running',
    runtime: 'Node.js',
    endpoints: {
        'GET /health': 'Estado del servicio',
        'POST /scrape': 'Scraping síncrono (espera resultado)',
        'POST /scrape/async': 'Scraping en background',
        'GET /scrape/status': 'Estado del scraping async',
        'GET /transactions': 'Transacciones en cache',
        'GET /debug/last-errors': 'Últimos errores'
    }
}));

fastify.get('/health', async () => ({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    runtime: 'Node.js + Puppeteer',
    scrapingRunning: scrapingStatus.running,
    cachedTransactions: cachedTransactions.length,
    lastRun: scrapingStatus.lastRun,
    lastError: scrapingStatus.error
}));

fastify.post('/scrape', async (request) => {
    const { email, password, status_filter: statusFilter, limit = 100 } = request.body;

    if (scrapingStatus.running) {
        return {
            success: false,
            total: 0,
            transactions: [],
            error: 'Ya hay un scraping en progreso',
            errors_log: ['Bloqueado: scraping anterior en ejecución']
        };
    }

    scrapingStatus.running = true;
    scrapingStatus.error = null;
    scrapingStatus.errorsLog = [];
    const startTime = Date.now();

    try {
        const scraper = new AddiScraper(email, password, statusFilter);
        const [transactions, errors] = await scraper.run(limit);

        cachedTransactions = transactions;
        scrapingStatus.lastRun = new Date().toISOString();
        scrapingStatus.errorsLog = errors;
        scrapingStatus.extracted = transactions.length;

        const executionTime = (Date.now() - startTime) / 1000;
        const success = transactions.length > 0 || errors.length === 0;

        if (!success && errors.length > 0) {
            scrapingStatus.error = errors[errors.length - 1];
        }

        return {
            success,
            total: transactions.length,
            filter: statusFilter,
            transactions,
            error: errors.length > 0 && transactions.length === 0 ? errors[errors.length - 1] : null,
            errors_log: errors,
            execution_time: Math.round(executionTime * 100) / 100
        };

    } catch (e) {
        const executionTime = (Date.now() - startTime) / 1000;
        const errorMsg = `Error fatal: ${e.message}`;
        scrapingStatus.error = errorMsg;

        return {
            success: false,
            total: 0,
            transactions: [],
            error: errorMsg,
            errors_log: [errorMsg, e.stack],
            execution_time: Math.round(executionTime * 100) / 100
        };
    } finally {
        scrapingStatus.running = false;
    }
});

fastify.post('/scrape/async', async (request) => {
    const { email, password, status_filter: statusFilter, limit = 100 } = request.body;

    if (scrapingStatus.running) {
        return {
            success: false,
            error: 'Ya hay un scraping en progreso'
        };
    }

    // Ejecutar en background
    setImmediate(async () => {
        scrapingStatus.running = true;
        scrapingStatus.error = null;

        try {
            const scraper = new AddiScraper(email, password, statusFilter);
            const [transactions, errors] = await scraper.run(limit);
            cachedTransactions = transactions;
            scrapingStatus.lastRun = new Date().toISOString();
            scrapingStatus.errorsLog = errors;
            if (errors.length > 0 && transactions.length === 0) {
                scrapingStatus.error = errors[errors.length - 1];
            }
        } catch (e) {
            scrapingStatus.error = e.message;
        } finally {
            scrapingStatus.running = false;
        }
    });

    return {
        success: true,
        message: 'Scraping iniciado en background',
        check_status: '/scrape/status'
    };
});

fastify.get('/scrape/status', async () => ({
    ...scrapingStatus,
    cached_count: cachedTransactions.length
}));

fastify.get('/transactions', async (request) => {
    let { status, limit } = request.query;
    let transactions = cachedTransactions;

    if (status) {
        transactions = transactions.filter(tx => tx.status === status);
    }

    if (limit) {
        transactions = transactions.slice(0, parseInt(limit));
    }

    return {
        success: true,
        total: transactions.length,
        last_run: scrapingStatus.lastRun,
        transactions
    };
});

fastify.get('/debug/last-errors', async () => ({
    running: scrapingStatus.running,
    lastRun: scrapingStatus.lastRun,
    error: scrapingStatus.error,
    errorsLog: scrapingStatus.errorsLog,
    extracted: scrapingStatus.extracted,
    total: scrapingStatus.total
}));

// Iniciar servidor
const port = process.env.PORT || 8000;
try {
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`ADDI Scraper API running on port ${port}`);
} catch (err) {
    fastify.log.error(err);
    process.exit(1);
}