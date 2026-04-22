function resolveApiBase(): string {
    const envBaseRaw = String(import.meta.env?.VITE_API_BASE_URL || '').trim();
    if (envBaseRaw) return envBaseRaw.replace(/\/+$/, '');

    if (typeof window !== 'undefined') {
        const { protocol, hostname, port } = window.location;
        // In Vite dev, call Flask directly only for localhost.
        // For LAN/IP access, keep same-origin /api so Vite proxy handles backend routing.
        const isLocalHost = hostname === 'localhost' || hostname === '127.0.0.1';
        if (isLocalHost && /^517\d+$/.test(String(port))) {
            return `${protocol}//${hostname}:5003/api`;
        }
    }

    // Default for same-origin deployments (frontend and backend under one host).
    return '/api';
}

const BASE = resolveApiBase();

type PmxAuthHeaders = {
    x_auth?: string;
    sid?: string;
    username?: string;
};

export type AppUser = {
    id: number;
    username: string;
    display_name: string;
    role: string;
    permissions: string[];
};

export type AdminUser = AppUser & {
    can_read: boolean;
    can_write: boolean;
    is_admin: boolean;
    is_active: boolean;
    created_at: string;
};

async function request<T>(path: string, options?: RequestInit): Promise<T> {
    const url = `${BASE}${path}`;
    const res = await fetch(url, {
        ...options,
        cache: 'no-store',
        credentials: 'include',
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            Pragma: 'no-cache',
            ...options?.headers,
        },
    });

    const contentType = String(res.headers.get('content-type') || '').toLowerCase();
    const raw = await res.text();
    let parsed: unknown = null;
    if (raw) {
        try {
            parsed = JSON.parse(raw);
        } catch {
            parsed = null;
        }
    }

    if (!res.ok) {
        const errMsg = (
            parsed &&
            typeof parsed === 'object' &&
            parsed !== null &&
            'error' in parsed &&
            typeof (parsed as { error?: unknown }).error === 'string'
        )
            ? String((parsed as { error: string }).error)
            : '';

        if (errMsg) {
            throw new Error(errMsg);
        }

        const preview = raw
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 180);
        const fallback = preview
            ? `${res.status} ${res.statusText} (${url}): ${preview}`
            : `${res.status} ${res.statusText} (${url})`;
        throw new Error(fallback || `HTTP ${res.status}`);
    }

    if (parsed === null) {
        const preview = raw
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 180)
            .toLowerCase();
        const htmlHint = preview.startsWith('<!doctype') || preview.startsWith('<html')
            ? ' (received HTML instead of JSON; check API proxy/base URL/backend route)'
            : '';
        const typeLabel = contentType || 'unknown content type';
        throw new Error(`Expected JSON response from ${url}, got ${typeLabel}${htmlHint}`);
    }

    return parsed as T;
}

// â”€â”€ Trades â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
export const api = {
    health: () => request<{ status: string; time: string }>('/health'),

    authLogin: (username: string, password: string) =>
        request<{ ok: boolean; user: AppUser }>('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ username, password }),
        }),

    authMe: () =>
        request<{ ok: boolean; user: AppUser }>('/auth/me'),

    authLogout: () =>
        request<{ ok: boolean }>('/auth/logout', { method: 'POST', body: JSON.stringify({}) }),

    authUsers: () =>
        request<{ ok: boolean; users: AdminUser[] }>('/auth/users'),

    authCreateUser: (data: {
        username: string;
        password: string;
        display_name?: string;
        role?: string;
        can_read?: boolean;
        can_write?: boolean;
        is_admin?: boolean;
        is_active?: boolean;
    }) =>
        request<{ ok: boolean; user: AdminUser }>('/auth/users', {
            method: 'POST',
            body: JSON.stringify(data),
        }),

    authUpdateUser: (userId: number, data: {
        username?: string;
        password?: string;
        display_name?: string;
        role?: string;
        can_read?: boolean;
        can_write?: boolean;
        is_admin?: boolean;
        is_active?: boolean;
    }) =>
        request<{ ok: boolean; user: AdminUser }>(`/auth/users/${userId}`, {
            method: 'PUT',
            body: JSON.stringify(data),
        }),

    authDeleteUser: (userId: number) =>
        request<{ ok: boolean; deleted_id: number }>(`/auth/users/${userId}`, {
            method: 'DELETE',
        }),

    getPmxLedger: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<Record<string, unknown>[]>(`/pmx/ledger${qs}`);
    },

    getPmxLedgerFullCsv: (params?: Record<string, string>, pmxHeaders?: PmxAuthHeaders) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        const headers: Record<string, string> = {};
        const xAuth = String(pmxHeaders?.x_auth ?? '').trim();
        const sid = String(pmxHeaders?.sid ?? '').trim();
        const username = String(pmxHeaders?.username ?? '').trim();
        if (xAuth) headers['x-auth'] = xAuth;
        if (sid) headers['sid'] = sid;
        if (username) headers['username'] = username;
        return fetch(`${BASE}/pmx/ledger-full-csv${qs}`, { headers, credentials: 'include' });
    },

    getPmxOpenPositionsReval: () =>
        request<{
            rows: Record<string, unknown>[];
            summary: Record<string, unknown>;
            market: Record<string, unknown>;
        }>('/pmx/open-positions-reval'),
    getPmxOpenPositionsRevalPdf: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return fetch(`${BASE}/pmx/open-positions-reval/pdf${qs}`, { credentials: 'include' });
    },

    getAccountBalances: () =>
        request<{
            account_code: string;
            xau: number | null;
            xag: number | null;
            usd: number | null;
            zar: number | null;
            as_of_date: string;
            fetched_at: string;
            ok: boolean;
            error: string;
        }>('/pmx/account-balances'),

    getAccountRecon: (params?: Record<string, string>, pmxHeaders?: PmxAuthHeaders) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        const headers: Record<string, string> = {};
        const xAuth = String(pmxHeaders?.x_auth ?? '').trim();
        const sid = String(pmxHeaders?.sid ?? '').trim();
        const username = String(pmxHeaders?.username ?? '').trim();
        if (xAuth) headers['x-auth'] = xAuth;
        if (sid) headers['sid'] = sid;
        if (username) {
            headers['username'] = username;
            headers['usercode'] = username;
        }
        return request<{
            start_date: string;
            end_date: string;
            month: string;
            currencies: Record<string, {
                opening_balance: number;
                transaction_total: number | null;
                expected_balance: number | null;
                actual_balance: number | null;
                delta: number | null;
            }>;
            actual_balances_ok: boolean;
            transactions_ok: boolean;
            error: string;
            rows: Record<string, unknown>[];
            diagnostics: {
                row_count_total: number;
                row_count_included_xau: number;
                row_count_included_usd: number;
                row_count_included_zar: number;
                view_a_ok: boolean;
                view_b_ok: boolean;
                view_c_ok: boolean;
                delta_formula: string;
            };
        }>(`/pmx/account-recon${qs}`, { headers });
    },

    setOpeningBalance: (month: string, currency: string, opening_balance: number) =>
        request<{ ok: boolean; error?: string }>('/pmx/account-recon/opening-balance', {
            method: 'POST',
            body: JSON.stringify({ month, currency, opening_balance }),
        }),

    getForwardExposure: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<{
            rows: Record<string, unknown>[];
            calendar: Record<string, unknown>[];
            summary: Record<string, unknown>;
            tenors: string[];
        }>(`/pmx/forward-exposure${qs}`);
    },

    getProfitMonthly: () =>
        request<{
            months: Record<string, unknown>[];
            summary: Record<string, unknown>;
        }>('/profit/monthly'),

    syncPmxLedger: (data?: Record<string, unknown>) =>
        request<Record<string, unknown>>('/pmx/sync-ledger', { method: 'POST', body: JSON.stringify(data || {}) }),

    getOpenPositions: () =>
        request<{ positions: Record<string, unknown>[]; summary: Record<string, number> }>('/trades/open-positions'),

    addTrade: (data: Record<string, unknown>) =>
        request<{ success: boolean }>('/trades', { method: 'POST', body: JSON.stringify(data) }),

    updateTradeNumber: async (id: number, tradeNumber: string, options?: { overrideValidation?: boolean }) => {
        const url = `${BASE}/trades/${id}/trade-number`;
        const res = await fetch(url, {
            method: 'PUT',
            cache: 'no-store',
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                Pragma: 'no-cache',
            },
            body: JSON.stringify({
                trade_number: tradeNumber,
                override_validation: Boolean(options?.overrideValidation),
            }),
        });

        const raw = await res.text();
        let parsed: unknown = null;
        if (raw) {
            try { parsed = JSON.parse(raw); } catch { parsed = null; }
        }

        // 409 = soft validation warning. The server did NOT commit the
        // assignment; the caller decides whether to retry with
        // overrideValidation=true based on user confirmation.
        if (res.status === 409 && parsed && typeof parsed === 'object' && (parsed as { requires_confirmation?: boolean }).requires_confirmation) {
            const payload = parsed as { requires_confirmation: boolean; warning?: string };
            return { ok: false, requiresConfirmation: true, warning: String(payload.warning || 'Trade number validation warning.') };
        }

        if (!res.ok) {
            const errMsg = (
                parsed && typeof parsed === 'object' && parsed !== null &&
                'error' in parsed &&
                typeof (parsed as { error?: unknown }).error === 'string'
            ) ? String((parsed as { error: string }).error) : (raw || `HTTP ${res.status}`);
            const err = new Error(errMsg) as Error & { status?: number; payload?: unknown };
            err.status = res.status;
            err.payload = parsed;
            throw err;
        }

        return { ok: true, requiresConfirmation: false, ...(parsed as Record<string, unknown>) };
    },

    updatePmxTradeNumber: async (id: number, tradeNumber: string, options?: { overrideValidation?: boolean }) => {
        const url = `${BASE}/pmx/trades/${id}/trade-number`;
        const res = await fetch(url, {
            method: 'PUT',
            cache: 'no-store',
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                Pragma: 'no-cache',
            },
            body: JSON.stringify({
                trade_number: tradeNumber,
                override_validation: Boolean(options?.overrideValidation),
            }),
        });

        const raw = await res.text();
        let parsed: unknown = null;
        if (raw) {
            try {
                parsed = JSON.parse(raw);
            } catch {
                parsed = null;
            }
        }

        // 409 = TradeMC soft-warning. Do NOT throw — let the caller prompt
        // the user and re-invoke with overrideValidation=true on accept.
        if (res.status === 409 && parsed && typeof parsed === 'object' && (parsed as { requires_confirmation?: boolean }).requires_confirmation) {
            const payload = parsed as { requires_confirmation: boolean; warning?: string };
            return { ok: false, requiresConfirmation: true, warning: String(payload.warning || 'Trade number validation warning.') };
        }

        if (!res.ok) {
            const errMsg = (
                parsed &&
                typeof parsed === 'object' &&
                parsed !== null &&
                'error' in parsed &&
                typeof (parsed as { error?: unknown }).error === 'string'
            )
                ? String((parsed as { error: string }).error)
                : (raw || `HTTP ${res.status}`);
            const err = new Error(errMsg) as Error & { status?: number; payload?: unknown };
            err.status = res.status;
            err.payload = parsed;
            throw err;
        }

        return { ok: true, requiresConfirmation: false, ...(parsed as Record<string, unknown>) };
    },

    updatePmxGrouping: (id: number, grouping: string) =>
        request<{ ok: boolean; trade_id: number; grouping: string }>(`/pmx/trades/${id}/grouping`, {
            method: 'PUT',
            body: JSON.stringify({ grouping }),
        }),

    // TradeMC
    getTradeMCTrades: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<Record<string, unknown>[]>(`/trademc/trades${qs}`);
    },

    syncTradeMC: (data?: Record<string, unknown>) =>
        request<Record<string, unknown>>('/trademc/sync', {
            method: 'POST',
            body: JSON.stringify(data || {}),
        }),

    getTradeMCSyncStatus: () =>
        request<Record<string, unknown>>('/trademc/sync/status'),

    getTradeMCDiagnostics: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<Record<string, unknown>>(`/trademc/diagnostics${qs}`);
    },

    updateTradeMCRefNumber: (id: number, refNumber: string) =>
        request<{ success: boolean; trade_id: number; ref_number: string }>(`/trademc/trades/${id}/ref-number`, {
            method: 'PUT',
            body: JSON.stringify({ ref_number: refNumber }),
        }),

    getCompanies: () =>
        request<Record<string, unknown>[]>('/trademc/companies'),

    getTradeMCStats: () =>
        request<Record<string, unknown>>('/trademc/stats'),

    getTradeMCLivePrices: () =>
        request<Record<string, unknown>>('/trademc/live-prices'),

    getWeightTransactions: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<Record<string, unknown>[]>(`/trademc/weight-transactions${qs}`);
    },

    getWeightTypes: () =>
        request<string[]>('/trademc/weight-types'),

    syncWeight: () =>
        request<Record<string, unknown>>('/trademc/sync-weight', { method: 'POST' }),

    // Hedging
    getHedging: () =>
        request<Record<string, unknown>[]>('/hedging'),
    getDashboardUnlinkedPmxTrades: () =>
        request<{
            summary: Record<string, unknown>;
            rows: Record<string, unknown>[];
        }>('/dashboard/unlinked-pmx-trades'),
    cacheDashboardTradingSummaryPdf: (data: { pdf_base64: string; run_date?: string }) =>
        request<{ ok: boolean; run_date?: string; path?: string; error?: string }>('/dashboard/trading-summary/pdf/cache', {
            method: 'POST',
            body: JSON.stringify(data),
        }),
    getDashboardTradingSummaryPdf: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return fetch(`${BASE}/dashboard/trading-summary/pdf${qs}`, { credentials: 'include' });
    },
    sendDashboardReportEmail: (data: {
        to?: string[] | string;
        cc?: string[] | string;
        subject?: string;
        body?: string;
        filename?: string;
        pdf_base64: string;
    }) =>
        request<{ ok: boolean; message?: string; error?: string; recipients?: string[] }>('/dashboard/report/email', {
            method: 'POST',
            body: JSON.stringify(data),
        }),

    // Weighted average
    getWeightedAverage: (tradeNum: string) =>
        request<Record<string, unknown>>(`/weighted-average/${encodeURIComponent(tradeNum)}`),

    // Trading ticket
    getTicket: (tradeNum: string) =>
        request<Record<string, unknown>>(`/ticket/${encodeURIComponent(tradeNum)}`),
    checkTradeBookScreenshot: async (tradeNum: string, file: File, tradeMcRows?: Record<string, unknown>[]) => {
        const form = new FormData();
        form.append('image', file);
        form.append('trade_num', String(tradeNum || '').trim());
        if (Array.isArray(tradeMcRows) && tradeMcRows.length > 0) {
            form.append('trademc_rows_json', JSON.stringify(tradeMcRows));
        }
        const endpointDynamic = `/ticket/${encodeURIComponent(tradeNum)}/book-check`;
        const endpointFlat = '/ticket/book-check';
        const urls: string[] = [`${BASE}${endpointDynamic}`, `${BASE}${endpointFlat}`];
        if (typeof window !== 'undefined') {
            const host = String(window.location.hostname || '').trim();
            const proto = String(window.location.protocol || 'http:');
            if (host) {
                const localHostDynamic = `${proto}//${host}:5003/api${endpointDynamic}`;
                const localHostFlat = `${proto}//${host}:5003/api${endpointFlat}`;
                if (!urls.includes(localHostDynamic)) urls.push(localHostDynamic);
                if (!urls.includes(localHostFlat)) urls.push(localHostFlat);
            }
            const localhostDynamic = `http://localhost:5003/api${endpointDynamic}`;
            const localhostFlat = `http://localhost:5003/api${endpointFlat}`;
            if (!urls.includes(localhostDynamic)) urls.push(localhostDynamic);
            if (!urls.includes(localhostFlat)) urls.push(localhostFlat);
        }

        let lastErr = 'Trade book checker request failed.';
        for (let idx = 0; idx < urls.length; idx += 1) {
            const url = urls[idx];
            const res = await fetch(url, {
                method: 'POST',
                credentials: 'include',
                body: form,
            });
            const raw = await res.text();
            let parsed: unknown = null;
            if (raw) {
                try {
                    parsed = JSON.parse(raw);
                } catch {
                    parsed = null;
                }
            }

            if (res.ok) return (parsed as Record<string, unknown>) || {};

            const errMsg = (
                parsed &&
                typeof parsed === 'object' &&
                parsed !== null &&
                'error' in parsed &&
                typeof (parsed as { error?: unknown }).error === 'string'
            )
                ? String((parsed as { error: string }).error)
                : (raw || `HTTP ${res.status}`);
            lastErr = errMsg;

            const looksLikeRouteMiss = /404\s+not\s+found|method\s+not\s+allowed/i.test(errMsg);
            const canTryFallback = (
                res.status === 404 ||
                res.status === 405 ||
                (res.status >= 500 && looksLikeRouteMiss)
            ) && idx < (urls.length - 1);
            if (!canTryFallback) break;
        }
        throw new Error(lastErr);
    },
    getTicketPdf: (tradeNum: string) =>
        fetch(`${BASE}/ticket/${encodeURIComponent(tradeNum)}/pdf`, { credentials: 'include' }),
    getPmxFncPdf: (cell: string, docType = 'FNC', pmxHeaders?: PmxAuthHeaders) => {
        const headers: Record<string, string> = {};
        const xAuth = String(pmxHeaders?.x_auth ?? '').trim();
        const sid = String(pmxHeaders?.sid ?? '').trim();
        const username = String(pmxHeaders?.username ?? '').trim();
        if (xAuth) headers['x-auth'] = xAuth;
        if (sid) headers['sid'] = sid;
        if (username) headers['username'] = username;
        return fetch(
            `${BASE}/pmx/fnc-pdf?cell=${encodeURIComponent(cell)}&DocType=${encodeURIComponent(docType)}`,
            { headers, credentials: 'include' }
        );
    },

    saveExportTradesToFolder: (data: {
        trades?: { trade_num: string; fnc_numbers: string[] }[];
        manual_fnc_numbers?: string[] | string;
        trade_prefix?: string;
        output_dir?: string;
    }) =>
        request<Record<string, unknown>>('/export-trades/save', {
            method: 'POST',
            body: JSON.stringify(data),
        }),

    getPmxReconciliation: (params?: Record<string, string>) => {
        const qs = params ? '?' + new URLSearchParams(params).toString() : '';
        return request<{
            ok: boolean;
            rows: Record<string, unknown>[];
            summary: Record<string, unknown>;
            account_balances: Record<string, unknown>;
        }>(`/pmx/reconciliation${qs}`);
    },

    // â”€â”€ Supplier Payment Recons (SharePoint) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    exportTradingWorksheetExcel: (data: {
        worksheet_ref?: string;
        usd_rows: { qty?: string; rate?: string }[];
        xau_rows: { qty?: string; rate?: string }[];
    }) =>
        fetch('/api/trading-worksheet/export-excel', {
            method: 'POST',
            cache: 'no-store',
            credentials: 'include',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                Pragma: 'no-cache',
            },
            body: JSON.stringify(data),
        }),

    getSupplierReconList: () =>
        request<{ suppliers: { name: string; filename: string }[] }>('/supplier-recons/suppliers'),

    getSupplierRecon: (supplier: string, force?: boolean) =>
        request<{
            supplier: string;
            rows: Record<string, unknown>[];
            row_count: number;
            synced_at?: string;
            cached?: boolean;
            error?: string;
        }>(`/supplier-recons/${encodeURIComponent(supplier)}${force ? '?force=1' : ''}`),

    syncSupplierRecons: () =>
        request<{
            suppliers: { name: string; filename: string }[];
            synced_at: string;
            row_counts: Record<string, number>;
        }>('/supplier-recons/sync', { method: 'POST' }),

    // Forecast
    getForecast: (pair: string, days?: number, sims?: number) => {
        const params = new URLSearchParams();
        if (days) params.set('days', String(days));
        if (sims) params.set('sims', String(sims));
        const qs = params.toString() ? `?${params.toString()}` : '';
        return request<Record<string, unknown>>(`/forecast/${pair}${qs}`);
    },

    getForecastCurrentPrice: (pair: string) =>
        request<Record<string, unknown>>(`/forecast/current-price/${pair}`),

    refreshForecast: (pair: string, days?: number, sims?: number) =>
        request<Record<string, unknown>>(`/forecast/refresh/${pair}`, {
            method: 'POST',
            body: JSON.stringify({ days: days || 30, sims: sims || 10000 }),
        }),
    getForecastMacroSummary: (month?: string, force?: boolean) => {
        const params = new URLSearchParams();
        if (month) params.set('month', month);
        if (force) params.set('force', '1');
        const qs = params.toString() ? `?${params.toString()}` : '';
        return request<Record<string, unknown>>(`/forecast/macro-summary${qs}`);
    },

    // Purchases ML Forecast
    getPurchasesForecast: (spotUsd?: number, fxRate?: number) => {
        const params = new URLSearchParams();
        if (spotUsd) params.set('spot_usd', String(spotUsd));
        if (fxRate) params.set('fx_rate', String(fxRate));
        const qs = params.toString() ? `?${params.toString()}` : '';
        return request<Record<string, unknown>>(`/forecast/purchases/predict${qs}`);
    },

    trainPurchasesModel: () =>
        request<Record<string, unknown>>('/forecast/purchases/train', { method: 'POST' }),

    getPurchasesModelStatus: () =>
        request<Record<string, unknown>>('/forecast/purchases/status'),
};

export default api;



