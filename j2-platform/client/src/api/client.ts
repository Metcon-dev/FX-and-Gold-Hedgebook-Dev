function resolveApiBase(): string {
    const envBaseRaw = String(import.meta.env?.VITE_API_BASE_URL || '').trim();
    if (envBaseRaw) return envBaseRaw.replace(/\/+$/, '');

    if (typeof window !== 'undefined') {
        const { protocol, hostname, port } = window.location;
        // In local Vite dev, call Flask directly to avoid proxy drift/misconfig.
        if (port === '5173') {
            return `${protocol}//${hostname}:5001/api`;
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

// ── Trades ──────────────────────────────────────────────────────────
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

    updateTradeNumber: (id: number, tradeNumber: string) =>
        request<{ ok: boolean }>(`/trades/${id}/trade-number`, {
            method: 'PUT',
            body: JSON.stringify({ trade_number: tradeNumber }),
        }),

    updatePmxTradeNumber: (id: number, tradeNumber: string) =>
        request<{ ok: boolean }>(`/pmx/trades/${id}/trade-number`, {
            method: 'PUT',
            body: JSON.stringify({ trade_number: tradeNumber }),
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

    // Weighted average
    getWeightedAverage: (tradeNum: string) =>
        request<Record<string, unknown>>(`/weighted-average/${encodeURIComponent(tradeNum)}`),

    // Trading ticket
    getTicket: (tradeNum: string) =>
        request<Record<string, unknown>>(`/ticket/${encodeURIComponent(tradeNum)}`),
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

    saveExportTradesToFolder: (data: { trades: { trade_num: string; fnc_numbers: string[] }[]; output_dir?: string }) =>
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
};

export default api;
