import { Fragment, useState, useEffect, useCallback, useMemo, useRef, Component, type ErrorInfo, type FormEvent, type ReactNode, type ChangeEvent } from 'react'
import { api, type AppUser, type AdminUser } from './api/client'
import { ResponsiveContainer, ComposedChart, Bar, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, Scatter } from 'recharts'

// ===================================================================
// SHARED HELPERS
// ===================================================================
type Row = Record<string, unknown>;
type PmxHeaderValues = {
    x_auth: string;
    sid: string;
    username: string;
};
const PMX_SUPPORT_DOC_RE = /(?:FNC|SWT|FCT)\/[^\s,;]+/gi;
const PMX_AUTO_SYNC_EVENT = 'pmx:auto-sync';
const TRADEMC_AUTO_SYNC_EVENT = 'trademc:auto-sync';
const RECON_DELTA_EVENT = 'recon:delta-changed';
const TRADEMC_MISSING_SAGE_EVENT = 'trademc:missing-sage-changed';
const RECON_CACHE_UPDATED_EVENT = 'recon:cache-updated';
const BACKGROUND_REFRESH_MS = 3 * 60 * 1000;
const RECON_CACHE_KEY = 'recon:cached_payload';
const RECON_DELTA_EPSILON = 1e-9;
const PRICE_WARNING_EVENT = 'price-warning:changed';
const PRICE_WARNING_ACK_EVENT = 'price-warning:acknowledged';
const PRICE_WARNING_ACK_PREFIX = 'price-warning:ack:';
const PMX_LEDGER_DEFAULT_FILTERS = {
    symbol: '',
    trade_num: '',
    narration: '',
    start_date: '',
    end_date: '',
};
const TRADEMC_DEFAULT_FILTERS = {
    status: '',
    ref_filter: '',
    company_id: '',
    start_date: '',
    end_date: '',
};
const FORWARD_EXPOSURE_DEFAULT_FILTERS = {
    symbol: '',
    start_date: '',
    end_date: '',
};

function usePersistentState<T>(key: string, initialValue: T) {
    const [state, setState] = useState<T>(() => {
        if (typeof window === 'undefined') return initialValue;
        try {
            const raw = window.localStorage.getItem(key);
            if (raw === null) return initialValue;
            const parsed = JSON.parse(raw) as T;
            // For object-shaped states (filters), backfill new/default keys so
            // navigation keeps a stable filter model across app versions/tabs.
            if (
                typeof initialValue === 'object' && initialValue !== null && !Array.isArray(initialValue)
                && typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
            ) {
                return { ...(initialValue as Record<string, unknown>), ...(parsed as Record<string, unknown>) } as T;
            }
            return parsed;
        } catch {
            return initialValue;
        }
    });

    useEffect(() => {
        if (typeof window === 'undefined') return;
        try {
            window.localStorage.setItem(key, JSON.stringify(state));
        } catch {
            // Ignore storage quota and private mode failures.
        }
    }, [key, state]);

    return [state, setState] as const;
}

function readAckedPriceWarnings(): Record<string, boolean> {
    if (typeof window === 'undefined') return {};
    try {
        const raw = window.localStorage.getItem('price-warning:acked');
        if (!raw) return {};
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        const out: Record<string, boolean> = {};
        for (const [key, value] of Object.entries(parsed || {})) {
            out[key] = Boolean(value);
        }
        return out;
    } catch {
        return {};
    }
}

function persistAckedPriceWarnings(map: Record<string, boolean>): void {
    if (typeof window === 'undefined') return;
    try {
        window.localStorage.setItem('price-warning:acked', JSON.stringify(map));
    } catch {
        // Ignore storage failures.
    }
}

function fmt(val: unknown, decimals = 2): string {
    if (val === '' || val === null || val === undefined) return '--';
    const n = Number(val);
    if (isNaN(n)) return String(val);
    return n.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function fmtFullPrecision(val: unknown): string {
    if (val === '' || val === null || val === undefined) return '--';
    const n = Number(val);
    if (isNaN(n)) return String(val);
    return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 20 });
}

function fmtDate(val: unknown): string {
    if (!val) return '--';
    const s = String(val);
    if (s.length >= 10) return s.slice(0, 10);
    return s;
}

function fmtDateTime(val: unknown): string {
    if (!val) return '--';
    const s = String(val).trim();
    if (!s) return '--';
    if (s.length >= 16) return s.slice(0, 16);
    if (s.length >= 10) return s.slice(0, 10);
    return s;
}

function asText(val: unknown, fallback = ''): string {
    if (val === null || val === undefined) return fallback;
    const text = String(val).trim();
    return text || fallback;
}

function toNullableNumber(val: unknown): number | null {
    if (val === '' || val === null || val === undefined) return null;
    const n = Number(val);
    return Number.isFinite(n) ? n : null;
}

function toTimestampMs(val: unknown): number {
    const raw = asText(val, '');
    if (!raw) return Number.NaN;
    const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
    const ms = Date.parse(normalized);
    return Number.isNaN(ms) ? Number.NaN : ms;
}

function toNumericId(val: unknown): number {
    const n = Number(val);
    return Number.isFinite(n) ? n : 0;
}

function normalizeTradeNumberValue(val: unknown): string {
    if (val === null || val === undefined) return '';
    let text = String(val).trim();
    if (text.endsWith('.0') && /^\d+\.0$/.test(text)) text = text.slice(0, -2);
    return text.toUpperCase();
}

const WEIGHT_TYPE_SIGN: Record<string, number> = {
    CREDIT: 1,
    TRADE: 1,
    OPENING_BALANCE: 1,
    DEBIT: -1,
    CREDIT_REVERSAL: -1,
    DEBIT_REVERSAL: 1,
};

function numClass(val: unknown): string {
    const n = Number(val);
    if (isNaN(n) || n === 0) return 'num';
    return n > 0 ? 'num positive' : 'num negative';
}

function parseFilenameFromDisposition(contentDisposition: string, fallback: string): string {
    const raw = String(contentDisposition || '');
    const utf8Match = raw.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8Match && utf8Match[1]) {
        try { return decodeURIComponent(utf8Match[1].replace(/['"]/g, '').trim()); } catch { return utf8Match[1].trim(); }
    }
    const simpleMatch = raw.match(/filename=([^;]+)/i);
    if (simpleMatch && simpleMatch[1]) {
        return simpleMatch[1].replace(/['"]/g, '').trim();
    }
    return fallback;
}

function sanitizeFilename(filename: string): string {
    const cleaned = String(filename || '')
        .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
        .trim();
    return cleaned || 'download.pdf';
}

function triggerBlobDownload(blob: Blob, filename: string): void {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = sanitizeFilename(filename);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
}

function readStoredPmxHeaders(): PmxHeaderValues {
    if (typeof window === 'undefined') {
        return { x_auth: '', sid: '', username: '' };
    }
    return {
        x_auth: window.localStorage.getItem('pmx_x_auth') || '',
        sid: window.localStorage.getItem('pmx_sid') || '',
        username: window.localStorage.getItem('pmx_username') || '',
    };
}

function readCachedReconPayload(): { summary: Row; rows: Row[] } {
    if (typeof window === 'undefined') return { summary: {}, rows: [] };
    try {
        const raw = window.localStorage.getItem(RECON_CACHE_KEY);
        if (!raw) return { summary: {}, rows: [] };
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        const summary = (parsed.summary && typeof parsed.summary === 'object') ? (parsed.summary as Row) : {};
        const rows = Array.isArray(parsed.rows) ? (parsed.rows as Row[]) : [];
        return { summary, rows };
    } catch {
        return { summary: {}, rows: [] };
    }
}

function persistCachedReconPayload(summary: Row, rows: Row[]): void {
    if (typeof window === 'undefined') return;
    try {
        window.localStorage.setItem(RECON_CACHE_KEY, JSON.stringify({
            summary: summary || {},
            rows: Array.isArray(rows) ? rows : [],
            cached_at: new Date().toISOString(),
        }));
        window.dispatchEvent(new Event(RECON_CACHE_UPDATED_EVENT));
    } catch {
        // Ignore storage quota/private-mode failures.
    }
}

async function parsePmxDownloadError(res: Response): Promise<string> {
    const body = await res.json().catch(async () => ({
        error: await res.text().catch(() => ''),
        status: res.status,
        url: '',
    }));
    const msg = asText((body as { error?: unknown }).error, '');
    const pmxStatusRaw = Number((body as { status?: unknown }).status);
    const pmxStatus = Number.isFinite(pmxStatusRaw) && pmxStatusRaw > 0 ? pmxStatusRaw : null;
    const pmxUrl = asText((body as { url?: unknown }).url, '');
    const details: string[] = [];
    if (pmxStatus !== null) details.push(`PMX HTTP ${pmxStatus}`);
    if (res.status) details.push(`API HTTP ${res.status}`);
    if (pmxUrl) details.push(pmxUrl);
    if (msg) return details.length ? `${msg} (${details.join(' | ')})` : msg;
    return details.length ? details.join(' | ') : `HTTP ${res.status}`;
}

function extractPmxSupportDocs(value: unknown): string[] {
    const text = asText(value, '');
    if (!text) return [];
    const matches = text.match(PMX_SUPPORT_DOC_RE) || [];
    if (matches.length > 0) {
        return Array.from(
            new Set(
                matches
                    .map(m => m.trim().replace(/[),.;:]+$/g, ''))
                    .filter(Boolean)
            )
        );
    }
    const fallback = text.trim().replace(/[),.;:]+$/g, '');
    return fallback ? [fallback] : [];
}

async function downloadPmxFncPdfFile(fncValue: string, suppliedHeaders?: Partial<PmxHeaderValues>): Promise<void> {
    const cell = String(fncValue || '').trim();
    if (!cell) throw new Error('Missing FNC value');
    const docType = cell.includes('/') ? cell.split('/', 1)[0].toUpperCase() : 'FNC';

    const storedHeaders = readStoredPmxHeaders();
    const mergedHeaders: PmxHeaderValues = {
        x_auth: String(suppliedHeaders?.x_auth ?? storedHeaders.x_auth ?? '').trim(),
        sid: String(suppliedHeaders?.sid ?? storedHeaders.sid ?? '').trim(),
        username: String(suppliedHeaders?.username ?? storedHeaders.username ?? '').trim(),
    };

    const attempts: PmxHeaderValues[] = [mergedHeaders];
    if (mergedHeaders.sid) {
        // PMX PDF requests often work without sid; retrying without it avoids failures after sid rotation.
        attempts.push({ ...mergedHeaders, sid: '' });
    }

    let res: Response | null = null;
    let lastError = '';
    for (const headers of attempts) {
        res = await api.getPmxFncPdf(cell, docType, headers);
        if (res.ok) break;
        lastError = await parsePmxDownloadError(res);
    }

    if (!res || !res.ok) {
        throw new Error(lastError || 'PMX PDF download failed');
    }

    const blob = await res.blob();
    const fallbackName = `Fixing_Invoice_${cell.replace(/[\\/]/g, '_')}.pdf`;
    const filename = parseFilenameFromDisposition(res.headers.get('content-disposition') || '', fallbackName);
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
}

async function fetchPmxFncPdfForExport(
    fncValue: string,
    suppliedHeaders?: Partial<PmxHeaderValues>
): Promise<{ blob: Blob; filename: string }> {
    const cell = String(fncValue || '').trim();
    if (!cell) throw new Error('Missing FNC value');
    const derivedDocType = cell.includes('/') ? cell.split('/', 1)[0].toUpperCase() : 'FNC';
    const docTypeAttempts = Array.from(new Set([derivedDocType, 'FNC'])).filter(Boolean);

    const storedHeaders = readStoredPmxHeaders();
    const mergedHeaders: PmxHeaderValues = {
        x_auth: String(suppliedHeaders?.x_auth ?? storedHeaders.x_auth ?? '').trim(),
        sid: String(suppliedHeaders?.sid ?? storedHeaders.sid ?? '').trim(),
        username: String(suppliedHeaders?.username ?? storedHeaders.username ?? '').trim(),
    };

    const headerAttempts: PmxHeaderValues[] = [mergedHeaders];
    if (mergedHeaders.sid) {
        // Retry without sid; PMX sid rotates frequently.
        headerAttempts.push({ ...mergedHeaders, sid: '' });
    }

    let res: Response | null = null;
    let lastError = '';
    for (const docType of docTypeAttempts) {
        for (const headers of headerAttempts) {
            res = await api.getPmxFncPdf(cell, docType, headers);
            if (res.ok) {
                const blob = await res.blob();
                const fallbackName = `Fixing_Invoice_${cell.replace(/[\\/]/g, '_')}.pdf`;
                const filename = parseFilenameFromDisposition(
                    res.headers.get('content-disposition') || '',
                    fallbackName
                );
                return { blob, filename };
            }
            lastError = await parsePmxDownloadError(res);
        }
    }

    throw new Error(lastError || 'PMX PDF download failed');
}

// ===================================================================
// GENERIC DATA TABLE
// ===================================================================
function DataTable({ columns, data, numericCols = [], dateCols = [], formatters = {}, rowClassName, cellClassName, renderCell }: {
    columns: { key: string; label: string }[];
    data: Row[];
    numericCols?: string[];
    dateCols?: string[];
    formatters?: Record<string, { decimals?: number; prefix?: string; suffix?: string }>;
    rowClassName?: (row: Row) => string;
    cellClassName?: (row: Row, key: string, value: unknown, isNumeric: boolean) => string;
    renderCell?: (row: Row, key: string) => ReactNode | undefined;
}) {
    const [sortKey, setSortKey] = useState('');
    const [sortAsc, setSortAsc] = useState(true);

    const sorted = [...data];
    if (sortKey) {
        sorted.sort((a, b) => {
            const va = a[sortKey], vb = b[sortKey];
            const na = Number(va), nb = Number(vb);
            const cmp = !isNaN(na) && !isNaN(nb) ? na - nb : String(va ?? '').localeCompare(String(vb ?? ''));
            return sortAsc ? cmp : -cmp;
        });
    }

    const handleSort = (key: string) => {
        if (sortKey === key) setSortAsc(!sortAsc);
        else { setSortKey(key); setSortAsc(true); }
    };

    return (
        <div className="table-container">
            <table className="data-table">
                <thead>
                    <tr>
                        {columns.map(c => (
                            <th key={c.key} onClick={() => handleSort(c.key)}>
                                {c.label} {sortKey === c.key ? (sortAsc ? '\u2191' : '\u2193') : ''}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {sorted.length === 0 && (
                        <tr><td colSpan={columns.length} style={{ textAlign: 'left', padding: '2.5rem', color: 'var(--text-muted)' }}>No data available</td></tr>
                    )}
                    {sorted.map((row, i) => (
                        <tr key={i} className={rowClassName ? rowClassName(row) : ''}>
                            {columns.map(c => {
                                const val = row[c.key];
                                const isNum = numericCols.includes(c.key);
                                const isDate = dateCols.includes(c.key);
                                const customRendered = renderCell ? renderCell(row, c.key) : undefined;
                                if (customRendered !== undefined) {
                                    return <td key={c.key}>{customRendered}</td>;
                                }
                                if (isNum) {
                                    const cfg = formatters[c.key] || {};
                                    const decimals = cfg.decimals ?? 2;
                                    const prefix = cfg.prefix ?? '';
                                    const suffix = cfg.suffix ?? '';
                                    const formatted = fmt(val, decimals);
                                    const customClass = cellClassName ? cellClassName(row, c.key, val, true) : '';
                                    return (
                                        <td key={c.key} className={customClass || numClass(val)}>
                                            {prefix}{formatted}{suffix}
                                        </td>
                                    );
                                }
                                const customClass = cellClassName ? cellClassName(row, c.key, val, false) : '';
                                return (
                                    <td key={c.key} className={customClass || (isNum ? numClass(val) : '')}>
                                        {isDate ? fmtDate(val) : String(val ?? '--')}
                                    </td>
                                );
                            })}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

// ===================================================================
// LOADING & EMPTY
// ===================================================================
function Loading({ text = 'Loading...' }: { text?: string }) {
    return <div className="loading-container"><div className="spinner"></div><span className="loading-text">{text}</span></div>;
}

function Empty({ title = 'No data', sub = '' }) {
    return (
        <div className="empty-state">
            <div className="empty-state-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                    <polyline points="14 2 14 8 20 8" />
                    <line x1="16" y1="13" x2="8" y2="13" />
                    <line x1="16" y1="17" x2="8" y2="17" />
                </svg>
            </div>
            <div className="empty-state-title">{title}</div>
            {sub && <div className="empty-state-sub">{sub}</div>}
        </div>
    );
}

type RenderGuardProps = {
    title: string;
    children: ReactNode;
};

type RenderGuardState = {
    hasError: boolean;
    message: string;
};

class RenderGuard extends Component<RenderGuardProps, RenderGuardState> {
    constructor(props: RenderGuardProps) {
        super(props);
        this.state = { hasError: false, message: '' };
    }

    static getDerivedStateFromError(error: unknown): RenderGuardState {
        const msg = error instanceof Error ? error.message : String(error || 'Unknown render error');
        return { hasError: true, message: msg };
    }

    componentDidCatch(error: unknown, info: ErrorInfo) {
        // Keep details in console for fast diagnosis without blank-screening the UI.
        console.error('RenderGuard caught error:', error, info?.componentStack || '');
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="section">
                    <div className="section-title">{this.props.title}</div>
                    <Empty title="Section failed to render" sub={this.state.message || 'Unexpected UI error'} />
                </div>
            );
        }
        return this.props.children;
    }
}

// ===================================================================
// TOAST
// ===================================================================
function useToast() {
    const [msg, setMsg] = useState<{ text: string; type: 'success' | 'error' | 'center-error' } | null>(null);
    const show = useCallback((text: string, type: 'success' | 'error' | 'center-error' = 'success') => {
        setMsg({ text, type });
        setTimeout(() => setMsg(null), 4000);
    }, []);
    const toastClass = msg
        ? msg.type === 'center-error'
            ? 'toast toast-error toast-center'
            : `toast toast-${msg.type}`
        : '';
    const Toast = msg ? <div className={toastClass}>{msg.text}</div> : null;
    return { show, Toast };
}

// ===================================================================
// EDITABLE TRADE # CELL
// ===================================================================
function EditableTradeNum({ value, rowId, onSaved, onError, saveTradeNumber, label = 'Trade #' }: {
    value: string;
    rowId: number;
    onSaved: (newVal: string) => void;
    onError: (msg: string) => void;
    saveTradeNumber?: (id: number, tradeNumber: string) => Promise<{ ok: boolean; savedValue?: string; message?: string }>;
    label?: string;
}) {
    const [editing, setEditing] = useState(false);
    const [draft, setDraft] = useState(value);
    const [saving, setSaving] = useState(false);
    const cellLabel = String(label || 'Trade #');

    useEffect(() => { setDraft(value); }, [value]);

    const save = async () => {
        const trimmed = draft.trim();
        const normalizedDraft = normalizeTradeNumberValue(trimmed);
        const normalizedValue = normalizeTradeNumberValue(value);
        if (normalizedDraft === normalizedValue) { setEditing(false); return; }
        setSaving(true);
        try {
            const saveFn: (id: number, tradeNumber: string) => Promise<{ ok: boolean; savedValue?: string; message?: string }> =
                saveTradeNumber
                || (async (id: number, tradeNumber: string) => {
                    const base = await api.updateTradeNumber(id, tradeNumber);
                    return { ok: Boolean((base as Row).ok) };
                });
            const result = await saveFn(rowId, normalizedDraft);
            if (!result?.ok) {
                throw new Error(asText(result?.message, `${cellLabel} update failed`));
            }
            onSaved(normalizeTradeNumberValue(asText(result?.savedValue, normalizedDraft)));
            setEditing(false);
        } catch (e: unknown) {
            onError(String(e));
            setDraft(value);
            setEditing(false);
        }
        setSaving(false);
    };

    if (!editing) {
        return (
            <span
                className="editable-cell"
                onClick={() => setEditing(true)}
                title={`Click to edit ${cellLabel}`}
                style={{ display: 'inline-block', minWidth: '60px', padding: '2px 6px', cursor: 'pointer' }}
            >
                {normalizeTradeNumberValue(value) || <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>--</span>}
            </span>
        );
    }

    return (
        <input
            className="editable-input"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onBlur={save}
            onKeyDown={e => { if (e.key === 'Enter') save(); if (e.key === 'Escape') { setDraft(value); setEditing(false); } }}
            autoFocus
            disabled={saving}
            style={{ width: '80px' }}
        />
    );
}

// ===================================================================
// TAB: PMX LEDGER
// ===================================================================
function PMXLedger() {
    const ALLOCATED_PAGE_SIZE = 20;
    const UNALLOCATED_PAGE_SIZE = 20;
    const [data, setData] = useState<Row[]>([]);
    const [visibleAllocatedCount, setVisibleAllocatedCount] = useState(ALLOCATED_PAGE_SIZE);
    const [visibleUnallocatedCount, setVisibleUnallocatedCount] = useState(UNALLOCATED_PAGE_SIZE);
    const [loading, setLoading] = useState(true);
    const [syncing, setSyncing] = useState(false);
    const [livePrices, setLivePrices] = useState<Row | null>(null);
    const [priceFlash, setPriceFlash] = useState<{ xau: '' | 'up' | 'down'; fx: '' | 'up' | 'down' }>({ xau: '', fx: '' });
    const prevLiveRef = useRef<{ xau: number | null; fx: number | null }>({ xau: null, fx: null });
    const flashTimeoutRef = useRef<{ xau: number | null; fx: number | null }>({ xau: null, fx: null });
    const [filters, setFilters] = usePersistentState('filters:pmx_ledger', PMX_LEDGER_DEFAULT_FILTERS);
    const [queryFilters, setQueryFilters] = useState({ ...filters });
    const latestLoadRequestRef = useRef(0);
    const { show, Toast } = useToast();
    const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc' }>({ key: 'Trade Date', dir: 'desc' });
    const [pageError, setPageError] = useState('');
    const [selectedUnallocatedByRowId, setSelectedUnallocatedByRowId] = useState<Record<number, boolean>>({});
    const [selectedAllocatedByRowId, setSelectedAllocatedByRowId] = useState<Record<number, boolean>>({});
    const [bulkAllocateTradeNum, setBulkAllocateTradeNum] = useState('');
    const [allocatingSelected, setAllocatingSelected] = useState(false);
    const [reassigningSelected, setReassigningSelected] = useState(false);
    const [deallocatingSelected, setDeallocatingSelected] = useState(false);
    const hasActiveFilters = useMemo(
        () => Object.values(filters).some((value) => String(value ?? '').trim() !== ''),
        [filters]
    );

    useEffect(() => {
        const handle = window.setTimeout(() => {
            setQueryFilters({ ...filters });
        }, 350);
        return () => window.clearTimeout(handle);
    }, [filters]);

    const load = useCallback(async (overrideFilters?: typeof PMX_LEDGER_DEFAULT_FILTERS) => {
        const activeFilters = overrideFilters ?? queryFilters;
        const requestId = ++latestLoadRequestRef.current;
        setLoading(true);
        setPageError('');
        setVisibleAllocatedCount(ALLOCATED_PAGE_SIZE);
        setVisibleUnallocatedCount(UNALLOCATED_PAGE_SIZE);
        try {
            const params: Record<string, string> = {};
            if (activeFilters.symbol) params.symbol = activeFilters.symbol;
            if (activeFilters.trade_num) params.trade_num = activeFilters.trade_num;
            if (activeFilters.narration) params.narration = activeFilters.narration;
            if (activeFilters.start_date) params.start_date = activeFilters.start_date;
            if (activeFilters.end_date) params.end_date = activeFilters.end_date;

            const rows = await api.getPmxLedger(Object.keys(params).length ? params : undefined);
            if (requestId !== latestLoadRequestRef.current) return;
            setData(
                rows.map((row) => ({
                    ...row,
                    'Trade #': normalizeTradeNumberValue((row as Row)['Trade #']),
                }))
            );
            setSelectedUnallocatedByRowId({});
            setSelectedAllocatedByRowId({});
            setPageError('');
        } catch (e: unknown) {
            if (requestId !== latestLoadRequestRef.current) return;
            setPageError(String(e));
        } finally {
            if (requestId === latestLoadRequestRef.current) {
                setLoading(false);
            }
        }
    }, [queryFilters, show]);

    useEffect(() => { load(); }, [load]);
    const triggerPriceFlash = useCallback((key: 'xau' | 'fx', direction: 'up' | 'down') => {
        setPriceFlash(prev => ({ ...prev, [key]: direction }));
        const existing = flashTimeoutRef.current[key];
        if (existing !== null) window.clearTimeout(existing);
        flashTimeoutRef.current[key] = window.setTimeout(() => {
            setPriceFlash(prev => ({ ...prev, [key]: '' }));
            flashTimeoutRef.current[key] = null;
        }, 1200);
    }, []);

    const loadLivePrices = useCallback(async () => {
        try {
            const res = await api.getTradeMCLivePrices();
            const nextFx = toNullableNumber((res as Row).usd_zar);
            const xauUsd = toNullableNumber((res as Row).xau_usd ?? (res as Row).gold_usd);
            const nextXau = (
                toNullableNumber((res as Row).xau_zar_g)
                ?? ((xauUsd !== null && nextFx !== null) ? ((xauUsd * nextFx) / 31.1035) : null)
            );

            const prevXau = prevLiveRef.current.xau;
            if (nextXau !== null) {
                if (prevXau !== null) {
                    if (nextXau > prevXau + 1e-12) triggerPriceFlash('xau', 'up');
                    else if (nextXau < prevXau - 1e-12) triggerPriceFlash('xau', 'down');
                }
                prevLiveRef.current.xau = nextXau;
            }

            const prevFx = prevLiveRef.current.fx;
            if (nextFx !== null) {
                if (prevFx !== null) {
                    if (nextFx > prevFx + 1e-12) triggerPriceFlash('fx', 'up');
                    else if (nextFx < prevFx - 1e-12) triggerPriceFlash('fx', 'down');
                }
                prevLiveRef.current.fx = nextFx;
            }

            setLivePrices(res);
        } catch (_e: unknown) {
        } finally {
        }
    }, [triggerPriceFlash]);

    useEffect(() => {
        void loadLivePrices();
        const handle = window.setInterval(() => {
            if (document.visibilityState !== 'visible') return;
            void loadLivePrices();
        }, 15 * 1000);
        return () => window.clearInterval(handle);
    }, [loadLivePrices]);

    useEffect(() => {
        const onAutoSync = () => {
            void load();
            void loadLivePrices();
        };
        window.addEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
        return () => window.removeEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
    }, [load, loadLivePrices]);

    useEffect(() => {
        return () => {
            const xauTimer = flashTimeoutRef.current.xau;
            const fxTimer = flashTimeoutRef.current.fx;
            if (xauTimer !== null) window.clearTimeout(xauTimer);
            if (fxTimer !== null) window.clearTimeout(fxTimer);
        };
    }, []);

    const syncFromPMX = async () => {
        setSyncing(true);
        try {
            const payload: Record<string, unknown> = {
                cmdty: 'All',
                trd_opt: 'All',
            };
            if (filters.start_date) payload.start_date = filters.start_date;
            if (filters.end_date) payload.end_date = filters.end_date;

            const res = await api.syncPmxLedger(payload);
            const fetched = Number(res['fetched_rows'] ?? 0);
            const inserted = Number(res['inserted'] ?? 0);
            const updated = Number(res['updated'] ?? 0);
            const skipped = Number(res['skipped'] ?? 0);
            show(
                `PMX sync complete: ${fetched.toLocaleString()} fetched, ${inserted.toLocaleString()} inserted, ${updated.toLocaleString()} updated, ${skipped.toLocaleString()} skipped`,
                'success'
            );
            setPageError('');
            await load(filters);
            await loadLivePrices();
        } catch (e: unknown) {
            setPageError(String(e));
        }
        setSyncing(false);
    };

    const clearFilters = () => {
        setFilters({ ...PMX_LEDGER_DEFAULT_FILTERS });
    };

    const liveUsdZar = toNullableNumber(livePrices?.usd_zar);
    const liveXauUsd = toNullableNumber(livePrices?.xau_usd ?? livePrices?.gold_usd);
    const liveStripItems: { key: 'xau' | 'fx'; label: string; value: number | null; decimals: number }[] = [
        { key: 'xau', label: 'Gold Spot (XAU/USD)', value: liveXauUsd, decimals: 2 },
        { key: 'fx', label: 'USD/ZAR', value: liveUsdZar, decimals: 4 },
    ];

    const cols = [
        { key: 'Trade #', label: 'Trade #' },
        { key: 'FNC #', label: 'FNC #' },
        { key: 'Trade Date', label: 'Trade Date' },
        { key: 'Value Date', label: 'Value Date' },
        { key: 'Symbol', label: 'Symbol' },
        { key: 'Side', label: 'Side' },
        { key: 'Narration', label: 'Narration' },
        { key: 'Debit USD', label: 'Debit USD' },
        { key: 'Credit USD', label: 'Credit USD' },
        { key: 'Balance USD', label: 'Balance USD' },
        { key: 'Net XAU g', label: 'Net Au g' },
        { key: 'Oz', label: 'Oz' },
        { key: 'Debit ZAR', label: 'Debit ZAR' },
        { key: 'Credit ZAR', label: 'Credit ZAR' },
        { key: 'Balance ZAR', label: 'Balance ZAR' },
        { key: 'Trader', label: 'Trade Name' },
    ];

    const getPmxSignedOz = (row: Row): number | null => {
        const symbol = String(row['Symbol'] ?? '').toUpperCase().replace(/[\/\-\s]/g, '');
        if (symbol !== 'XAUUSD') return null;
        const qty = toNullableNumber(row['Quantity']);
        if (qty === null) return null;
        const side = String(row['Side'] ?? '').toUpperCase().trim();
        const sign = side === 'BUY' ? 1 : side === 'SELL' ? -1 : 0;
        if (!sign) return null;
        return Math.abs(qty) * sign;
    };

    const numericSortCols = new Set(['Debit USD', 'Credit USD', 'Balance USD', 'Net XAU g', 'Oz', 'Debit ZAR', 'Credit ZAR', 'Balance ZAR']);
    const pmxSortableCols = new Set(['Debit USD', 'Credit USD', 'Balance USD', 'Net XAU g', 'Oz', 'Debit ZAR', 'Credit ZAR', 'Balance ZAR']);
    const sortedData = useMemo(() => {
        return [...data].sort((a, b) => {
            const aRaw = sort.key === 'Oz' ? getPmxSignedOz(a) : a[sort.key];
            const bRaw = sort.key === 'Oz' ? getPmxSignedOz(b) : b[sort.key];
            let cmp: number;
            if (numericSortCols.has(sort.key)) {
                const aNum = toNullableNumber(aRaw) ?? -Infinity;
                const bNum = toNullableNumber(bRaw) ?? -Infinity;
                cmp = aNum - bNum;
            } else {
                cmp = String(aRaw ?? '').localeCompare(String(bRaw ?? ''));
            }
            return sort.dir === 'asc' ? cmp : -cmp;
        });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data, sort]);

    const sortDateDesc = (row: Row): number => {
        const candidates = [
            toTimestampMs(row.allocated_at),
            toTimestampMs(row['Trade Date']),
            toTimestampMs(row['Value Date']),
            toTimestampMs(row.trade_timestamp),
            toTimestampMs(row.trade_date),
            toTimestampMs(row.created_at),
            toTimestampMs(row.updated_at),
        ].filter(v => Number.isFinite(v));
        return candidates.length > 0 ? Math.max(...candidates) : Number.NaN;
    };
    const allocatedData = sortedData
        .filter(row => normalizeTradeNumberValue(row['Trade #']) !== '')
        .sort((a, b) => {
            const aTs = sortDateDesc(a);
            const bTs = sortDateDesc(b);
            if (Number.isFinite(aTs) && Number.isFinite(bTs) && aTs !== bTs) return bTs - aTs;
            if (Number.isFinite(bTs)) return 1;
            if (Number.isFinite(aTs)) return -1;
            const aTrade = normalizeTradeNumberValue(a['Trade #']);
            const bTrade = normalizeTradeNumberValue(b['Trade #']);
            return bTrade.localeCompare(aTrade, undefined, { numeric: true, sensitivity: 'base' });
        });
    const unallocatedData = sortedData.filter(row => normalizeTradeNumberValue(row['Trade #']) === '');
    const visibleAllocatedData = allocatedData.slice(0, visibleAllocatedCount);
    const visibleUnallocatedData = unallocatedData.slice(0, visibleUnallocatedCount);
    const hasMoreAllocated = visibleAllocatedCount < allocatedData.length;
    const hasMoreUnallocated = visibleUnallocatedCount < unallocatedData.length;
    const selectedUnallocatedCount = useMemo(
        () => unallocatedData.reduce((acc, row) => {
            const rowId = toNumericId(row['id']);
            return acc + (rowId > 0 && selectedUnallocatedByRowId[rowId] ? 1 : 0);
        }, 0),
        [unallocatedData, selectedUnallocatedByRowId]
    );
    const selectedAllocatedCount = useMemo(
        () => allocatedData.reduce((acc, row) => {
            const rowId = toNumericId(row['id']);
            return acc + (rowId > 0 && selectedAllocatedByRowId[rowId] ? 1 : 0);
        }, 0),
        [allocatedData, selectedAllocatedByRowId]
    );

    const allocateSelectedTrades = async () => {
        const targetTradeNum = normalizeTradeNumberValue(bulkAllocateTradeNum);
        if (!targetTradeNum) {
            show('Enter a Trade # first.', 'center-error');
            return;
        }

        const selectedRows = unallocatedData.filter((row) => {
            const rowId = toNumericId(row['id']);
            return rowId > 0 && selectedUnallocatedByRowId[rowId];
        });
        if (selectedRows.length === 0) {
            show('Select at least one unallocated trade.', 'center-error');
            return;
        }

        setAllocatingSelected(true);
        try {
            let okCount = 0;
            let failCount = 0;
            let firstError = '';
            for (const row of selectedRows) {
                const rowId = toNumericId(row['id']);
                if (rowId <= 0) continue;
                try {
                    await api.updatePmxTradeNumber(rowId, targetTradeNum);
                    okCount += 1;
                } catch (e: unknown) {
                    failCount += 1;
                    if (!firstError) firstError = String(e);
                }
            }

            if (okCount > 0) show(`Allocated ${okCount.toLocaleString()} trade(s) to ${targetTradeNum}.`, 'success');
            if (failCount > 0) show(`Failed on ${failCount.toLocaleString()} row(s): ${firstError}`, 'center-error');

            setBulkAllocateTradeNum('');
            setSelectedUnallocatedByRowId({});
            await load(filters);
        } finally {
            setAllocatingSelected(false);
        }
    };

    const reassignSelectedTrades = async () => {
        const targetTradeNum = normalizeTradeNumberValue(bulkAllocateTradeNum);
        if (!targetTradeNum) {
            show('Enter a Trade # first.', 'center-error');
            return;
        }

        const selectedRows = allocatedData.filter((row) => {
            const rowId = toNumericId(row['id']);
            return rowId > 0 && selectedAllocatedByRowId[rowId];
        });
        if (selectedRows.length === 0) {
            show('Select at least one allocated trade.', 'center-error');
            return;
        }

        setReassigningSelected(true);
        try {
            let okCount = 0;
            let failCount = 0;
            let firstError = '';
            for (const row of selectedRows) {
                const rowId = toNumericId(row['id']);
                if (rowId <= 0) continue;
                try {
                    await api.updatePmxTradeNumber(rowId, targetTradeNum);
                    okCount += 1;
                } catch (e: unknown) {
                    failCount += 1;
                    if (!firstError) firstError = String(e);
                }
            }

            if (okCount > 0) show(`Reassigned ${okCount.toLocaleString()} trade(s) to ${targetTradeNum}.`, 'success');
            if (failCount > 0) show(`Failed on ${failCount.toLocaleString()} row(s): ${firstError}`, 'center-error');

            setBulkAllocateTradeNum('');
            setSelectedAllocatedByRowId({});
            await load(filters);
        } finally {
            setReassigningSelected(false);
        }
    };

    const deallocateSelectedTrades = async () => {
        const selectedRows = allocatedData.filter((row) => {
            const rowId = toNumericId(row['id']);
            return rowId > 0 && selectedAllocatedByRowId[rowId];
        });
        if (selectedRows.length === 0) {
            show('Select at least one allocated trade.', 'center-error');
            return;
        }

        setDeallocatingSelected(true);
        try {
            let okCount = 0;
            let failCount = 0;
            let firstError = '';
            for (const row of selectedRows) {
                const rowId = toNumericId(row['id']);
                if (rowId <= 0) continue;
                try {
                    await api.updatePmxTradeNumber(rowId, '');
                    okCount += 1;
                } catch (e: unknown) {
                    failCount += 1;
                    if (!firstError) firstError = String(e);
                }
            }
            if (okCount > 0) show(`Deallocated ${okCount.toLocaleString()} trade(s).`, 'success');
            if (failCount > 0) show(`Failed on ${failCount.toLocaleString()} row(s): ${firstError}`, 'center-error');
            setSelectedAllocatedByRowId({});
            await load(filters);
        } finally {
            setDeallocatingSelected(false);
        }
    };

    const downloadFilteredCsv = async () => {
        const params: Record<string, string> = {};
        // Full statement export: include allocated + unallocated rows.
        // Keep only optional date window; do not narrow by symbol/trade/narration.
        if (filters.start_date) params.start_date = filters.start_date;
        if (filters.end_date) params.end_date = filters.end_date;

        try {
            const res = await api.getPmxLedgerFullCsv(
                Object.keys(params).length ? params : undefined,
                readStoredPmxHeaders()
            );
            if (!res.ok) {
                const errText = await res.text().catch(() => '');
                throw new Error(errText || `HTTP ${res.status}`);
            }
            const blob = await res.blob();
            const fallback = `pmx_full_report_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.csv`;
            const filename = parseFilenameFromDisposition(res.headers.get('content-disposition') || '', fallback);
            triggerBlobDownload(blob, filename);
        } catch (e: unknown) {
            show(`Full PMX CSV download failed: ${String(e)}`, 'error');
        }
    };

    const renderLedgerTable = (rows: Row[], emptyMessage: string, containerClassName = '', selectionMode: 'unallocated' | 'allocated' | null = null) => (
        <div className={`table-container ${containerClassName}`.trim()}>
            <table className="data-table">
                <thead>
                    <tr>
                        {selectionMode && (
                            <th style={{ width: 36 }}>
                                <input
                                    type="checkbox"
                                    aria-label={`Select all visible ${selectionMode} trades`}
                                    checked={rows.length > 0 && rows.every((row) => {
                                        const rowId = toNumericId(row['id']);
                                        if (rowId <= 0) return false;
                                        return selectionMode === 'unallocated'
                                            ? !!selectedUnallocatedByRowId[rowId]
                                            : !!selectedAllocatedByRowId[rowId];
                                    })}
                                    onChange={(e) => {
                                        const checked = e.target.checked;
                                        if (selectionMode === 'unallocated') {
                                            setSelectedUnallocatedByRowId((prev) => {
                                                const next = { ...prev };
                                                for (const row of rows) {
                                                    const rowId = toNumericId(row['id']);
                                                    if (rowId <= 0) continue;
                                                    if (checked) next[rowId] = true;
                                                    else delete next[rowId];
                                                }
                                                return next;
                                            });
                                        } else {
                                            setSelectedAllocatedByRowId((prev) => {
                                                const next = { ...prev };
                                                for (const row of rows) {
                                                    const rowId = toNumericId(row['id']);
                                                    if (rowId <= 0) continue;
                                                    if (checked) next[rowId] = true;
                                                    else delete next[rowId];
                                                }
                                                return next;
                                            });
                                        }
                                    }}
                                />
                            </th>
                        )}
                        {cols.map(c => {
                            const isSorted = sort.key === c.key;
                            const canSort = pmxSortableCols.has(c.key);
                            return (
                                <th
                                    key={c.key}
                                    onClick={canSort ? () => setSort(prev =>
                                        prev.key === c.key
                                            ? { key: c.key, dir: prev.dir === 'asc' ? 'desc' : 'asc' }
                                            : { key: c.key, dir: 'asc' }
                                    ) : undefined}
                                    style={{ cursor: canSort ? 'pointer' : 'default', userSelect: 'none', whiteSpace: 'nowrap' }}
                                    title={canSort ? `Sort by ${c.label}` : c.label}
                                >
                                    {c.label}{isSorted ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}
                                </th>
                            );
                        })}
                    </tr>
                </thead>
                <tbody>
                    {rows.length === 0 && (
                        <tr><td colSpan={cols.length + (selectionMode ? 1 : 0)} style={{ textAlign: 'left', padding: '2.5rem', color: 'var(--text-muted)' }}>{emptyMessage}</td></tr>
                    )}
                    {rows.map((row, i) => (
                        <tr key={`${String(row['id'] ?? '')}-${i}`}>
                            {selectionMode && (
                                <td>
                                    {toNumericId(row['id']) > 0 && (
                                        <input
                                            type="checkbox"
                                            checked={selectionMode === 'unallocated'
                                                ? !!selectedUnallocatedByRowId[toNumericId(row['id'])]
                                                : !!selectedAllocatedByRowId[toNumericId(row['id'])]}
                                            onChange={(e) => {
                                                const rowId = toNumericId(row['id']);
                                                if (rowId <= 0) return;
                                                const checked = e.target.checked;
                                                if (selectionMode === 'unallocated') {
                                                    setSelectedUnallocatedByRowId((prev) => ({ ...prev, [rowId]: checked }));
                                                } else {
                                                    setSelectedAllocatedByRowId((prev) => ({ ...prev, [rowId]: checked }));
                                                }
                                            }}
                                        />
                                    )}
                                </td>
                            )}
                            {cols.map(c => {
                                const val = c.key === 'Oz' ? getPmxSignedOz(row) : row[c.key];
                                const isNum = ['Debit USD', 'Credit USD', 'Balance USD', 'Net XAU g', 'Oz', 'Debit ZAR', 'Credit ZAR', 'Balance ZAR'].includes(c.key);
                                const isDate = ['Trade Date', 'Value Date'].includes(c.key);

                                if (c.key === 'Trade #') {
                                    const editRowId = toNumericId(row['id']);
                                    if (editRowId <= 0) {
                                        return <td key={c.key}>{String(val ?? '--')}</td>;
                                    }
                                    return (
                                        <td key={c.key}>
                                            <EditableTradeNum
                                                value={String(val ?? '')}
                                                rowId={editRowId}
                                                onSaved={(newVal) => {
                                                    const normalizedNewVal = normalizeTradeNumberValue(newVal);
                                                    setData(prev => prev.map(r =>
                                                        r['id'] === row['id'] ? { ...r, 'Trade #': normalizedNewVal } : r
                                                    ));
                                                }}
                                                onError={(msg) => show(msg, 'center-error')}
                                                saveTradeNumber={api.updatePmxTradeNumber}
                                            />
                                        </td>
                                    );
                                }

                                if (c.key === 'FNC #') {
                                    const fnc = String(val ?? '').trim();
                                    if (!fnc) {
                                        return <td key={c.key}>--</td>;
                                    }
                                    return (
                                        <td key={c.key}>
                                            <span
                                                className="fnc-download-link"
                                                title="Click to download PMX fixing invoice PDF"
                                                role="button"
                                                tabIndex={0}
                                                aria-label={`Download fixing invoice for ${fnc}`}
                                                onClick={async () => {
                                                    try {
                                                        await downloadPmxFncPdfFile(fnc);
                                                    } catch (e: unknown) {
                                                        show(String(e), 'error');
                                                    }
                                                }}
                                                onKeyDown={async (e) => {
                                                    if (e.key === 'Enter' || e.key === ' ') {
                                                        e.preventDefault();
                                                        try {
                                                            await downloadPmxFncPdfFile(fnc);
                                                        } catch (err: unknown) {
                                                            show(String(err), 'error');
                                                        }
                                                    }
                                                }}
                                            >
                                                {fnc}
                                            </span>
                                        </td>
                                    );
                                }

                                return (
                                    <td key={c.key} className={isNum ? numClass(val) : ''}>
                                        {isNum ? fmt(val) : isDate ? fmtDate(val) : String(val ?? '--')}
                                    </td>
                                );
                            })}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>PMX Trading Ledger</h2>
                    <div className="page-subtitle">
                        {data.length === 0
                            ? 'No entries'
                            : `${data.length.toLocaleString()} entries (${allocatedData.length.toLocaleString()} allocated, ${unallocatedData.length.toLocaleString()} unallocated)`}
                    </div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm btn-primary" onClick={syncFromPMX} disabled={syncing}>
                        {syncing ? 'Syncing...' : 'Sync PMX Trades'}
                    </button>
                    <button className="btn btn-sm" onClick={downloadFilteredCsv} disabled={data.length === 0}>Download CSV</button>
                    <button className="btn btn-sm btn-secondary" onClick={() => { void load(filters); void loadLivePrices(); }}>Refresh</button>
                </div>
            </div>

            <div className="pmx-live-strip" role="status" aria-live="polite">
                {liveStripItems.map((item, idx) => {
                    const trend = priceFlash[item.key];
                    const trendClass = trend === 'up' ? 'positive' : trend === 'down' ? 'negative' : '';
                    const arrow = trend === 'up' ? '▲' : trend === 'down' ? '▼' : '•';
                    return (
                        <div key={item.key} className={`pmx-live-strip-item ${idx < liveStripItems.length - 1 ? 'with-divider' : ''}`}>
                            <span className="pmx-live-strip-label">{item.label}:</span>
                            <span className={`pmx-live-strip-value ${trendClass} ${trend === 'up' ? 'price-flash-up' : trend === 'down' ? 'price-flash-down' : ''}`}>
                                {item.value !== null ? fmt(item.value, item.decimals ?? 2) : '--'}
                            </span>
                            <span className={`pmx-live-strip-arrow ${trendClass}`}>{arrow}</span>
                        </div>
                    );
                })}
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Symbol</label>
                    <select value={filters.symbol} onChange={e => setFilters(f => ({ ...f, symbol: e.target.value }))}>
                        <option value="">All</option>
                        <option value="XAU/USD">XAU/USD</option>
                        <option value="XAG/USD">XAG/USD</option>
                        <option value="XPT/USD">XPT/USD</option>
                        <option value="XPD/USD">XPD/USD</option>
                        <option value="USD/ZAR">USD/ZAR</option>
                    </select>
                </div>
                <div className="filter-group">
                    <label>Trade #</label>
                    <input placeholder="e.g. P1019" value={filters.trade_num} onChange={e => setFilters(f => ({ ...f, trade_num: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>Allocate To Trade #</label>
                    <input
                        placeholder="e.g. 9896 / JOS-070"
                        value={bulkAllocateTradeNum}
                        onChange={e => setBulkAllocateTradeNum(e.target.value)}
                    />
                </div>
                <div className="filter-group">
                    <label>Narration</label>
                    <input placeholder="contains text" value={filters.narration} onChange={e => setFilters(f => ({ ...f, narration: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>From (sync/filter)</label>
                    <input type="date" value={filters.start_date} onChange={e => setFilters(f => ({ ...f, start_date: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>To (sync/filter)</label>
                    <input type="date" value={filters.end_date} onChange={e => setFilters(f => ({ ...f, end_date: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button
                        className="btn btn-sm btn-primary"
                        onClick={() => { void allocateSelectedTrades(); }}
                        disabled={allocatingSelected || selectedUnallocatedCount === 0 || !normalizeTradeNumberValue(bulkAllocateTradeNum)}
                    >
                        {allocatingSelected ? 'Allocating...' : `Allocate Selected (${selectedUnallocatedCount})`}
                    </button>
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button
                        className="btn btn-sm btn-primary"
                        onClick={() => { void reassignSelectedTrades(); }}
                        disabled={reassigningSelected || selectedAllocatedCount === 0 || !normalizeTradeNumberValue(bulkAllocateTradeNum)}
                    >
                        {reassigningSelected ? 'Reassigning...' : `Reassign Selected (${selectedAllocatedCount})`}
                    </button>
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button
                        className="btn btn-sm"
                        style={{ borderColor: 'var(--danger)', color: 'var(--danger)' }}
                        onClick={() => { void deallocateSelectedTrades(); }}
                        disabled={deallocatingSelected || selectedAllocatedCount === 0}
                    >
                        {deallocatingSelected ? 'Deallocating...' : `Deallocate Selected (${selectedAllocatedCount})`}
                    </button>
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button className="btn btn-sm" onClick={clearFilters} disabled={!hasActiveFilters}>
                        Clear Filters
                    </button>
                </div>
            </div>
            {pageError && <div className="stat-sub" style={{ color: 'var(--danger)', marginTop: '0.5rem' }}>{pageError}</div>}

            {loading ? <Loading /> : (
                <>
                    <div className="section">
                        <div className="section-title">Unallocated Trades</div>
                        {renderLedgerTable(visibleUnallocatedData, 'No unallocated trades', 'pmx-ledger-table-scroll pmx-ledger-table-scroll-unallocated', 'unallocated')}
                        {hasMoreUnallocated && (
                            <div style={{ textAlign: 'center', padding: '1rem' }}>
                                <button className="btn btn-sm" onClick={() => setVisibleUnallocatedCount(c => c + UNALLOCATED_PAGE_SIZE)}>
                                    Show More ({Math.min(UNALLOCATED_PAGE_SIZE, unallocatedData.length - visibleUnallocatedCount).toLocaleString()} more)
                                </button>
                            </div>
                        )}
                        {!hasMoreUnallocated && unallocatedData.length > 0 && (
                            <div style={{ textAlign: 'center', padding: '0.5rem 1rem 0.25rem', color: 'var(--text-muted)', fontSize: '0.75rem' }}>
                                Showing all unallocated trades
                            </div>
                        )}
                    </div>

                    <div className="section mt-3">
                        <div className="section-title">Allocated Trades</div>
                        {renderLedgerTable(visibleAllocatedData, 'No allocated trades', 'pmx-ledger-table-scroll', 'allocated')}
                        {hasMoreAllocated && (
                            <div style={{ textAlign: 'center', padding: '1rem' }}>
                                <button className="btn btn-sm" onClick={() => setVisibleAllocatedCount(c => c + ALLOCATED_PAGE_SIZE)}>
                                    Show More ({Math.min(ALLOCATED_PAGE_SIZE, allocatedData.length - visibleAllocatedCount).toLocaleString()} more)
                                </button>
                            </div>
                        )}
                    </div>
                </>
            )}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: OPEN POSITIONS REVAL
// ===================================================================
function OpenPositionsReval() {
    const [rows, setRows] = useState<Row[]>([]);
    const [summary, setSummary] = useState<Row>({});
    const [market, setMarket] = useState<Row>({});
    const [trademcRows, setTradeMCRows] = useState<Row[]>([]);
    const [tradeMCLoadError, setTradeMCLoadError] = useState('');
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [downloadingReport, setDownloadingReport] = useState(false);
    const [loadError, setLoadError] = useState('');
    const { show, Toast } = useToast();

    // Account recon state
    const today = new Date();
    const defaultReconStart = '2026-03-02';
    const defaultReconEnd = today.toISOString().slice(0, 10);
    const [reconStartDate, setReconStartDate] = usePersistentState('open-reval-recon:start_date', defaultReconStart);
    const [reconEndDate, setReconEndDate] = usePersistentState('open-reval-recon:end_date', defaultReconEnd);
    // Manual opening balance inputs — simple local state, not persisted to DB
    const [openingUSD, setOpeningUSD] = usePersistentState('open-reval-recon:opening_usd', '');
    const [openingXAU, setOpeningXAU] = usePersistentState('open-reval-recon:opening_xau', '');
    const [openingZAR, setOpeningZAR] = usePersistentState('open-reval-recon:opening_zar', '');
    const [recon, setRecon] = useState<ReconData | null>(null);
    const [reconLoading, setReconLoading] = useState(false);
    const [reconError, setReconError] = useState('');

    const load = useCallback(async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);

        try {
            const res = await api.getPmxOpenPositionsReval();
            setRows(Array.isArray((res as Row).rows) ? ((res as Row).rows as Row[]) : []);
            setSummary(((res as Row).summary as Row) || {});
            setMarket(((res as Row).market as Row) || {});
            try {
                const tm = await api.getTradeMCTrades();
                setTradeMCRows(Array.isArray(tm) ? tm as Row[] : []);
                setTradeMCLoadError('');
            } catch (tmErr: unknown) {
                setTradeMCRows([]);
                setTradeMCLoadError(String(tmErr));
            }
            setLoadError('');
        } catch (e: unknown) {
            const msg = String(e);
            setRows([]);
            setSummary({});
            setMarket({});
            setTradeMCRows([]);
            setTradeMCLoadError('');
            setLoadError(msg);
            show(`Failed to load open positions reval: ${msg}`, 'error');
        } finally {
            if (isRefresh) setRefreshing(false);
            else setLoading(false);
        }
    }, [show]);

    // Fetch only transaction totals + actual balances from server — no opening balance in params
    // so the 20s server-side cache is always hit after the first load for a given date range.
    const loadRecon = useCallback(async (sd: string, ed: string) => {
        setReconLoading(true);
        setReconError('');
        try {
            const res = await api.getAccountRecon({ start_date: sd, end_date: ed });
            setRecon(res as unknown as ReconData);
        } catch (e: unknown) {
            setReconError(String(e));
        } finally {
            setReconLoading(false);
        }
    }, []);

    useEffect(() => { void load(false); }, [load]);
    useEffect(() => { void loadRecon(reconStartDate, reconEndDate); }, [loadRecon, reconStartDate, reconEndDate]);

    const totalPnlZar = toNullableNumber(summary.total_pnl_zar);
    const marketXau = toNullableNumber(market.xau_usd);
    const marketFx = toNullableNumber(market.usd_zar);
    const tradeMCSummary = useMemo(() => {
        const dayKey = (dt: Date): string => {
            const y = dt.getFullYear();
            const m = String(dt.getMonth() + 1).padStart(2, '0');
            const d = String(dt.getDate()).padStart(2, '0');
            return `${y}-${m}-${d}`;
        };
        const todayKey = dayKey(new Date());

        let buyTotalG = 0;
        let sellTotalG = 0;
        let netTotalG = 0;
        let countedTrades = 0;

        for (const raw of trademcRows) {
            if (!raw || typeof raw !== 'object') continue;
            const row = raw as Row;
            const ts = row.trade_timestamp ?? row.trade_date ?? row.created_at ?? row.timestamp ?? row.date;
            const tsMs = toTimestampMs(ts);
            if (!Number.isFinite(tsMs) || tsMs <= 0) continue;
            if (dayKey(new Date(tsMs)) !== todayKey) continue;

            const weight = toNullableNumber(row.weight ?? row.Weight ?? row.qty ?? row.quantity ?? row.Quantity);
            if (weight === null) continue;

            const side = asText(row.side ?? row.Side ?? row.trade_side ?? row.trade_type ?? row.type, '').toUpperCase();
            const absWeight = Math.abs(weight);
            if (side === 'BUY') {
                buyTotalG += absWeight;
                netTotalG += absWeight;
            } else if (side === 'SELL') {
                sellTotalG -= absWeight;
                netTotalG -= absWeight;
            } else {
                if (weight >= 0) buyTotalG += weight;
                else sellTotalG += weight;
                netTotalG += weight;
            }
            countedTrades += 1;
        }

        return { buyTotalG, sellTotalG, netTotalG, countedTrades, todayKey };
    }, [trademcRows]);

    useEffect(() => {
        const onAutoSync = () => { void load(true); };
        window.addEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
        return () => window.removeEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
    }, [load]);

    const downloadReport = async () => {
        if (downloadingReport) return;
        setDownloadingReport(true);
        try {
            const params: Record<string, string> = {
                start_date: reconStartDate,
                end_date: reconEndDate,
            };
            if (openingUSD.trim()) params.opening_usd = openingUSD.trim();
            if (openingXAU.trim()) params.opening_xau = openingXAU.trim();
            if (openingZAR.trim()) params.opening_zar = openingZAR.trim();

            const res = await api.getPmxOpenPositionsRevalPdf(params);
            if (!res.ok) {
                const msg = await parsePmxDownloadError(res);
                throw new Error(msg || 'Failed to download report');
            }
            const blob = await res.blob();
            const fallbackName = `open_positions_reval_report_${new Date().toISOString().slice(0, 10)}.pdf`;
            const filename = parseFilenameFromDisposition(res.headers.get('content-disposition') || '', fallbackName);
            triggerBlobDownload(blob, filename);
        } catch (e: unknown) {
            show(String(e), 'error');
        } finally {
            setDownloadingReport(false);
        }
    };

    const openNetRows = useMemo(() => {
        const toLongShort = (sideRaw: unknown, qtyRaw: unknown): string => {
            const qty = toNullableNumber(qtyRaw);
            if (qty !== null && Math.abs(qty) > 1e-12) return qty > 0 ? 'LONG' : 'SHORT';
            const side = asText(sideRaw, '').toUpperCase();
            if (side === 'BUY') return 'LONG';
            if (side === 'SELL') return 'SHORT';
            return '';
        };

        const out: Row[] = [];
        for (const raw of rows) {
            if (!raw || typeof raw !== 'object') continue;
            const r = raw as Row;
            const pairRaw = asText(r.trade_num || r.pair || r.pair_symbol, '').toUpperCase();
            const pair = pairRaw.replace('-', '/');
            const fxQty = toNullableNumber(r.fx_qty_usd);
            const goldQtyOz = toNullableNumber(r.gold_qty_oz);

            if ((pair === 'USD/ZAR' || pair === 'USDZAR') && fxQty !== null && Math.abs(fxQty) > 1e-9) {
                const currentRate = toNullableNumber(r.market_usd_zar) ?? marketFx;
                const pnlZar = toNullableNumber(r.fx_pnl_zar);
                out.push({
                    pair: 'USD/ZAR',
                    net_side: toLongShort(r.fx_side, fxQty),
                    net_value: Math.abs(fxQty),
                    wa_rate: toNullableNumber(r.fx_wa_rate),
                    current_rate: currentRate,
                    pnl_zar: pnlZar,
                });
            }
            if ((pair === 'XAU/USD' || pair === 'XAUUSD') && goldQtyOz !== null && Math.abs(goldQtyOz) > 1e-9) {
                const currentRate = toNullableNumber(r.market_xau_usd) ?? marketXau;
                const pnlZar = toNullableNumber(r.gold_pnl_zar);
                out.push({
                    pair: 'XAU/USD',
                    net_side: toLongShort(r.gold_side, goldQtyOz),
                    net_value: Math.abs(goldQtyOz),
                    wa_rate: toNullableNumber(r.gold_wa_price),
                    current_rate: currentRate,
                    pnl_zar: pnlZar,
                });
            }
        }
        const pairRank: Record<string, number> = { 'XAU/USD': 0, 'USD/ZAR': 1 };
        out.sort((a, b) => (pairRank[asText(a.pair, '')] ?? 99) - (pairRank[asText(b.pair, '')] ?? 99));
        return out;
    }, [rows, marketFx, marketXau]);

    if (loading) return <><Loading text="Loading open positions revaluation..." />{Toast}</>;
    if (loadError) return <><Empty title="Could not load revaluation" sub={loadError} />{Toast}</>;

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Open Positions Reval</h2>
                    <div className="page-subtitle">
                        Unallocated PMX trades netted by pair and revalued at current market rates
                    </div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(true); }} disabled={refreshing}>
                        {refreshing ? 'Refreshing...' : 'Refresh'}
                    </button>
                </div>
            </div>

            <div className="stat-grid dashboard-stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Open Pairs</div>
                    <div className="stat-value">{fmt(summary.open_trades, 0)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Current Gold ($/oz)</div>
                    <div className="stat-value">{marketXau !== null ? fmt(marketXau, 4) : '--'}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Current FX (USD/ZAR)</div>
                    <div className="stat-value">{marketFx !== null ? fmt(marketFx, 5) : '--'}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Total PnL (ZAR)</div>
                    <div className={`stat-value ${numClass(totalPnlZar).replace('num ', '')}`}>
                        {totalPnlZar !== null ? `R${fmt(totalPnlZar, 2)}` : '--'}
                    </div>
                </div>
            </div>

            <div className="section mt-3">
                <div className="section-title">Download Report</div>
                <button className="btn btn-sm btn-primary" onClick={() => { void downloadReport(); }} disabled={downloadingReport}>
                    {downloadingReport ? 'Generating PDF...' : 'Download Open Positions Reval (PDF)'}
                </button>
            </div>

            {openNetRows.length === 0 ? (
                <Empty title="No open PMX positions" sub="No unallocated pair has a non-zero net exposure." />
            ) : (
                <DataTable
                    columns={[
                        { key: 'pair', label: 'Pair' },
                        { key: 'net_side', label: 'Net Side' },
                        { key: 'net_value', label: 'Net Value' },
                        { key: 'wa_rate', label: 'Weighted Avg Rate' },
                        { key: 'current_rate', label: 'Current Rate' },
                        { key: 'pnl_zar', label: 'Current PnL (ZAR)' },
                    ]}
                    data={openNetRows}
                    renderCell={(row, key) => {
                        const safeRow = (row && typeof row === 'object') ? row as Row : {};
                        const pairName = asText(safeRow.pair, '').toUpperCase();
                        if (key === 'net_side') {
                            const side = asText(safeRow.net_side, '--').toUpperCase();
                            if (side === 'LONG') return <span className="positive" style={{ fontWeight: 700 }}>{side}</span>;
                            if (side === 'SHORT') return <span className="negative" style={{ fontWeight: 700 }}>{side}</span>;
                            return side || '--';
                        }
                        if (key === 'net_value') {
                            const value = toNullableNumber(safeRow.net_value);
                            if (value === null) return '--';
                            if (pairName === 'USD/ZAR') return `${fmt(value, 2)} USD`;
                            if (pairName === 'XAU/USD') return `${fmt(value, 4)} oz`;
                            return fmt(value, 2);
                        }
                        if (key === 'wa_rate') {
                            const value = toNullableNumber(safeRow.wa_rate);
                            if (value === null) return '--';
                            if (pairName === 'USD/ZAR') return `R${fmt(value, 5)}`;
                            if (pairName === 'XAU/USD') return `$${fmt(value, 4)}/oz`;
                            return fmt(value, 4);
                        }
                        if (key === 'current_rate') {
                            const value = toNullableNumber(safeRow.current_rate);
                            if (value === null) return '--';
                            if (pairName === 'USD/ZAR') return `R${fmt(value, 5)}`;
                            if (pairName === 'XAU/USD') return `$${fmt(value, 4)}/oz`;
                            return fmt(value, 4);
                        }
                        if (key === 'pnl_zar') {
                            const value = toNullableNumber(safeRow.pnl_zar);
                            if (value === null) return '--';
                            return <span className={numClass(value)}>{`R${fmt(value, 2)}`}</span>;
                        }
                        return undefined;
                    }}
                />
            )}
            <div className="section open-reval-daily-totals">
                <div className="section-title">TradeMC Daily Totals ({tradeMCSummary.todayKey})</div>
                <div className="stat-grid">
                    <div className="stat-card">
                        <div className="stat-label">Daily Buys</div>
                        <div className={`stat-value ${numClass(tradeMCSummary.buyTotalG).replace('num ', '')}`}>
                            {fmt(tradeMCSummary.buyTotalG, 2)}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Daily Sells</div>
                        <div className={`stat-value ${numClass(tradeMCSummary.sellTotalG).replace('num ', '')}`}>
                            {fmt(tradeMCSummary.sellTotalG, 2)}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Daily Trades Counted</div>
                        <div className="stat-value">{fmt(tradeMCSummary.countedTrades, 0)}</div>
                    </div>
                </div>
                {tradeMCLoadError && <div className="stat-sub" style={{ color: 'var(--danger)' }}>{tradeMCLoadError}</div>}
            </div>

            <div className="section reval-account-section">
                <div className="section-title">Account Balance Recon</div>
                <div className="filter-bar">
                    <div className="filter-group">
                        <label>From</label>
                        <input
                            type="date"
                            value={reconStartDate}
                            onChange={e => setReconStartDate(e.target.value)}
                        />
                    </div>
                    <div className="filter-group">
                        <label>To</label>
                        <input
                            type="date"
                            value={reconEndDate}
                            onChange={e => setReconEndDate(e.target.value)}
                        />
                    </div>
                    {reconLoading && <div className="stat-sub">Loading...</div>}
                </div>
                <div className="filter-bar">
                    <div className="filter-group">
                        <label>Opening USD (LC)</label>
                        <input
                            type="text"
                            placeholder="0.00"
                            value={openingUSD}
                            onChange={e => setOpeningUSD(e.target.value)}
                        />
                    </div>
                    <div className="filter-group">
                        <label>Opening XAU (oz)</label>
                        <input
                            type="text"
                            placeholder="0.0000"
                            value={openingXAU}
                            onChange={e => setOpeningXAU(e.target.value)}
                        />
                    </div>
                    <div className="filter-group">
                        <label>Opening ZAR</label>
                        <input
                            type="text"
                            placeholder="0.00"
                            value={openingZAR}
                            onChange={e => setOpeningZAR(e.target.value)}
                        />
                    </div>
                    <button
                        className="btn btn-sm btn-primary"
                        onClick={() => void loadRecon(reconStartDate, reconEndDate)}
                        disabled={reconLoading}
                    >
                        {reconLoading ? 'Running...' : 'Run Recon'}
                    </button>
                </div>

                {reconError && (
                    <div className="stat-sub" style={{ color: 'var(--danger)' }}>{reconError}</div>
                )}
                {recon?.error && (
                    <div className="stat-sub" style={{ color: 'var(--warning, orange)' }}>{recon.error}</div>
                )}

                {(() => {
                    const DELTA_THRESH: Record<string, number> = { XAU: 0.0001, USD: 0.01, ZAR: 0.01 };
                    const reconCurrencies: { ccy: string; label: string; dp: number }[] = [
                        { ccy: 'USD', label: 'USD (LC)', dp: 2 },
                        { ccy: 'XAU', label: 'XAU (oz)', dp: 4 },
                        { ccy: 'ZAR', label: 'ZAR', dp: 2 },
                    ];
                    return (
                        <div className="table-container">
                            <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>Currency</th>
                                        <th className="num">Opening Balance</th>
                                        <th className="num">Net Transactions</th>
                                        <th className="num">Expected Balance</th>
                                        <th className="num">Actual Balance</th>
                                        <th className="num">Delta</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {reconCurrencies.map(({ ccy, label, dp }) => {
                                        const ccyData = recon?.currencies?.[ccy];
                                        // Opening balance comes from user-entered input fields (frontend only, no server round-trip)
                                        const openingRaw = ccy === 'USD' ? openingUSD : ccy === 'XAU' ? openingXAU : openingZAR;
                                        const openingVal = openingRaw.trim() === '' ? 0 : toNullableNumber(openingRaw.replace(/,/g, ''));
                                        const txTotal = ccyData?.transaction_total ?? null;
                                        const actualBal = ccyData?.actual_balance ?? null;
                                        // Compute expected and delta locally
                                        const expectedBal = openingVal !== null && txTotal !== null ? openingVal + txTotal : null;
                                        const delta = actualBal !== null && expectedBal !== null ? actualBal - expectedBal : null;
                                        const thresh = DELTA_THRESH[ccy] ?? 0.01;
                                        const hasDelta = delta !== null && Math.abs(delta) > thresh;
                                        const deltaClass = delta === null ? '' : (hasDelta ? 'negative' : 'positive');
                                        return (
                                            <tr key={ccy} className={hasDelta ? 'recon-row-alert' : ''}>
                                                <td><strong>{label}</strong></td>
                                                <td className={`num ${numClass(openingVal).replace('num ', '')}`}>
                                                    {openingVal !== null ? fmt(openingVal, dp) : '--'}
                                                </td>
                                                <td className={`num ${numClass(txTotal).replace('num ', '')}`}>
                                                    {txTotal !== null ? fmt(txTotal, dp) : '--'}
                                                </td>
                                                <td className={`num ${numClass(expectedBal).replace('num ', '')}`}>
                                                    {expectedBal !== null ? fmt(expectedBal, dp) : '--'}
                                                </td>
                                                <td className={`num ${numClass(actualBal).replace('num ', '')}`}>
                                                    {actualBal !== null ? fmt(actualBal, dp) : '--'}
                                                </td>
                                                <td className={`num ${deltaClass} ${hasDelta ? 'recon-delta-alert' : ''}`}>
                                                    {delta !== null ? fmt(delta, dp) : '--'}
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    );
                })()}

                <div className="stat-sub">
                    Enter opening balances above. Net transactions = sum of credits minus debits from the PMX statement.
                    Delta = Actual − Expected. Red rows indicate a balance discrepancy.
                    {recon && ` Statement period: ${recon.start_date} → ${recon.end_date}.`}
                </div>
            </div>

            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: FORWARD EXPOSURE
// ===================================================================
function ForwardExposure() {
    const FORWARD_LEDGER_ROWS_LIMIT = 50;
    const [rows, setRows] = useState<Row[]>([]);
    const [calendarRows, setCalendarRows] = useState<Row[]>([]);
    const [expandedValueDates, setExpandedValueDates] = useState<Record<string, boolean>>({});
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [loadError, setLoadError] = useState('');
    const [filters, setFilters] = usePersistentState('filters:forward_exposure', FORWARD_EXPOSURE_DEFAULT_FILTERS);
    const { show, Toast } = useToast();

    const hasActiveFilters = useMemo(() => (
        String(filters.symbol || '').trim() !== ''
        || String(filters.start_date || '').trim() !== ''
        || String(filters.end_date || '').trim() !== ''
    ), [filters]);

    const load = useCallback(async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);

        try {
            const params: Record<string, string> = {};
            if (filters.symbol) params.symbol = String(filters.symbol);
            if (filters.start_date) params.start_date = String(filters.start_date);
            if (filters.end_date) params.end_date = String(filters.end_date);

            const res = await api.getForwardExposure(params);
            setRows(Array.isArray((res as Row).rows) ? ((res as Row).rows as Row[]) : []);
            setCalendarRows(Array.isArray((res as Row).calendar) ? ((res as Row).calendar as Row[]) : []);
            setLoadError('');
        } catch (e: unknown) {
            const msg = String(e);
            setRows([]);
            setCalendarRows([]);
            setLoadError(msg);
            show(`Failed to load forward exposure: ${msg}`, 'error');
        } finally {
            if (isRefresh) setRefreshing(false);
            else setLoading(false);
        }
    }, [filters, show]);

    useEffect(() => { void load(false); }, [load]);
    useEffect(() => {
        const onAutoSync = () => { void load(true); };
        window.addEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
        return () => window.removeEventListener(PMX_AUTO_SYNC_EVENT, onAutoSync);
    }, [load]);

    const clearFilters = () => {
        setFilters({ ...FORWARD_EXPOSURE_DEFAULT_FILTERS });
    };

    const todayIso = useMemo(() => {
        const now = new Date();
        const yyyy = now.getFullYear();
        const mm = String(now.getMonth() + 1).padStart(2, '0');
        const dd = String(now.getDate()).padStart(2, '0');
        return `${yyyy}-${mm}-${dd}`;
    }, []);

    // Ledger uses only strictly future value dates.
    const rowsForLedger = useMemo(
        () => rows.filter(row => String(row.value_date || '') > todayIso),
        [rows, todayIso]
    );

    // Calendar uses only strictly future value dates.
    const calendarRowsFuture = useMemo(
        () => calendarRows.filter(row => String(row.value_date || '') > todayIso),
        [calendarRows, todayIso]
    );

    const futureExposureRows = useMemo(
        () => [...calendarRowsFuture].sort((a, b) => String(a.value_date || '').localeCompare(String(b.value_date || ''))),
        [calendarRowsFuture]
    );

    const summarizeRows = useCallback((bucketRows: Row[]) => {
        const usd = bucketRows.reduce((sum, row) => sum + (toNullableNumber(row.usd_net) ?? 0), 0);
        const gold = bucketRows.reduce((sum, row) => sum + (toNullableNumber(row.gold_net_oz) ?? 0), 0);
        const tradeSet = new Set<string>();
        for (const row of bucketRows) {
            const tn = normalizeTradeNumberValue(row.trade_num);
            const doc = asText(row.doc_number, '');
            const idKey = toNumericId(row.id) > 0 ? `ID:${toNumericId(row.id)}` : '';
            const key = tn || doc || idKey;
            if (key) tradeSet.add(key);
        }
        return {
            rows: bucketRows.length,
            trades: tradeSet.size,
            usd,
            gold,
        };
    }, []);

    const totalExposure = useMemo(() => {
        const base = summarizeRows(rowsForLedger);
        const zar = rowsForLedger.reduce((sum, row) => sum + (toNullableNumber(row.zar_flow) ?? 0), 0);
        return { ...base, zar };
    }, [rowsForLedger, summarizeRows]);

    const pmxRowsByValueDate = useMemo(() => {
        const grouped: Record<string, Row[]> = {};
        for (const row of rowsForLedger) {
            const key = asText(row.value_date, '');
            if (!key) continue;
            if (!grouped[key]) grouped[key] = [];
            grouped[key].push(row);
        }
        const symbolRank = (symbol: string): number => {
            if (symbol === 'XAUUSD') return 0;
            if (symbol === 'USDZAR') return 1;
            return 2;
        };
        for (const [valueDate, valueRows] of Object.entries(grouped)) {
            grouped[valueDate] = [...valueRows].sort((a, b) => {
                const symbolCmp = symbolRank(asText(a.symbol, '').toUpperCase()) - symbolRank(asText(b.symbol, '').toUpperCase());
                if (symbolCmp !== 0) return symbolCmp;
                const tradeCmp = toTimestampMs(b.trade_date) - toTimestampMs(a.trade_date);
                if (Number.isFinite(tradeCmp) && Math.abs(tradeCmp) > 1e-9) return tradeCmp;
                return String(b.doc_number ?? '').localeCompare(String(a.doc_number ?? ''));
            });
        }
        return grouped;
    }, [rowsForLedger]);

    useEffect(() => {
        setExpandedValueDates(prev => {
            const activeDates = new Set(futureExposureRows.map(row => asText(row.value_date, '')).filter(Boolean));
            let changed = false;
            const next: Record<string, boolean> = {};
            for (const [key, expanded] of Object.entries(prev)) {
                if (activeDates.has(key) && expanded) {
                    next[key] = true;
                } else if (expanded) {
                    changed = true;
                }
            }
            if (!changed && Object.keys(next).length === Object.keys(prev).length) {
                return prev;
            }
            return next;
        });
    }, [futureExposureRows]);

    const toggleExpanded = (valueDate: string) => {
        if (!valueDate) return;
        setExpandedValueDates(prev => ({ ...prev, [valueDate]: !prev[valueDate] }));
    };

    const computeDaysToMaturity = (calendarRow: Row, detailRows: Row[]): number | null => {
        // Days to maturity = business days from TODAY to value_date
        const businessDaysBetween = (start: Date, end: Date): number => {
            const s = new Date(start.getFullYear(), start.getMonth(), start.getDate());
            const e = new Date(end.getFullYear(), end.getMonth(), end.getDate());
            if (e.getTime() === s.getTime()) return 0;
            const forward = e.getTime() > s.getTime();
            let cur = forward ? new Date(s.getTime()) : new Date(e.getTime());
            const target = forward ? e : s;
            let count = 0;
            while (cur.getTime() < target.getTime()) {
                cur.setDate(cur.getDate() + 1);
                const dow = cur.getDay();
                if (dow !== 0 && dow !== 6) count += 1;
            }
            return forward ? count : -count;
        };
        const today = new Date();
        // Try value_date from the calendar row first
        const vd = asText(calendarRow.value_date, '');
        if (vd) {
            const value = new Date(`${vd}T00:00:00`);
            if (!Number.isNaN(value.getTime())) return businessDaysBetween(today, value);
        }
        // Fallback: use first valid detail row's value_date
        for (const d of detailRows) {
            const dvd = asText(d.value_date, '');
            if (!dvd) continue;
            const value = new Date(`${dvd}T00:00:00`);
            if (!Number.isNaN(value.getTime())) return businessDaysBetween(today, value);
        }
        return null;
    };

    if (loading) return <><Loading text="Loading forward exposure..." />{Toast}</>;
    if (loadError) return <><Empty title="Could not load forward exposure" sub={loadError} />{Toast}</>;

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Forward Exposure</h2>
                    <div className="page-subtitle">
                        Forward-only view (&gt; spot): only future value dates are shown.
                    </div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(true); }} disabled={refreshing}>
                        {refreshing ? 'Refreshing...' : 'Refresh'}
                    </button>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Symbol</label>
                    <select value={String(filters.symbol || '')} onChange={e => setFilters(f => ({ ...f, symbol: e.target.value }))}>
                        <option value="">All</option>
                        <option value="XAUUSD">XAU/USD</option>
                        <option value="USDZAR">USD/ZAR</option>
                    </select>
                </div>
                <div className="filter-group">
                    <label>From Value Date</label>
                    <input type="date" value={String(filters.start_date || '')} onChange={e => setFilters(f => ({ ...f, start_date: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>To Value Date</label>
                    <input type="date" value={String(filters.end_date || '')} onChange={e => setFilters(f => ({ ...f, end_date: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button className="btn btn-sm" onClick={clearFilters} disabled={!hasActiveFilters}>
                        Clear Filters
                    </button>
                </div>
            </div>

            <div className="section">
                <div className="section-title">Future Dated Exposure</div>
                <div className="table-container forward-exposure-table">
                    <table className="data-table">
                        <thead>
                            <tr>
                                <th>Value Date</th>
                                <th>Days to Maturity</th>
                                <th>Trades</th>
                                <th>Rows</th>
                                <th>USD</th>
                                <th>Gold (oz)</th>
                                <th>ZAR</th>
                                <th>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {futureExposureRows.length === 0 && (
                                <tr>
                                    <td colSpan={8} style={{ textAlign: 'left', padding: '2.5rem', color: 'var(--text-muted)' }}>
                                        No future-dated exposure rows
                                    </td>
                                </tr>
                            )}
                            {futureExposureRows.map((row, idx) => {
                                const valueDate = asText(row.value_date, '');
                                const detailRows = pmxRowsByValueDate[valueDate] || [];
                                const expanded = valueDate ? Boolean(expandedValueDates[valueDate]) : false;
                                const daysToMaturity = computeDaysToMaturity(row, detailRows);
                                return (
                                    <Fragment key={`${valueDate || 'row'}-${idx}`}>
                                        <tr>
                                            <td>{fmtDate(valueDate)}</td>
                                            <td className="num">{daysToMaturity !== null ? fmt(daysToMaturity, 0) : '--'}</td>
                                            <td className="num">{fmt(row.trade_numbers, 0)}</td>
                                            <td className="num">{fmt(row.trade_count, 0)}</td>
                                            <td className={numClass(row.usd_net)}>{fmt(row.usd_net, 2)}</td>
                                            <td className={numClass(row.gold_net_oz)}>{fmt(row.gold_net_oz, 4)}</td>
                                            <td className={numClass(row.zar_flow)}>{fmt(row.zar_flow, 2)}</td>
                                            <td className="forward-expand-cell">
                                                <button
                                                    className="btn btn-sm"
                                                    onClick={() => toggleExpanded(valueDate)}
                                                    disabled={!valueDate || detailRows.length === 0}
                                                >
                                                    {expanded ? 'Collapse' : 'Expand'}
                                                </button>
                                            </td>
                                        </tr>
                                        {expanded && (
                                            <tr>
                                                <td colSpan={8} className="forward-detail-cell">
                                                    <div className="forward-ledger-title">Trade Details ({valueDate})</div>
                                                    {detailRows.length > FORWARD_LEDGER_ROWS_LIMIT && (
                                                        <div className="forward-ledger-sub">
                                                            Showing latest {FORWARD_LEDGER_ROWS_LIMIT} rows
                                                        </div>
                                                    )}
                                                    <DataTable
                                                        columns={[
                                                            { key: 'trade_num', label: 'Trade #' },
                                                            { key: 'trade_date', label: 'Trade Date' },
                                                            { key: 'value_date', label: 'Value Date' },
                                                            { key: 'days_from_spot', label: 'Days to Maturity' },
                                                            { key: 'symbol', label: 'Symbol' },
                                                            { key: 'side', label: 'Side' },
                                                            { key: 'quantity', label: 'Quantity' },
                                                            { key: 'price', label: 'Price' },
                                                            { key: 'usd_net', label: 'USD Net' },
                                                            { key: 'gold_net_oz', label: 'Gold Net (oz)' },
                                                            { key: 'zar_flow', label: 'ZAR Flow' },
                                                            { key: 'doc_number', label: 'Doc #' },
                                                        ]}
                                                        data={detailRows.slice(0, FORWARD_LEDGER_ROWS_LIMIT)}
                                                        numericCols={['days_from_spot', 'quantity', 'price', 'usd_net', 'gold_net_oz', 'zar_flow']}
                                                        dateCols={['trade_date', 'value_date']}
                                                        formatters={{
                                                            days_from_spot: { decimals: 0 },
                                                            quantity: { decimals: 4 },
                                                            price: { decimals: 5 },
                                                            usd_net: { decimals: 2 },
                                                            gold_net_oz: { decimals: 4 },
                                                            zar_flow: { decimals: 2, prefix: 'R' },
                                                        }}
                                                    />
                                                </td>
                                            </tr>
                                        )}
                                    </Fragment>
                                );
                            })}
                            {futureExposureRows.length > 0 && (
                                <tr className="forward-total-row">
                                    <td>Total Exposure</td>
                                    <td>--</td>
                                    <td className="num">{fmt(totalExposure.trades, 0)}</td>
                                    <td className="num">{fmt(totalExposure.rows, 0)}</td>
                                    <td className={numClass(totalExposure.usd)}>{fmt(totalExposure.usd, 2)}</td>
                                    <td className={numClass(totalExposure.gold)}>{fmt(totalExposure.gold, 4)}</td>
                                    <td className={numClass(totalExposure.zar)}>{fmt(totalExposure.zar, 2)}</td>
                                    <td></td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: OPEN POSITIONS
// ===================================================================
function OpenPositions() {
    const [positions, setPositions] = useState<Row[]>([]);
    const [summary, setSummary] = useState<Record<string, number>>({});
    const [loading, setLoading] = useState(true);
    const [tradeSearch, setTradeSearch] = usePersistentState('filters:open_positions:trade_search', '');

    const normalizeTradeNumber = (value: unknown): string => {
        if (value === null || value === undefined) return '';
        let text = String(value).trim();
        if (text.endsWith('.0') && /^\d+\.0$/.test(text)) {
            text = text.slice(0, -2);
        }
        return text.toUpperCase();
    };

    useEffect(() => {
        (async () => {
            setLoading(true);
            const res = await api.getOpenPositions();
            setPositions(res.positions);
            setSummary(res.summary);
            setLoading(false);
        })();
    }, []);

    if (loading) return <Loading />;

    const tradeSearchNorm = normalizeTradeNumber(tradeSearch);
    const filteredPositions = tradeSearchNorm
        ? positions.filter(r => {
            const tradeNum = normalizeTradeNumber(r['Trade #']);
            return tradeNum === tradeSearchNorm;
        })
        : positions;

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Open Positions</h2>
                    <div className="page-subtitle">
                        Unallocated PMX trades netted by pair (allocated trades excluded)
                    </div>
                </div>
            </div>

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Open Pairs</div>
                    <div className="stat-value">{summary.open_trades ?? 0}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">USD Exposure</div>
                    <div className={`stat-value ${(summary.open_usd ?? 0) >= 0 ? 'positive' : 'negative'}`}>${fmt(summary.open_usd)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">ZAR Exposure</div>
                    <div className={`stat-value ${(summary.open_zar ?? 0) >= 0 ? 'positive' : 'negative'}`}>R{fmt(summary.open_zar)}</div>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Pair</label>
                    <input
                        placeholder="e.g. XAU/USD"
                        value={tradeSearch}
                        onChange={e => setTradeSearch(e.target.value)}
                    />
                </div>
            </div>

            {positions.length === 0 ? <Empty title="All positions closed" /> : (
                <DataTable
                    columns={[
                        { key: 'Trade #', label: 'Trade #' },
                        { key: 'Trade Date', label: 'Date' },
                        { key: 'Symbol', label: 'Symbol' },
                        { key: 'Side', label: 'Side' },
                        { key: 'Narration', label: 'Narration' },
                        { key: 'Balance USD', label: 'Balance USD' },
                        { key: 'Balance ZAR', label: 'Balance ZAR' },
                        { key: 'Net XAU g', label: 'Net Au g' },
                    ]}
                    data={filteredPositions}
                    numericCols={['Balance USD', 'Balance ZAR', 'Net XAU g']}
                    dateCols={['Trade Date']}
                />
            )}
        </div>
    );
}

// ===================================================================
// TAB: TRADEMC TRADES
// ===================================================================
function TradeMCTrades() {
    const PAGE_SIZE = 50;
    const GRAMS_PER_TROY_OUNCE = 31.1035;
    const [data, setData] = useState<Row[]>([]);
    const [companies, setCompanies] = useState<Row[]>([]);
    const [loading, setLoading] = useState(true);
    const [syncing, setSyncing] = useState(false);
    const [hardSyncing, setHardSyncing] = useState(false);
    const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
    const [filters, setFilters] = usePersistentState('filters:trademc', TRADEMC_DEFAULT_FILTERS);
    const { show, Toast } = useToast();
    const [pageError, setPageError] = useState('');
    const [priceWarnings, setPriceWarnings] = useState<Array<{ id: string; label: string; message: string; rowId: number }>>([]);
    const [ackedWarnings, setAckedWarnings] = useState<Record<string, boolean>>(() => readAckedPriceWarnings());
    const hasActiveFilters = useMemo(
        () => Object.values(filters).some((value) => String(value ?? '').trim() !== ''),
        [filters]
    );

    const load = useCallback(async () => {
        setLoading(true);
        setPageError('');
        setVisibleCount(PAGE_SIZE);
        try {
            const params: Record<string, string> = {};
            if (filters.status) params.status = filters.status;
            if (filters.ref_filter) params.ref_filter = filters.ref_filter;
            if (filters.company_id) params.company_id = filters.company_id;
            if (filters.start_date) params.start_date = filters.start_date;
            if (filters.end_date) params.end_date = filters.end_date;
            const [rows, cos] = await Promise.all([
                api.getTradeMCTrades(Object.keys(params).length ? params : undefined),
                api.getCompanies(),
            ]);
            setData(rows);
            setCompanies(cos);
            setPageError('');
        } catch (e: unknown) { setPageError(String(e)); }
        setLoading(false);
    }, [filters]);

    const priceThreshold = 0.025;
    const buildWarnings = useCallback((rows: Row[]) => {
        const warnings: Array<{ id: string; label: string; message: string; rowId: number }> = [];
        const sorted = [...rows].sort((a, b) => {
            const ta = toTimestampMs(a.trade_timestamp);
            const tb = toTimestampMs(b.trade_timestamp);
            if (Number.isNaN(ta) && Number.isNaN(tb)) return 0;
            if (Number.isNaN(ta)) return 1;
            if (Number.isNaN(tb)) return -1;
            return ta - tb;
        });
        let prevFx: number | null = null;
        let prevXau: number | null = null;
        for (const row of sorted) {
            const rowId = toNumericId(row.id);
            if (rowId <= 0) continue;
            const tradeLabel = asText(row.ref_number ?? row['Ref #'] ?? rowId, String(rowId));
            const fx = toNullableNumber(row.zar_to_usd_confirmed ?? row.zar_to_usd);
            const xau = toNullableNumber(row.usd_per_troy_ounce_confirmed ?? row.usd_per_troy_ounce);
            if (fx !== null && prevFx !== null) {
                const delta = Math.abs(fx - prevFx) / Math.max(Math.abs(prevFx), 1e-12);
                if (delta > priceThreshold) {
                    warnings.push({
                        id: `fx:${rowId}`,
                        rowId,
                        label: `TradeMC ${tradeLabel}`,
                        message: `USD/ZAR moved ${(delta * 100).toFixed(2)}% versus the previous TradeMC price.`,
                    });
                }
            }
            if (xau !== null && prevXau !== null) {
                const delta = Math.abs(xau - prevXau) / Math.max(Math.abs(prevXau), 1e-12);
                if (delta > priceThreshold) {
                    warnings.push({
                        id: `xau:${rowId}`,
                        rowId,
                        label: `TradeMC ${tradeLabel}`,
                        message: `Gold price moved ${(delta * 100).toFixed(2)}% versus the previous TradeMC price.`,
                    });
                }
            }
            if (fx !== null) prevFx = fx;
            if (xau !== null) prevXau = xau;
        }
        return warnings;
    }, []);

    useEffect(() => { load(); }, [load]);
    useEffect(() => {
        const onAutoSync = () => {
            void load();
        };
        window.addEventListener(TRADEMC_AUTO_SYNC_EVENT, onAutoSync);
        return () => window.removeEventListener(TRADEMC_AUTO_SYNC_EVENT, onAutoSync);
    }, [load]);

    useEffect(() => {
        const onAck = () => {
            setAckedWarnings(readAckedPriceWarnings());
        };
        window.addEventListener(PRICE_WARNING_ACK_EVENT, onAck);
        return () => window.removeEventListener(PRICE_WARNING_ACK_EVENT, onAck);
    }, []);

    const acknowledgeWarnings = (ids: string[]) => {
        if (ids.length === 0) return;
        setAckedWarnings(prev => {
            const next = { ...prev };
            for (const id of ids) next[id] = true;
            persistAckedPriceWarnings(next);
            if (typeof window !== 'undefined') {
                window.dispatchEvent(new CustomEvent(PRICE_WARNING_ACK_EVENT));
            }
            return next;
        });
    };

    const sync = async () => {
        setHardSyncing(true);
        try {
            // Default to incremental sync for speed while still pulling remote updates.
            const startRes = await api.syncTradeMC({ wait: true, incremental: true, replace: false });
            const startPayload = (startRes as Row) || {};
            const hasImmediateResult = Boolean(startPayload.trades || startPayload.companies);

            const poll = async (): Promise<Row> => {
                for (let i = 0; i < 200; i++) {  // max ~10 min
                    await new Promise(r => setTimeout(r, 3000));
                    const statusPayload = await api.getTradeMCSyncStatus();
                    const status = (statusPayload as Row) || {};
                    const isRunning = Boolean(status.running);
                    const result = status.result as Row | undefined;
                    if (!isRunning && result) return result;
                }
                throw new Error('Sync timed out after 10 minutes');
            };

            let result: Row;
            if (hasImmediateResult) {
                result = startPayload;
            } else {
                const syncStatus = asText(startPayload.status, '').toLowerCase();
                if (syncStatus === 'already_running') show('TradeMC sync is already running...', 'success');
                else if (syncStatus === 'started') show('TradeMC sync started...', 'success');
                result = await poll();
            }

            const syncError = asText(result.error, '');
            if (syncError) throw new Error(syncError);

            const tradeResult = (result.trades as Row) || {};
            if (tradeResult.success === false) {
                const verify = (tradeResult.verification as Row) || {};
                const mismatchIdsRaw = verify.field_mismatch_ids;
                const mismatchIds = Array.isArray(mismatchIdsRaw)
                    ? mismatchIdsRaw.map(v => asText(v, '')).filter(Boolean).slice(0, 5)
                    : [];
                const detail = mismatchIds.length > 0
                    ? ` Mismatch IDs: ${mismatchIds.join(', ')}`
                    : '';
                throw new Error(`${asText(tradeResult.error, 'TradeMC sync failed.')}${detail}`);
            }

            const tc = Number(tradeResult.count ?? 0);
            const inserted = Number(tradeResult.inserted ?? 0);
            const updated = Number(tradeResult.updated ?? 0);
            const removed = Number(tradeResult.removed ?? 0);
            const cc = Number(((result.companies as Row) || {}).count ?? 0);
            const remoteMaxId = Number((((tradeResult.remote_snapshot as Row) || {}).max_id) ?? 0);
            const syncMode = asText(result.mode ?? tradeResult.mode, '').toLowerCase();
            const syncLabel = syncMode === 'incremental'
                ? 'incremental sync'
                : (syncMode === 'full_replace' ? 'full replace' : 'full sync');
            const warningRaw = tradeResult.warnings;
            const warnings = Array.isArray(warningRaw)
                ? warningRaw.map(v => asText(v, '')).filter(Boolean)
                : [];
            show(
                `TradeMC ${syncLabel}: ${tc.toLocaleString()} changed (${inserted.toLocaleString()} inserted, ${updated.toLocaleString()} updated, ${removed.toLocaleString()} removed), ${cc.toLocaleString()} companies${remoteMaxId > 0 ? `, remote max ID ${remoteMaxId.toLocaleString()}` : ''}`,
                'success'
            );
            setPageError('');
            if (warnings.length > 0) {
                show(`TradeMC sync warning: ${warnings[0]}`, 'error');
            }
            await load();
        } catch (e: unknown) { setPageError(String(e)); }
        setHardSyncing(false);
    };

    const cols = [
        { key: 'ref_number', label: 'Ref #' },
        { key: 'company_name', label: 'Company' },
        { key: 'weight', label: 'Weight (g)' },
        { key: 'usd_per_troy_ounce_confirmed', label: 'USD/oz' },
        { key: 'zar_to_usd_confirmed', label: 'ZAR/USD' },
        { key: 'zar_per_gram', label: 'ZAR/g' },
        { key: 'zar_per_gram_less_refining', label: 'ZAR/g Less Refining' },
        { key: 'trade_timestamp', label: 'Date' },
        { key: 'notes', label: 'Sage Reference' },
        { key: 'id', label: 'ID' },
        { key: 'status', label: 'Status' },
    ];
    const companyRefiningRateById = useMemo(() => {
        const map = new Map<number, number>();
        for (const company of companies) {
            const id = toNumericId(company.id);
            const refiningRate = toNullableNumber(company.refining_rate);
            if (id > 0 && refiningRate !== null) {
                map.set(id, refiningRate);
            }
        }
        return map;
    }, [companies]);
    const tableData = useMemo(() => data.map((row) => {
        const fxRate = toNullableNumber(row.zar_to_usd_confirmed ?? row.zar_to_usd);
        let usdPerOz = toNullableNumber(row.usd_per_troy_ounce_confirmed ?? row.usd_per_troy_ounce);
        if (usdPerOz === null) {
            const zarPerOz = toNullableNumber(row.zar_per_troy_ounce_confirmed ?? row.zar_per_troy_ounce);
            if (zarPerOz !== null && fxRate !== null && Math.abs(fxRate) > 1e-12) {
                usdPerOz = zarPerOz / fxRate;
            }
        }

        const zarPerGram = (usdPerOz !== null && fxRate !== null)
            ? ((usdPerOz * fxRate) / GRAMS_PER_TROY_OUNCE)
            : null;
        const rowRefiningRate = toNullableNumber(row.company_refining_rate);
        const companyId = toNumericId(row.company_id);
        const refiningRate = rowRefiningRate ?? companyRefiningRateById.get(companyId) ?? 0;
        const zarPerGramLessRefining = zarPerGram !== null
            ? (zarPerGram * (1 - (refiningRate / 100)))
            : null;

        return {
            ...row,
            usd_per_troy_ounce_confirmed: usdPerOz,
            zar_to_usd_confirmed: fxRate,
            zar_per_gram: zarPerGram,
            zar_per_gram_less_refining: zarPerGramLessRefining,
        };
    }), [data, companyRefiningRateById]);

    useEffect(() => {
        const warnings = buildWarnings(tableData);
        const activeWarnings = warnings.filter(w => !ackedWarnings[w.id]);
        setPriceWarnings(activeWarnings);
        persistAckedPriceWarnings(ackedWarnings);
        if (typeof window !== 'undefined') {
            window.dispatchEvent(new CustomEvent(PRICE_WARNING_EVENT, { detail: { warnings: activeWarnings } }));
        }
    }, [ackedWarnings, buildWarnings, tableData]);
    const visibleData = tableData.slice(0, visibleCount);
    const hasMoreRows = visibleCount < tableData.length;
    useEffect(() => {
        if (typeof window === 'undefined') return;
        const hasMissing = tableData.some((row) => {
            const r = row as Row;
            const weight = toNullableNumber(r.weight ?? r.Weight ?? r.qty ?? r.quantity ?? r.Quantity);
            const sageRef = asText(
                r.notes ?? r.Notes ?? r.sage_reference ?? r.sageReference ?? r.sage_ref,
                ''
            );
            const refNumber = asText(r.ref_number ?? r.refNumber ?? r['Ref #'], '');
            const missingSage = !sageRef || ['-', '--', 'n/a', 'na'].includes(sageRef.toLowerCase());
            const missingRef = !refNumber || ['-', '--', 'n/a', 'na'].includes(refNumber.toLowerCase());
            return weight !== null && weight > 0 && (missingSage || missingRef);
        });
        window.dispatchEvent(new CustomEvent(TRADEMC_MISSING_SAGE_EVENT, { detail: { hasMissing } }));
    }, [tableData]);

    const csvEscape = (value: unknown): string => {
        if (value === null || value === undefined) return '';
        const text = String(value);
        if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
        return text;
    };

    const formatTradeMCCsvValue = (row: Row, key: string): string => {
        if (key === 'trade_timestamp') return fmtDate(row[key]);
        if (key === 'id') {
            const id = toNumericId(row[key]);
            return id > 0 ? String(id) : '';
        }

        if (key === 'weight' || key === 'zar_per_gram' || key === 'zar_per_gram_less_refining') {
            const n = toNullableNumber(row[key]);
            return n === null ? '' : n.toFixed(2);
        }
        if (key === 'usd_per_troy_ounce_confirmed' || key === 'zar_to_usd_confirmed') {
            const n = toNullableNumber(row[key]);
            return n === null ? '' : n.toFixed(4);
        }

        const raw = row[key];
        return raw === null || raw === undefined ? '' : String(raw);
    };

    const downloadFilteredTradeMCCsv = () => {
        if (tableData.length === 0) {
            show('No TradeMC rows to download', 'error');
            return;
        }
        const keys = cols.map(c => c.key);
        const headers = cols.map(c => c.label);
        const lines = [headers.map(csvEscape).join(',')];
        for (const row of tableData) {
            lines.push(keys.map(key => csvEscape(formatTradeMCCsvValue(row, key))).join(','));
        }
        const csv = lines.join('\n');
        const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        triggerBlobDownload(blob, `trademc_filtered_${stamp}.csv`);
    };

    const clearFilters = () => {
        setFilters({ ...TRADEMC_DEFAULT_FILTERS });
    };

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>TradeMC Trades</h2>
                    <div className="page-subtitle">{data.length.toLocaleString()} trades</div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={load} disabled={syncing || hardSyncing}>Refresh</button>
                    <button className="btn btn-sm btn-primary" onClick={() => { void sync(); }} disabled={syncing || hardSyncing}>
                        {hardSyncing ? 'Syncing...' : 'Sync from API'}
                    </button>
                    <button className="btn btn-sm" onClick={downloadFilteredTradeMCCsv} disabled={loading || tableData.length === 0}>
                        Download CSV
                    </button>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Status</label>
                    <select value={filters.status} onChange={e => setFilters(f => ({ ...f, status: e.target.value }))}>
                        <option value="">All</option>
                        <option value="confirmed">Confirmed</option>
                        <option value="pending">Pending</option>
                    </select>
                </div>
                <div className="filter-group">
                    <label>Reference</label>
                    <input placeholder="e.g. P1019" value={filters.ref_filter} onChange={e => setFilters(f => ({ ...f, ref_filter: e.target.value }))} />
                </div>
                <div className="filter-group">
                    <label>Company</label>
                    <select value={filters.company_id} onChange={e => setFilters(f => ({ ...f, company_id: e.target.value }))}>
                        <option value="">All Companies</option>
                        {companies.map(c => <option key={String(c.id)} value={String(c.id)}>{String(c.company_name || `Company ${c.id}`)}</option>)}
                    </select>
                </div>
                <div className="filter-group">
                    <label>From Date</label>
                    <input
                        type="date"
                        value={filters.start_date}
                        onChange={e => setFilters(f => ({ ...f, start_date: e.target.value }))}
                    />
                </div>
                <div className="filter-group">
                    <label>To Date</label>
                    <input
                        type="date"
                        value={filters.end_date}
                        onChange={e => setFilters(f => ({ ...f, end_date: e.target.value }))}
                    />
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button className="btn btn-sm" onClick={clearFilters} disabled={!hasActiveFilters}>
                        Clear Filters
                    </button>
                </div>
            </div>
            {pageError && <div className="stat-sub" style={{ color: 'var(--danger)', marginTop: '0.5rem' }}>{pageError}</div>}

            {loading ? <Loading /> : (
                <>
                    {priceWarnings.length > 0 && (
                        <div className="warning-banner warning-banner-danger">
                            <div>
                                <strong>Price check warning</strong>
                                <div>One or more TradeMC prices moved more than 0.5% versus the previous trade.</div>
                            </div>
                            <button className="btn btn-sm" onClick={() => acknowledgeWarnings(priceWarnings.map(w => w.id))}>
                                Ack
                            </button>
                        </div>
                    )}
                    <DataTable columns={cols} data={visibleData}
                        numericCols={[
                            'weight',
                            'zar_to_usd_confirmed',
                            'usd_per_troy_ounce_confirmed',
                            'zar_per_gram',
                            'zar_per_gram_less_refining',
                        ]}
                        formatters={{
                            zar_per_gram: { decimals: 2 },
                            zar_per_gram_less_refining: { decimals: 2 },
                        }}
                        dateCols={['trade_timestamp']}
                        rowClassName={(row) => {
                            const hasSageReference = String(row.notes ?? '').trim() !== '';
                            const weight = toNullableNumber(row.weight);
                            const isNegativeTrade = weight !== null && weight < 0;
                            const rowId = toNumericId(row.id);
                            const hasPriceWarning = priceWarnings.some(w => w.rowId === rowId);
                            return [hasSageReference || isNegativeTrade ? 'trademc-has-sage-ref' : '', hasPriceWarning ? 'trademc-price-warning' : ''].filter(Boolean).join(' ');
                        }}
                        renderCell={(row, key) => {
                            if (key !== 'ref_number') return undefined;
                            const rowId = toNumericId(row['id']);
                            const currentRef = String(row['ref_number'] ?? '');
                            if (rowId <= 0) return currentRef || '--';
                            return (
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                                    <EditableTradeNum
                                        value={currentRef}
                                        rowId={rowId}
                                        label="Ref #"
                                        onSaved={(newVal) => {
                                            setData(prev => prev.map(r =>
                                                toNumericId(r['id']) === rowId ? { ...r, ref_number: newVal } : r
                                            ));
                                            show(`Ref # updated to "${newVal}" on TradeMC`, 'success');
                                            void load();
                                        }}
                                        onError={(msg) => show(msg, 'center-error')}
                                        saveTradeNumber={async (id, tradeNumber) => {
                                            const res = await api.updateTradeMCRefNumber(id, tradeNumber);
                                            const savedRef = asText((res as Row).ref_number, tradeNumber);
                                            return { ok: true, savedValue: savedRef };
                                        }}
                                    />
                                    {priceWarnings.some(w => w.rowId === rowId) && <span className="price-warning-pill">!</span>}
                                </span>
                            );
                        }}
                    />
                    {hasMoreRows && (
                        <div style={{ textAlign: 'center', padding: '1rem' }}>
                            <button className="btn btn-sm" onClick={() => setVisibleCount(v => v + PAGE_SIZE)}>
                                Show More ({Math.min(PAGE_SIZE, tableData.length - visibleCount).toLocaleString()} more)
                            </button>
                        </div>
                    )}
                </>
            )}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: HEDGING
// ===================================================================
function GoldHedging() {
    const DEFAULT_METAL_TOLERANCE_G = 32.0;
    const GRAMS_PER_TROY_OUNCE = 31.1035;
    const [data, setData] = useState<Row[]>([]);
    const [loading, setLoading] = useState(true);
    const [loadError, setLoadError] = useState('');
    const [tradeSearch, setTradeSearch] = usePersistentState('filters:hedging:trade_search', '');
    const [metalTolerance, setMetalTolerance] = usePersistentState('filters:hedging:metal_tolerance', DEFAULT_METAL_TOLERANCE_G);
    const [usdTolerance, setUsdTolerance] = usePersistentState('filters:hedging:usd_tolerance', 1.0);
    const [hedgeStatusFilter, setHedgeStatusFilter] = usePersistentState('filters:hedging:status', '');
    const [expandedTrades, setExpandedTrades] = useState<Record<string, boolean>>({});
    const [tradeDetails, setTradeDetails] = useState<Record<string, {
        loading: boolean;
        loaded: boolean;
        error: string;
        trades: Row[];
        trademc: Row[];
        wa: Row | null;
    }>>({});
    const { show, Toast } = useToast();

    const loadTradeDetails = useCallback(async (tradeNumRaw: string) => {
        const tradeNum = normalizeTradeNumberValue(tradeNumRaw);
        if (!tradeNum) return;

        setTradeDetails(prev => ({
            ...prev,
            [tradeNum]: {
                loading: true,
                loaded: false,
                error: '',
                trades: prev[tradeNum]?.trades || [],
                trademc: prev[tradeNum]?.trademc || [],
                wa: prev[tradeNum]?.wa || null,
            },
        }));

        try {
            const [rowsRaw, waRaw, tmRaw, ticketRaw] = await Promise.all([
                api.getPmxLedger({ trade_num: tradeNum }),
                api.getWeightedAverage(tradeNum).catch(() => null),
                api.getTradeMCTrades({ ref_filter: tradeNum }).catch(() => []),
                api.getTicket(tradeNum).catch(() => null),
            ]);

            const rows = (Array.isArray(rowsRaw) ? rowsRaw : []).filter(
                row => normalizeTradeNumberValue((row as Row)['Trade #']) === tradeNum
            );
            const ticketStonexRows = (ticketRaw && Array.isArray((ticketRaw as Row).stonex))
                ? ((ticketRaw as Row).stonex as Row[])
                : [];
            const tradeNumKey = normalizeTradeNumberValue(tradeNum).replace(/[^A-Z0-9]/g, '');
            const trademcRows = (Array.isArray(tmRaw) ? tmRaw : []).filter((row) => {
                const refRaw = normalizeTradeNumberValue((row as Row).ref_number);
                const refKey = refRaw.replace(/[^A-Z0-9]/g, '');
                return Boolean(refKey) && refKey === tradeNumKey;
            });

            setTradeDetails(prev => ({
                ...prev,
                [tradeNum]: {
                    loading: false,
                    loaded: true,
                    error: '',
                    // Prefer ticket PMX rows (has Quantity/Price) for quick hedge details.
                    trades: ticketStonexRows.length > 0 ? ticketStonexRows : (rows as Row[]),
                    trademc: trademcRows as Row[],
                    wa: (waRaw as Row | null) || null,
                },
            }));
        } catch (e: unknown) {
            setTradeDetails(prev => ({
                ...prev,
                [tradeNum]: {
                    loading: false,
                    loaded: true,
                    error: String(e),
                    trades: [],
                    trademc: [],
                    wa: null,
                },
            }));
        }
    }, []);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            setLoading(true);
            setLoadError('');
            try {
                const rows = await api.getHedging();
                if (cancelled) return;
                setData(rows);
            } catch (e: unknown) {
                if (cancelled) return;
                const msg = String(e);
                setData([]);
                setLoadError(msg);
                show(`Failed to load hedging data: ${msg}`, 'error');
            } finally {
                if (!cancelled) setLoading(false);
            }
        })();
        return () => { cancelled = true; };
    }, [show]);
    useEffect(() => {
        // One-time migration from old default (1g) to new default (32g).
        if (Number.isFinite(metalTolerance) && Math.abs(metalTolerance - 1.0) < 1e-9) {
            setMetalTolerance(DEFAULT_METAL_TOLERANCE_G);
        }
    }, [metalTolerance, setMetalTolerance]);

    if (loading) return <><Loading text="Loading hedging data..." />{Toast}</>;
    if (loadError) return <><Empty title="Could not load hedging data" sub={loadError} />{Toast}</>;
    if (data.length === 0) return <><Empty title="No hedging data" sub="Ensure both TradeMC and PMX ledger have data" />{Toast}</>;

    const tradeSearchNorm = normalizeTradeNumberValue(tradeSearch);
    const filteredData = tradeSearchNorm
        ? data.filter(r => normalizeTradeNumberValue(r.trade_num).includes(tradeSearchNorm))
        : data;

    const toNumber = (val: unknown) => {
        if (val === null || val === undefined) return NaN;
        const s = String(val).replace(/,/g, '').trim();
        if (!s || s === '--') return NaN;
        const n = Number(s);
        return Number.isFinite(n) ? n : NaN;
    };

    const roundTo = (val: number, decimals: number) => {
        if (!Number.isFinite(val)) return NaN;
        const m = Math.pow(10, decimals);
        return Math.round(val * m) / m;
    };

    const metalTol = Number.isFinite(metalTolerance) ? Math.abs(metalTolerance) : DEFAULT_METAL_TOLERANCE_G;
    const usdTol = Number.isFinite(usdTolerance) ? Math.abs(usdTolerance) : 1.0;

    const computed = filteredData
        .map(r => {
            const tmWeightG = toNumber(r.tm_weight_g);
            const tmWeightOzRaw = toNumber(r.tm_weight_oz);
            const tmWeightOz = Number.isFinite(tmWeightOzRaw)
                ? tmWeightOzRaw
                : (Number.isFinite(tmWeightG) ? tmWeightG / 31.1035 : NaN);

            const stonexBuyOz = toNumber(r.stonex_buy_oz);
            const stonexSellOz = toNumber(r.stonex_sell_oz);
            // Display PMX hedge as BUY-positive / SELL-negative so hedge need reads:
            // TradeMC + PMX hedge.
            const pmxDisplayNetOzFromSides = Number.isFinite(stonexBuyOz) && Number.isFinite(stonexSellOz)
                ? (stonexBuyOz - stonexSellOz)
                : NaN;
            const pmxNetOzRaw = toNumber(r.pmx_net_oz ?? r.stonex_net_oz);
            const pmxDisplayNetOzFromApi = Number.isFinite(pmxNetOzRaw) ? -pmxNetOzRaw : NaN;
            const pmxNetOz = Number.isFinite(pmxDisplayNetOzFromSides)
                ? pmxDisplayNetOzFromSides
                : pmxDisplayNetOzFromApi;

            const pmxHedgeGFromNet = Number.isFinite(pmxNetOz) ? pmxNetOz * 31.1035 : NaN;
            const pmxHedgeGRaw = toNumber(r.pmx_hedge_g ?? r.stonex_hedge_g);
            const pmxHedgeGFromApi = Number.isFinite(pmxHedgeGRaw) ? -pmxHedgeGRaw : NaN;
            const pmxHedgeG = Number.isFinite(pmxHedgeGFromNet)
                ? pmxHedgeGFromNet
                : pmxHedgeGFromApi;

            const metalNeedGComputed = Number.isFinite(tmWeightG) && Number.isFinite(pmxHedgeG) ? (tmWeightG + pmxHedgeG) : NaN;
            const metalNeedGRaw = toNumber(r.hedge_need_g);
            const metalNeedG = Number.isFinite(metalNeedGComputed)
                ? metalNeedGComputed
                : (Number.isFinite(metalNeedGRaw) ? metalNeedGRaw : NaN);
            const metalNeedOzRaw = toNumber(r.metal_need_oz);
            const metalNeedOz = Number.isFinite(metalNeedOzRaw)
                ? metalNeedOzRaw
                : (Number.isFinite(tmWeightOz) && Number.isFinite(pmxNetOz) ? tmWeightOz + pmxNetOz : NaN);

            const pmxNetUsd = toNumber(r.pmx_net_usd);
            const usdToCutRaw = toNumber(r.usd_to_cut ?? r.usd_need);
            const usdToCut = Number.isFinite(usdToCutRaw)
                ? usdToCutRaw
                : (Number.isFinite(pmxNetUsd) ? Math.abs(pmxNetUsd) : NaN);

            const metalHedged = Number.isFinite(metalNeedG) && Math.abs(metalNeedG) <= (metalTol + 1e-9);
            const usdHedged = Number.isFinite(usdToCut) && Math.abs(usdToCut) <= (usdTol + 1e-9);
            const hedged = metalHedged && usdHedged;
            const status = hedged
                ? 'Hedged'
                : metalHedged
                    ? 'USD to cut'
                    : usdHedged
                        ? 'Metal to hedge'
                        : 'Metal + USD';

            return {
                ...r,
                trade_num: normalizeTradeNumberValue(r.trade_num),
                tm_latest_trade_ts: asText(r.tm_latest_trade_ts, ''),
                tm_weight_g: roundTo(tmWeightG, 2),
                tm_weight_oz: roundTo(tmWeightOz, 4),
                pmx_net_oz: roundTo(pmxNetOz, 4),
                pmx_hedge_g: roundTo(pmxHedgeG, 2),
                hedge_need_g: roundTo(metalNeedG, 2),
                metal_need_oz: roundTo(metalNeedOz, 4),
                pmx_net_usd: roundTo(pmxNetUsd, 2),
                usd_to_cut: roundTo(usdToCut, 2),
                usd_need: roundTo(usdToCut, 2),
                metal_hedged: metalHedged,
                usd_hedged: usdHedged,
                hedged,
                status,
            };
        })
        .sort((a, b) => {
            const aHedged = Boolean(a.hedged);
            const bHedged = Boolean(b.hedged);
            if (aHedged !== bHedged) return aHedged ? 1 : -1; // needs-hedging first

            const ta = normalizeTradeNumberValue(a.trade_num);
            const tb = normalizeTradeNumberValue(b.trade_num);
            const aNumMatch = ta.match(/(\d+)(?!.*\d)/);
            const bNumMatch = tb.match(/(\d+)(?!.*\d)/);
            const aNum = aNumMatch ? Number(aNumMatch[1]) : Number.NaN;
            const bNum = bNumMatch ? Number(bNumMatch[1]) : Number.NaN;
            const aFinite = Number.isFinite(aNum);
            const bFinite = Number.isFinite(bNum);
            if (aFinite && bFinite && aNum !== bNum) return bNum - aNum;
            if (aFinite !== bFinite) return bFinite ? 1 : -1;
            return tb.localeCompare(ta);
        });

    const computedView = computed.filter((r) => {
        const mode = String(hedgeStatusFilter || '').trim().toLowerCase();
        if (mode === 'hedged') return Boolean(r.hedged);
        if (mode === 'unhedged') return !Boolean(r.hedged);
        return true;
    });

    const fullyHedged = computedView.filter(r => r.hedged);
    const totalMetalGapG = computedView.reduce((sum, r) => sum + Math.abs(Number(r.hedge_need_g) || 0), 0);
    const totalMetalGapOz = totalMetalGapG / GRAMS_PER_TROY_OUNCE;
    const controlAccountG = computedView.reduce((sum, r) => {
        if (!r.hedged) return sum;
        return sum + (Number(r.hedge_need_g) || 0);
    }, 0);
    const controlAccountOz = controlAccountG / GRAMS_PER_TROY_OUNCE;
    const totalUsdGap = computedView.reduce((sum, r) => sum + (r.usd_hedged ? 0 : Math.abs(Number(r.usd_to_cut) || 0)), 0);
    const hasMetalGap = Number.isFinite(totalMetalGapG) && totalMetalGapG > (metalTol + 1e-9);
    const hasUsdGap = Number.isFinite(totalUsdGap) && totalUsdGap > (usdTol + 1e-9);

    const toggleTradeExpand = (tradeNumRaw: string) => {
        const tradeNum = normalizeTradeNumberValue(tradeNumRaw);
        if (!tradeNum) return;

        const nextExpanded = !expandedTrades[tradeNum];
        setExpandedTrades(prev => ({ ...prev, [tradeNum]: nextExpanded }));

        const details = tradeDetails[tradeNum];
        const hasTrades = Array.isArray(details?.trades) && details.trades.length > 0;
        const shouldLoad =
            !details
            || (!details.loaded && !details.loading)
            || Boolean(details?.error)
            || !hasTrades;
        if (nextExpanded && shouldLoad && !details?.loading) {
            void loadTradeDetails(tradeNum);
        }
    };

    return (
        <div>
            <div className="page-header"><h2>Hedging</h2></div>

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Trades In View</div>
                    <div className="stat-value">{computedView.length}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Fully Hedged</div>
                    <div className="stat-value positive">{fullyHedged.length}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Control Account</div>
                    <div className={`stat-value ${controlAccountG >= 0 ? 'positive' : 'negative'}`}>
                        {`${fmt(controlAccountOz, 3)} oz / ${fmt(controlAccountG, 2)} g`}
                    </div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Metal Gap Total (oz / g)</div>
                    <div className={`stat-value ${hasMetalGap ? 'negative' : 'positive'}`}>{`${fmt(totalMetalGapOz, 3)} oz / ${fmt(totalMetalGapG, 2)} g`}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">USD Remaining To Cut</div>
                    <div className={`stat-value ${hasUsdGap ? 'negative' : 'positive'}`}>${fmt(totalUsdGap, 2)}</div>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Trade #</label>
                    <input
                        placeholder="e.g. 9847"
                        value={tradeSearch}
                        onChange={e => setTradeSearch(e.target.value)}
                    />
                </div>
                <div className="filter-group">
                    <label>Metal Tol (g)</label>
                    <input
                        type="number"
                        min="0"
                        step="0.01"
                        value={Number.isFinite(metalTolerance) ? metalTolerance : 1}
                        onChange={e => setMetalTolerance(Math.abs(Number(e.target.value)))}
                    />
                </div>
                <div className="filter-group">
                    <label>USD Tol ($)</label>
                    <input
                        type="number"
                        min="0"
                        step="1"
                        value={Number.isFinite(usdTolerance) ? usdTolerance : 1}
                        onChange={e => setUsdTolerance(Math.abs(Number(e.target.value)))}
                    />
                </div>
                <div className="filter-group">
                    <label>Hedge Status</label>
                    <select
                        value={String(hedgeStatusFilter || '')}
                        onChange={e => setHedgeStatusFilter(e.target.value)}
                    >
                        <option value="">All</option>
                        <option value="hedged">Hedged</option>
                        <option value="unhedged">Unhedged</option>
                    </select>
                </div>
            </div>

            <div className="table-container">
                <table className="data-table hedging-summary-table">
                    <thead>
                        <tr>
                            <th>Trade #</th>
                            <th>TradeMC (g)</th>
                            <th>PMX Net (g)</th>
                            <th>Hedging Need (g / oz)</th>
                            <th>PMX USD Net</th>
                            <th>USD Remaining</th>
                            <th>Status</th>
                            <th>Details</th>
                        </tr>
                    </thead>
                    <tbody>
                        {computedView.length === 0 && (
                            <tr>
                                <td colSpan={8} style={{ textAlign: 'left', padding: '2.5rem', color: 'var(--text-muted)' }}>
                                    No data available
                                </td>
                            </tr>
                        )}
                        {computedView.map((row, idx) => {
                            const tradeNum = normalizeTradeNumberValue(row.trade_num);
                            const expanded = !!expandedTrades[tradeNum];
                            const details = tradeDetails[tradeNum];
                            const hedgeNeedG = Number(row.hedge_need_g);
                            const usdNeed = Number(row.usd_to_cut);
                            return (
                                <Fragment key={`${tradeNum}-${idx}`}>
                                    <tr className={row.hedged ? 'row-fully-hedged' : 'row-needs-hedging'}>
                                        <td>{tradeNum || '--'}</td>
                                        <td className={numClass(row.tm_weight_g)}>{fmt(row.tm_weight_g)}</td>
                                        <td className={numClass(row.pmx_hedge_g)}>{fmt(row.pmx_hedge_g)}</td>
                                        <td className={Number.isFinite(hedgeNeedG) && Math.abs(hedgeNeedG) > metalTol ? 'num negative' : numClass(row.hedge_need_g)}>
                                            {Number.isFinite(hedgeNeedG)
                                                ? `${fmt(hedgeNeedG, 2)} g / ${fmt(hedgeNeedG / GRAMS_PER_TROY_OUNCE, 3)} oz`
                                                : '--'}
                                        </td>
                                        <td className={numClass(row.pmx_net_usd)}>{fmt(row.pmx_net_usd)}</td>
                                        <td className={Number.isFinite(usdNeed) && Math.abs(usdNeed) > usdTol ? 'num negative' : numClass(row.usd_to_cut)}>
                                            {fmt(row.usd_to_cut)}
                                        </td>
                                        <td className="hedging-status-cell">{String(row.status ?? '--')}</td>
                                        <td className="hedging-details-cell">
                                            <button
                                                type="button"
                                                className="btn btn-sm hedging-expand-btn"
                                                onClick={() => toggleTradeExpand(tradeNum)}
                                            >
                                                {expanded ? 'Collapse' : 'Expand'}
                                            </button>
                                        </td>
                                    </tr>
                                    {expanded && (
                                        <tr className="supplier-transactions-row">
                                            <td colSpan={8}>
                                                <div className="supplier-transactions-wrap">

                                                    {!details && (
                                                        <Loading text={`Loading PMX trades for ${tradeNum}...`} />
                                                    )}

                                                    {details?.loading && (
                                                        <Loading text={`Loading PMX trades for ${tradeNum}...`} />
                                                    )}

                                                    {!details?.loading && details?.error && (
                                                        <div style={{ color: 'var(--danger-600)', paddingBottom: '0.75rem' }}>
                                                            {details.error}
                                                        </div>
                                                    )}

                                                    {!details?.loading && !details?.error && (() => {
                                                        // Use the authoritative WA values from build_weighted_average
                                                        // (signed net quantities, correct WA formula matching the WA tab).
                                                        const wa = details?.wa as Row | null | undefined;
                                                        const waGoldQty   = toNullableNumber(wa?.xau_usd_total_qty);
                                                        const waGoldPrice = toNullableNumber(wa?.xau_usd_wa_price);
                                                        const waGoldVal   = toNullableNumber(wa?.xau_usd_total_val);
                                                        const waFxQty     = toNullableNumber(wa?.usd_zar_total_qty);
                                                        const waFxPrice   = toNullableNumber(wa?.usd_zar_wa_price);
                                                        const waFxVal     = toNullableNumber(wa?.usd_zar_total_val);

                                                        const gold = {
                                                            amount: waGoldQty !== null ? Math.abs(waGoldQty) : null,
                                                            rate:   waGoldPrice,
                                                            total:  waGoldVal  !== null ? Math.abs(waGoldVal)  : null,
                                                        };
                                                        const usd = {
                                                            amount: waFxQty !== null ? Math.abs(waFxQty) : null,
                                                            rate:   waFxPrice,
                                                            total:  waFxVal !== null ? Math.abs(waFxVal) : null,
                                                        };
                                                        const quickRows = [
                                                            {
                                                                leg: 'Gold (XAU/USD)',
                                                                amount: gold.amount,
                                                                rate: gold.rate,
                                                                total: gold.total,
                                                                amountPrefix: '',
                                                                amountSuffix: ' oz',
                                                                amountDecimals: 3,
                                                                ratePrefix: '$',
                                                                rateSuffix: '',
                                                                rateDecimals: 4,
                                                                totalPrefix: '$',
                                                                totalSuffix: '',
                                                                totalDecimals: 2,
                                                                includeGrams: true,
                                                            },
                                                            {
                                                                leg: 'Dollar (USD/ZAR)',
                                                                amount: usd.amount,
                                                                rate: usd.rate,
                                                                total: usd.total,
                                                                amountPrefix: '$',
                                                                amountSuffix: '',
                                                                amountDecimals: 2,
                                                                ratePrefix: 'R',
                                                                rateSuffix: '',
                                                                rateDecimals: 5,
                                                                totalPrefix: 'R',
                                                                totalSuffix: '',
                                                                totalDecimals: 2,
                                                                includeGrams: false,
                                                            },
                                                        ];
                                                        const hasQuickRows = quickRows.some(r => r.amount !== null || r.rate !== null || r.total !== null);

                                                        return (
                                                            <>
                                                                <div className="supplier-transactions-title">
                                                                    PMX quick details for {tradeNum}
                                                                </div>
                                                                {hasQuickRows ? (
                                                                    <div className="table-container" style={{ marginBottom: '0.75rem' }}>
                                                                        <table className="data-table">
                                                                            <thead>
                                                                                <tr>
                                                                                    <th>Leg</th>
                                                                                    <th>Amount</th>
                                                                                    <th>Rate</th>
                                                                                    <th>Total</th>
                                                                                </tr>
                                                                            </thead>
                                                                            <tbody>
                                                                                {quickRows.map((quickRow) => {
                                                                                    const fmtCell = (
                                                                                        value: number | null,
                                                                                        decimals: number,
                                                                                        prefix = '',
                                                                                        suffix = '',
                                                                                    ) => (value === null ? '--' : `${prefix}${fmt(value, decimals)}${suffix}`);
                                                                                    return (
                                                                                        <tr key={`${tradeNum}-${quickRow.leg}`}>
                                                                                            <td>{quickRow.leg}</td>
                                                                                            <td className="num">
                                                                                                {quickRow.includeGrams && quickRow.amount !== null
                                                                                                    ? `${fmt(quickRow.amount, quickRow.amountDecimals)} oz / ${fmt(quickRow.amount * GRAMS_PER_TROY_OUNCE, 2)} g`
                                                                                                    : fmtCell(quickRow.amount, quickRow.amountDecimals, quickRow.amountPrefix, quickRow.amountSuffix)}
                                                                                            </td>
                                                                                            <td className="num">
                                                                                                {fmtCell(quickRow.rate, quickRow.rateDecimals, quickRow.ratePrefix, quickRow.rateSuffix)}
                                                                                            </td>
                                                                                            <td className="num">
                                                                                                {fmtCell(quickRow.total, quickRow.totalDecimals, quickRow.totalPrefix, quickRow.totalSuffix)}
                                                                                            </td>
                                                                                        </tr>
                                                                                    );
                                                                                })}
                                                                            </tbody>
                                                                        </table>
                                                                    </div>
                                                                ) : (
                                                                    <div style={{ color: 'var(--text-muted)', paddingBottom: '0.75rem' }}>
                                                                        No PMX trades found for {tradeNum}
                                                                    </div>
                                                                )}
                                                            </>
                                                        );
                                                    })()}

                                                    {!details?.loading && !details?.error && (
                                                        details?.trademc?.length ? (
                                                            <>
                                                                <div className="supplier-transactions-title" style={{ marginTop: '0.35rem' }}>
                                                                    TradeMC details for {tradeNum}
                                                                </div>
                                                                <div className="table-container" style={{ marginBottom: '0.75rem' }}>
                                                                    <table className="data-table">
                                                                        <thead>
                                                                            <tr>
                                                                                <th>Supplier</th>
                                                                                <th>Weight (g)</th>
                                                                                <th>$/oz</th>
                                                                                <th>USD/ZAR</th>
                                                                            </tr>
                                                                        </thead>
                                                                        <tbody>
                                                                            {(() => {
                                                                                const tmSummary = details.trademc.reduce<{
                                                                                    totalWeight: number;
                                                                                    usdOzWeightedSum: number;
                                                                                    usdOzWeight: number;
                                                                                    usdZarWeightedSum: number;
                                                                                    usdZarWeight: number;
                                                                                }>((acc, tmRow) => {
                                                                                    const weight = Number(tmRow.weight);
                                                                                    const usdOz = Number(tmRow.usd_per_troy_ounce_confirmed ?? tmRow.usd_per_troy_ounce);
                                                                                    const usdZar = Number(tmRow.zar_to_usd_confirmed ?? tmRow.zar_to_usd);
                                                                                    if (Number.isFinite(weight)) {
                                                                                        acc.totalWeight += weight;
                                                                                    }
                                                                                    if (Number.isFinite(weight) && Math.abs(weight) > 0 && Number.isFinite(usdOz)) {
                                                                                        acc.usdOzWeightedSum += weight * usdOz;
                                                                                        acc.usdOzWeight += weight;
                                                                                    }
                                                                                    if (Number.isFinite(weight) && Math.abs(weight) > 0 && Number.isFinite(usdZar)) {
                                                                                        acc.usdZarWeightedSum += weight * usdZar;
                                                                                        acc.usdZarWeight += weight;
                                                                                    }
                                                                                    return acc;
                                                                                }, {
                                                                                    totalWeight: 0,
                                                                                    usdOzWeightedSum: 0,
                                                                                    usdOzWeight: 0,
                                                                                    usdZarWeightedSum: 0,
                                                                                    usdZarWeight: 0,
                                                                                });

                                                                                const usdOzWa = Math.abs(tmSummary.usdOzWeight) > 0
                                                                                    ? tmSummary.usdOzWeightedSum / tmSummary.usdOzWeight
                                                                                    : null;
                                                                                const usdZarWa = Math.abs(tmSummary.usdZarWeight) > 0
                                                                                    ? tmSummary.usdZarWeightedSum / tmSummary.usdZarWeight
                                                                                    : null;

                                                                                return (
                                                                                    <>
                                                                                        {details.trademc.map((tmRow, tmIdx) => {
                                                                                            const supplier = asText(tmRow.company_name, asText(tmRow.company_id, '--'));
                                                                                            const weight = tmRow.weight;
                                                                                            const usdOz = tmRow.usd_per_troy_ounce_confirmed ?? tmRow.usd_per_troy_ounce;
                                                                                            const usdZar = tmRow.zar_to_usd_confirmed ?? tmRow.zar_to_usd;
                                                                                            return (
                                                                                                <tr key={`${tradeNum}-tm-${tmIdx}`}>
                                                                                                    <td>{supplier}</td>
                                                                                                    <td className={numClass(weight)}>{fmt(weight)}</td>
                                                                                                    <td className={numClass(usdOz)}>{fmt(usdOz, 4)}</td>
                                                                                                    <td className={numClass(usdZar)}>{fmt(usdZar, 5)}</td>
                                                                                                </tr>
                                                                                            );
                                                                                        })}
                                                                                        <tr className="trademc-summary-row">
                                                                                            <td>Total / WA</td>
                                                                                            <td className={numClass(tmSummary.totalWeight)}>{fmt(tmSummary.totalWeight)}</td>
                                                                                            <td className={numClass(usdOzWa)}>{fmt(usdOzWa, 4)}</td>
                                                                                            <td className={numClass(usdZarWa)}>{fmt(usdZarWa, 5)}</td>
                                                                                        </tr>
                                                                                    </>
                                                                                );
                                                                            })()}
                                                                        </tbody>
                                                                    </table>
                                                                </div>
                                                            </>
                                                        ) : (
                                                            <div style={{ color: 'var(--text-muted)', paddingBottom: '0.75rem' }}>
                                                                No TradeMC rows found for {tradeNum}
                                                            </div>
                                                        )
                                                    )}

                                                </div>
                                            </td>
                                        </tr>
                                    )}
                                </Fragment>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: SUPPLIER BALANCES
// ===================================================================
function SupplierBalances() {
    type SupplierSummaryRow = {
        supplier: string;
        balance_g: number | null;
        tx_count: number;
        last_tx: string;
        last_trade_tx: string;
        latest_trademc_trade_ts: string;
    };

    type SupplierTxRow = {
        ledger_id: string;
        transaction_time: string;
        type: string;
        pc_code: string;
        weight: number | null;
        rolling_balance: number | null;
        trade_id: string;
        notes: string;
    };

    const [data, setData] = useState<Row[]>([]);
    const [tradeMCTrades, setTradeMCTrades] = useState<Row[]>([]);
    const [companies, setCompanies] = useState<Row[]>([]);
    const [weightTypes, setWeightTypes] = useState<string[]>([]);
    const [loading, setLoading] = useState(true);
    const [syncing, setSyncing] = useState(false);
    const [filters, setFilters] = usePersistentState('filters:supplier_balances', { company_id: '', type: '' });
    const [expandedSuppliers, setExpandedSuppliers] = useState<Record<string, boolean>>({});
    const { show, Toast } = useToast();
    const [pageError, setPageError] = useState('');

    const load = useCallback(async () => {
        setLoading(true);
        setPageError('');
        try {
            const params: Record<string, string> = {};
            if (filters.company_id) params.company_id = filters.company_id;
            if (filters.type) params.type = filters.type;
            const [rows, trades, cos, types] = await Promise.all([
                api.getWeightTransactions(Object.keys(params).length ? params : undefined),
                api.getTradeMCTrades(),
                api.getCompanies(),
                api.getWeightTypes(),
            ]);
            setData(rows);
            setTradeMCTrades(trades);
            setCompanies(cos);
            setWeightTypes(types);
            setPageError('');
        } catch (e: unknown) { setPageError(String(e)); }
        setLoading(false);
    }, [filters]);

    useEffect(() => { load(); }, [load]);
    useEffect(() => { setExpandedSuppliers({}); }, [data]);

    const toggleSupplier = (supplier: string) => {
        setExpandedSuppliers(prev => ({ ...prev, [supplier]: !prev[supplier] }));
    };

    const { summaryRows, transactionsBySupplier } = useMemo(() => {
        const grouped = new Map<string, Row[]>();
        for (const row of data) {
            const supplier = asText(row.company_name, 'Unknown');
            const rows = grouped.get(supplier);
            if (rows) rows.push(row);
            else grouped.set(supplier, [row]);
        }
        const latestTradeByCompanyId = new Map<string, string>();
        const latestTradeBySupplier = new Map<string, string>();
        for (const row of tradeMCTrades) {
            const companyId = asText(row.company_id, '');
            const supplier = asText(row.company_name, '');
            const ts = asText(row.trade_timestamp, '')
                || asText(row.date_updated, '')
                || asText(row.date_created, '');
            const tsMs = toTimestampMs(ts);
            if (!Number.isFinite(tsMs)) continue;
            if (companyId) {
                const prev = asText(latestTradeByCompanyId.get(companyId), '');
                const prevMs = toTimestampMs(prev);
                if (!Number.isFinite(prevMs) || tsMs > prevMs) latestTradeByCompanyId.set(companyId, ts);
            }
            if (supplier) {
                const prev = asText(latestTradeBySupplier.get(supplier), '');
                const prevMs = toTimestampMs(prev);
                if (!Number.isFinite(prevMs) || tsMs > prevMs) latestTradeBySupplier.set(supplier, ts);
            }
        }

        const summaries: SupplierSummaryRow[] = [];
        const txMap: Record<string, SupplierTxRow[]> = {};

        for (const [supplier, rows] of grouped.entries()) {
            const ordered = [...rows].sort((a, b) => {
                const ta = toTimestampMs(a.transaction_timestamp);
                const tb = toTimestampMs(b.transaction_timestamp);
                if (Number.isFinite(ta) && Number.isFinite(tb) && ta !== tb) return ta - tb;
                if (Number.isFinite(ta) && !Number.isFinite(tb)) return 1;
                if (!Number.isFinite(ta) && Number.isFinite(tb)) return -1;
                return toNumericId(a.id) - toNumericId(b.id);
            });

            let lastRollingBalance: number | null = null;
            let netWeight = 0;
            let hasNetWeight = false;

            for (const row of ordered) {
                const rolling = toNullableNumber(row.rolling_balance);
                if (rolling !== null) lastRollingBalance = rolling;

                const weight = toNullableNumber(row.weight);
                if (weight !== null) {
                    const typeKey = asText(row.type).toUpperCase();
                    const sign = WEIGHT_TYPE_SIGN[typeKey] ?? 0;
                    netWeight += weight * sign;
                    hasNetWeight = true;
                }
            }

            let balanceG: number | null = null;
            if (lastRollingBalance !== null && Math.abs(lastRollingBalance) > 1e-6) balanceG = lastRollingBalance;
            else if (hasNetWeight) balanceG = netWeight;
            else if (lastRollingBalance !== null) balanceG = lastRollingBalance;

            const lastTx = ordered.length > 0 ? asText(ordered[ordered.length - 1].transaction_timestamp) : '';
            const tradeOnly = ordered.filter((row) => {
                const typeKey = asText(row.type).toUpperCase();
                const tradeId = asText(row.trade_id);
                return typeKey === 'TRADE' || tradeId !== '';
            });
            const lastTradeTx = tradeOnly.length > 0
                ? asText(tradeOnly[tradeOnly.length - 1].transaction_timestamp)
                : '';
            const companyIdFromRows = asText(
                rows.find((row) => asText(row.company_id, '') !== '')?.company_id,
                ''
            );
            const latestTradeMCTs = (
                (companyIdFromRows ? asText(latestTradeByCompanyId.get(companyIdFromRows), '') : '')
                || asText(latestTradeBySupplier.get(supplier), '')
            );
            summaries.push({
                supplier,
                balance_g: balanceG,
                tx_count: ordered.length,
                last_tx: lastTx,
                last_trade_tx: lastTradeTx,
                latest_trademc_trade_ts: latestTradeMCTs,
            });

            txMap[supplier] = [...ordered].reverse().map((row) => ({
                ledger_id: asText(row.id),
                transaction_time: fmtDateTime(row.transaction_timestamp),
                type: asText(row.type),
                pc_code: asText(row.pc_code),
                weight: toNullableNumber(row.weight),
                rolling_balance: toNullableNumber(row.rolling_balance),
                trade_id: asText(row.trade_id),
                notes: asText(row.notes),
            }));
        }

        summaries.sort((a, b) => {
            const tma = toTimestampMs(a.latest_trademc_trade_ts);
            const tmb = toTimestampMs(b.latest_trademc_trade_ts);
            if (Number.isFinite(tma) && Number.isFinite(tmb) && tma !== tmb) return tmb - tma;
            if (Number.isFinite(tma) && !Number.isFinite(tmb)) return -1;
            if (!Number.isFinite(tma) && Number.isFinite(tmb)) return 1;
            const ta = toTimestampMs(a.last_trade_tx);
            const tb = toTimestampMs(b.last_trade_tx);
            if (Number.isFinite(ta) && Number.isFinite(tb) && ta !== tb) return tb - ta;
            if (Number.isFinite(ta) && !Number.isFinite(tb)) return -1;
            if (!Number.isFinite(ta) && Number.isFinite(tb)) return 1;
            const la = toTimestampMs(a.last_tx);
            const lb = toTimestampMs(b.last_tx);
            if (Number.isFinite(la) && Number.isFinite(lb) && la !== lb) return lb - la;
            if (Number.isFinite(la) && !Number.isFinite(lb)) return -1;
            if (!Number.isFinite(la) && Number.isFinite(lb)) return 1;
            return a.supplier.localeCompare(b.supplier);
        });
        return { summaryRows: summaries, transactionsBySupplier: txMap };
    }, [data, tradeMCTrades]);

    const sync = async () => {
        setSyncing(true);
        try {
            const res = await api.syncWeight();
            show(`Synced ${(res as { count?: number }).count ?? 0} weight transactions`);
            setPageError('');
            load();
        } catch (e: unknown) { setPageError(String(e)); }
        setSyncing(false);
    };

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Supplier Balances</h2>
                    <div className="page-subtitle">Weight transaction ledger</div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={load}>Refresh</button>
                    <button className="btn btn-sm btn-primary" onClick={sync} disabled={syncing}>
                        {syncing ? 'Syncing...' : 'Sync from API'}
                    </button>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Company</label>
                    <select value={filters.company_id} onChange={e => setFilters(f => ({ ...f, company_id: e.target.value }))}>
                        <option value="">All Companies</option>
                        {companies.map(c => <option key={String(c.id)} value={String(c.id)}>{String(c.company_name || `Company ${c.id}`)}</option>)}
                    </select>
                </div>
                <div className="filter-group">
                    <label>Type</label>
                    <select value={filters.type} onChange={e => setFilters(f => ({ ...f, type: e.target.value }))}>
                        <option value="">All Types</option>
                        {weightTypes.map(t => <option key={t} value={t}>{t}</option>)}
                    </select>
                </div>
            </div>
            {pageError && <div className="stat-sub" style={{ color: 'var(--danger)', marginTop: '0.5rem' }}>{pageError}</div>}

            {loading ? <Loading /> : (
                summaryRows.length === 0 ? (
                    <Empty title="No weight transactions found" sub="Adjust filters or sync from API." />
                ) : (
                    <div className="table-container">
                        <table className="data-table supplier-summary-table">
                            <thead>
                                <tr>
                                    <th>Supplier</th>
                                    <th>Balance (g)</th>
                                    <th>Transactions</th>
                                    <th>Last Transaction</th>
                                    <th>Details</th>
                                </tr>
                            </thead>
                            <tbody>
                                {summaryRows.map((row) => {
                                    const expanded = !!expandedSuppliers[row.supplier];
                                    const transactions = transactionsBySupplier[row.supplier] || [];
                                    return (
                                        <Fragment key={row.supplier}>
                                            <tr>
                                                <td className="supplier-name-cell">{row.supplier}</td>
                                                <td className={numClass(row.balance_g)}>{fmt(row.balance_g)}</td>
                                                <td className="num">{fmt(row.tx_count, 0)}</td>
                                                <td>{fmtDateTime(row.last_tx)}</td>
                                                <td className="supplier-expand-cell">
                                                    <button
                                                        type="button"
                                                        className="btn btn-sm"
                                                        onClick={() => toggleSupplier(row.supplier)}
                                                    >
                                                        {expanded ? 'Collapse' : 'Expand'}
                                                    </button>
                                                </td>
                                            </tr>
                                            {expanded && (
                                                <tr className="supplier-transactions-row">
                                                    <td colSpan={5}>
                                                        <div className="supplier-transactions-wrap">
                                                            <div className="supplier-transactions-title">
                                                                {row.supplier} transactions
                                                            </div>
                                                            <DataTable
                                                                columns={[
                                                                    { key: 'ledger_id', label: 'Ledger ID' },
                                                                    { key: 'transaction_time', label: 'Transaction Time' },
                                                                    { key: 'type', label: 'Type' },
                                                                    { key: 'pc_code', label: 'PC Code' },
                                                                    { key: 'weight', label: 'Weight (g)' },
                                                                    { key: 'rolling_balance', label: 'Rolling Balance (g)' },
                                                                    { key: 'trade_id', label: 'Trade ID' },
                                                                    { key: 'notes', label: 'Notes' },
                                                                ]}
                                                                data={transactions}
                                                                numericCols={['weight', 'rolling_balance']}
                                                                formatters={{
                                                                    weight: { decimals: 2 },
                                                                    rolling_balance: { decimals: 2 },
                                                                }}
                                                            />
                                                        </div>
                                                    </td>
                                                </tr>
                                            )}
                                        </Fragment>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )
            )}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: WEIGHTED AVERAGE CALCULATOR
// ===================================================================
function WeightedAverage() {
    const [tradeNum, setTradeNum] = usePersistentState('filters:weighted_average:trade_num', '');
    const [result, setResult] = useState<Record<string, unknown> | null>(null);
    const [loading, setLoading] = useState(false);
    const { show, Toast } = useToast();
    type SheetRow = { id: string; side: string; weight: string; rate: string; total: string; locked: boolean };
    const TOLERANCE = 0.1;
    const makeBlankRow = (): SheetRow => ({
        id: `${Date.now()}-${Math.random()}`,
        side: '',
        weight: '',
        rate: '',
        total: '',
        locked: false,
    });
    const [goldSheetRows, setGoldSheetRows] = useState<SheetRow[]>([makeBlankRow()]);
    const [fxSheetRows, setFxSheetRows] = useState<SheetRow[]>([makeBlankRow()]);

    const toNum = (v: unknown): number | null => {
        const n = Number(String(v ?? '').replace(/,/g, '').trim());
        return Number.isFinite(n) ? n : null;
    };
    const sheetTotals = (rows: SheetRow[]) => {
        let totalWeight = 0;
        let enteredTotal = 0;
        let calculatedTotal = 0;
        let loadedReferenceTotal = 0;
        let loadedRowsCount = 0;
        let enteredRowsCount = 0;
        for (const r of rows) {
            const w = toNum(r.weight);
            const rate = toNum(r.rate);
            const total = toNum(r.total);
            if (w !== null) totalWeight += w;
            if (w !== null && rate !== null) calculatedTotal += w * rate;
            if (total !== null) {
                enteredTotal += total;
                enteredRowsCount += 1;
                if (r.locked) {
                    loadedReferenceTotal += total;
                    loadedRowsCount += 1;
                }
            }
        }
        const wa = Math.abs(totalWeight) > 1e-12 ? (calculatedTotal / totalWeight) : null;
        const delta = enteredRowsCount > 0 ? (enteredTotal - calculatedTotal) : null;
        const out = delta !== null ? Math.abs(delta) > TOLERANCE : false;
        return {
            totalWeight,
            enteredTotal,
            calculatedTotal,
            wa,
            delta,
            out,
            loadedReferenceTotal,
            loadedRowsCount,
        };
    };
    const goldTotals = useMemo(() => sheetTotals(goldSheetRows), [goldSheetRows]);
    const fxTotals = useMemo(() => sheetTotals(fxSheetRows), [fxSheetRows]);
    const overallDelta = useMemo(() => {
        const deltas = [goldTotals.delta, fxTotals.delta].filter((v): v is number => v !== null);
        if (deltas.length === 0) return null;
        return deltas.reduce((acc, v) => acc + v, 0);
    }, [goldTotals.delta, fxTotals.delta]);
    const overallOut = overallDelta !== null ? Math.abs(overallDelta) > TOLERANCE : false;

    const search = async () => {
        if (!tradeNum.trim()) return;
        setLoading(true);
        try {
            const res = await api.getWeightedAverage(tradeNum.trim());
            setResult(res);
            const xauRows = ((res?.xau_usd as Row[] | undefined) || []);
            const zarRows = ((res?.usd_zar as Row[] | undefined) || []);
            const pickFirst = (row: Row, keys: string[]): unknown => keys.map((k) => row[k]).find((v) => v !== undefined && v !== null && String(v).trim() !== '');
            const withThousands = (raw: string): string => {
                if (!raw) return raw;
                const [intPart, decPart] = raw.split('.');
                const intNum = Number(intPart.replace(/,/g, ''));
                if (!Number.isFinite(intNum)) return raw;
                const fmtInt = intNum.toLocaleString('en-US');
                return decPart !== undefined ? `${fmtInt}.${decPart}` : fmtInt;
            };
            const mapToSheet = (rows: Row[], weightKeys: string[], rateKeys: string[], totalKeys: string[]): SheetRow[] => {
                const mapped = rows.map((row) => {
                    const weightVal = pickFirst(row, weightKeys);
                    const rateVal = pickFirst(row, rateKeys);
                    const totalVal = pickFirst(row, totalKeys);
                    const weightNum = weightVal !== undefined && weightVal !== null ? Number(weightVal) : NaN;
                    const sideFromSign = Number.isFinite(weightNum) && weightNum !== 0
                        ? (weightNum < 0 ? 'SELL' : 'BUY')
                        : '';
                    const totalNum = totalVal !== undefined && totalVal !== null ? Number(totalVal) : NaN;
                    const totalStr = Number.isFinite(totalNum)
                        ? Math.abs(totalNum).toFixed(2)
                        : (totalVal !== undefined && totalVal !== null ? String(totalVal) : '');
                    const weightStr = Number.isFinite(weightNum)
                        ? String(Math.abs(weightNum))
                        : (weightVal !== undefined && weightVal !== null ? String(weightVal) : '');
                    const rateStr = rateVal !== undefined && rateVal !== null ? String(rateVal) : '';
                    return {
                        id: `${Date.now()}-${Math.random()}`,
                        side: sideFromSign,
                        weight: withThousands(weightStr),
                        rate: withThousands(rateStr),
                        total: withThousands(totalStr),
                        locked: true,
                    };
                });
                return mapped.length > 0 ? [...mapped, makeBlankRow()] : [makeBlankRow()];
            };
            setGoldSheetRows(
                mapToSheet(
                    xauRows,
                    ['quantity', 'qty', 'weight', 'weight_oz'],
                    ['price', 'rate', 'usd_per_oz'],
                    ['total', 'value', 'notional', 'total_val', 'usd_value']
                )
            );
            setFxSheetRows(
                mapToSheet(
                    zarRows,
                    ['quantity', 'qty', 'usd_amount', 'amount'],
                    ['price', 'rate', 'usd_zar']
                    ,
                    ['total', 'value', 'notional', 'total_val', 'zar_value']
                )
            );
        } catch (e: unknown) {
            show(String(e), 'error');
            setResult(null);
        }
        setLoading(false);
    };

    const updateRow = (
        setRows: React.Dispatch<React.SetStateAction<SheetRow[]>>,
        rowId: string,
        field: 'side' | 'weight' | 'rate' | 'total',
        value: string
    ) => {
        setRows((prev) => prev.map((r) => {
            if (r.id !== rowId) return r;
            if (r.locked) return r;
            const next = { ...r, [field]: value };
            if (field === 'weight') {
                const n = Number(String(value).replace(/,/g, '').trim());
                if (Number.isFinite(n) && n !== 0) {
                    next.side = n < 0 ? 'SELL' : 'BUY';
                } else if (String(value).trim() === '') {
                    next.side = '';
                }
            }
            return next;
        }));
    };
    const addRow = (setRows: React.Dispatch<React.SetStateAction<SheetRow[]>>) =>
        setRows((prev) => [...prev, makeBlankRow()]);
    const removeRow = (setRows: React.Dispatch<React.SetStateAction<SheetRow[]>>, rowId: string) =>
        setRows((prev) => prev.filter(r => !(r.id === rowId && !r.locked)));
    const manualRowCount = (rows: SheetRow[]) => rows.filter((r) => !r.locked).length;
    const renderSheet = (
        title: string,
        rows: SheetRow[],
        setRows: React.Dispatch<React.SetStateAction<SheetRow[]>>,
        weightLabel: string,
        rateLabel: string,
        totalPrefix: string,
        totals: {
            totalTraded: number;
            weightedRate: number | null;
            product: number;
        }
    ) => (
        <div className="worksheet-card section">
            <div className="section-title">{title}</div>
            <div className="worksheet-table-wrap">
                <div className="worksheet-summary-bar">
                    <div className="worksheet-summary-cell">{`Total Traded: ${fmt(totals.totalTraded, 4)}`}</div>
                    <div className="worksheet-summary-cell">{fmtFullPrecision(totals.weightedRate)}</div>
                    <div className="worksheet-summary-cell">{`Product: ${totalPrefix}${fmt(totals.product, 2)}`}</div>
                </div>
                <table className="data-table worksheet-table">
                    <thead>
                        <tr>
                            <th style={{ width: '14%' }}>Side</th>
                            <th style={{ width: '24%' }}>{weightLabel}</th>
                            <th style={{ width: '24%' }}>{rateLabel}</th>
                            <th style={{ width: '28%' }}>Total</th>
                            <th style={{ width: '10%' }}></th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((r) => {
                            const w = toNum(r.weight);
                            const rate = toNum(r.rate);
                            const calc = w !== null && rate !== null ? w * rate : null;
                            const entered = toNum(r.total);
                            const rowDelta = entered !== null && calc !== null ? entered - calc : null;
                            const rowOut = rowDelta !== null ? Math.abs(rowDelta) > TOLERANCE : false;
                            return (
                                <tr key={r.id} className={r.locked ? 'worksheet-row-locked' : ''}>
                                    <td>
                                        {r.locked ? (
                                            <span className={`worksheet-side-chip ${String(r.side).toLowerCase()}`}>{r.side || '--'}</span>
                                        ) : (
                                            <input
                                                className="worksheet-cell-input"
                                                value={r.side}
                                                placeholder="BUY/SELL"
                                                onChange={(e) => updateRow(setRows, r.id, 'side', e.target.value.toUpperCase())}
                                            />
                                        )}
                                    </td>
                                    <td>
                                        <input
                                            className="worksheet-cell-input"
                                            value={r.weight}
                                            placeholder="0"
                                            disabled={r.locked}
                                            onChange={(e) => updateRow(setRows, r.id, 'weight', e.target.value)}
                                        />
                                    </td>
                                    <td>
                                        <input
                                            className="worksheet-cell-input"
                                            value={r.rate}
                                            placeholder="0"
                                            disabled={r.locked}
                                            onChange={(e) => updateRow(setRows, r.id, 'rate', e.target.value)}
                                        />
                                    </td>
                                    <td className={`num ${rowOut ? 'trade-book-diff-bad' : ''}`}>
                                        <input
                                            className="worksheet-cell-input"
                                            value={r.total}
                                            placeholder={calc !== null ? fmt(calc, 2) : '0'}
                                            disabled={r.locked}
                                            onChange={(e) => updateRow(setRows, r.id, 'total', e.target.value)}
                                        />
                                    </td>
                                    <td>
                                        {r.locked ? (
                                            <span className="worksheet-side-empty">-</span>
                                        ) : (
                                            <button className="btn btn-sm" onClick={() => removeRow(setRows, r.id)}>x</button>
                                        )}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            <div style={{ marginTop: '0.5rem' }}>
                <button className="btn btn-sm" onClick={() => addRow(setRows)}>Add Row</button>
            </div>
        </div>
    );

    return (
        <div>
            <div className="page-header"><h2>Trading Worksheet</h2></div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Trade #</label>
                    <input placeholder="e.g. P1019" value={tradeNum} onChange={e => setTradeNum(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && search()} />
                </div>
                <button className="btn btn-primary btn-sm" onClick={search} style={{ alignSelf: 'flex-end' }}>Load Trade</button>
            </div>

            {loading && <Loading />}

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Gold WA ($/oz)</div>
                    <div className="stat-value">{goldTotals.wa !== null ? `$${fmt(goldTotals.wa, 4)}` : '--'}</div>
                    <div className="stat-sub">Weight: {fmt(goldTotals.totalWeight, 3)} | Calc: ${fmt(goldTotals.calculatedTotal, 2)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">USD/ZAR WA</div>
                    <div className="stat-value">{fxTotals.wa !== null ? `R${fmt(fxTotals.wa, 5)}` : '--'}</div>
                    <div className="stat-sub">Weight: {fmt(fxTotals.totalWeight, 2)} | Calc: R{fmt(fxTotals.calculatedTotal, 2)}</div>
                </div>
                <div className={`worksheet-balance-kpi ${overallOut ? 'unbalanced' : 'balanced'}`}>
                    <div className="worksheet-balance-label">Out Check (Tolerance {TOLERANCE})</div>
                    <div className="worksheet-balance-value">{overallOut ? 'Out' : 'In Balance'}</div>
                    <div className="worksheet-balance-meta">{overallDelta !== null ? `Combined Delta: ${fmt(overallDelta, 2)}` : 'Enter totals in manual rows to compare.'}</div>
                </div>
            </div>

            <div className="worksheet-grid-two">
                {renderSheet(
                    'Gold Worksheet',
                    goldSheetRows,
                    setGoldSheetRows,
                    'Weight (oz)',
                    'PM Price ($/oz)',
                    '$',
                    {
                        totalTraded: goldTotals.totalWeight,
                        weightedRate: goldTotals.wa,
                        product: goldTotals.calculatedTotal,
                    }
                )}
                {renderSheet(
                    'FX Worksheet',
                    fxSheetRows,
                    setFxSheetRows,
                    'Weight (USD)',
                    'FX Rate (USD/ZAR)',
                    'R',
                    {
                        totalTraded: fxTotals.totalWeight,
                        weightedRate: fxTotals.wa,
                        product: fxTotals.calculatedTotal,
                    }
                )}
            </div>

            {!loading && !result && <Empty title="Load a trade number to lock source rows, then add manual rows below for adjustments." />}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: TRADING TICKET
// ===================================================================
function TradingTicket() {
    const [tradeNum, setTradeNum] = usePersistentState('filters:trading_ticket:trade_num', '');
    const [ticket, setTicket] = useState<Record<string, unknown> | null>(null);
    const [loading, setLoading] = useState(false);
    const [downloading, setDownloading] = useState(false);
    const [bookCheckRunning, setBookCheckRunning] = useState(false);
    const [bookCheckResult, setBookCheckResult] = useState<Record<string, unknown> | null>(null);
    const [bookCheckFileName, setBookCheckFileName] = useState('');
    const { show, Toast } = useToast();

    const search = async () => {
        if (!tradeNum.trim()) return;
        setLoading(true);
        try {
            const res = await api.getTicket(tradeNum.trim());
            setTicket(res);
            setBookCheckResult(null);
            setBookCheckFileName('');
        } catch (e: unknown) {
            show(String(e), 'error');
            setTicket(null);
            setBookCheckResult(null);
            setBookCheckFileName('');
        }
        setLoading(false);
    };

    const tmData = (ticket?.trademc as Row[]) || [];
    const stData = (ticket?.stonex as Row[]) || [];
    const summaryData = (ticket?.summary as Row[]) || [];
    const summaryRow = summaryData[0] || null;
    const ticketTradeNum = ticket?.trade_num ? String(ticket.trade_num) : '';
    const isControlAccountRow = (row: Row): boolean => {
        const rowType = String(row['Row Type'] ?? '').trim().toLowerCase();
        if (rowType === 'control_account') return true;
        const company = String(row['Company'] ?? '').trim().toUpperCase();
        return company === 'TRADING CONTROL ACCOUNT';
    };
    const tmBookingSummaryRow = useMemo<Row | null>(() => {
        if (!tmData.length) return null;

        let totalWeightG = 0;
        let totalWeightOz = 0;
        let totalUsdValue = 0;
        let totalZarValue = 0;
        let hasWeightG = false;
        let hasWeightOz = false;
        let hasUsdValue = false;
        let hasZarValue = false;

        for (const row of tmData) {
            if (isControlAccountRow(row)) continue;
            const weightG = toNullableNumber(row['Weight (g)']);
            const weightOz = toNullableNumber(row['Weight (oz)']);
            const usdValue = toNullableNumber(row['USD Value']);
            const zarValue = toNullableNumber(row['ZAR Value']);

            if (weightG !== null) {
                totalWeightG += weightG;
                hasWeightG = true;
            }
            if (weightOz !== null) {
                totalWeightOz += weightOz;
                hasWeightOz = true;
            }
            if (usdValue !== null) {
                totalUsdValue += usdValue;
                hasUsdValue = true;
            }
            if (zarValue !== null) {
                totalZarValue += zarValue;
                hasZarValue = true;
            }
        }

        if (!hasWeightOz && hasWeightG) {
            totalWeightOz = totalWeightG / 31.1035;
            hasWeightOz = true;
        }

        const usdWa = (hasUsdValue && hasWeightOz && Math.abs(totalWeightOz) > 1e-9)
            ? (totalUsdValue / totalWeightOz)
            : null;
        const fxWa = (hasUsdValue && hasZarValue && Math.abs(totalUsdValue) > 1e-9)
            ? (totalZarValue / totalUsdValue)
            : null;
        const totalBuyZar = hasZarValue ? Math.abs(totalZarValue) : null;

        return {
            'Total Weight (g)': hasWeightG ? totalWeightG : null,
            'TradeMC WA $/oz': usdWa,
            'TradeMC WA FX': fxWa,
            'Total Buy ZAR': totalBuyZar,
        };
    }, [tmData]);
    const tmValueTotals = useMemo(() => {
        let totalWeightG = 0;
        let totalUsdValue = 0;
        let totalZarValue = 0;
        let hasWeightG = false;
        let hasUsdValue = false;
        let hasZarValue = false;

        for (const row of tmData) {
            if (isControlAccountRow(row)) continue;
            const weightG = toNullableNumber(row['Weight (g)']);
            const usdValue = toNullableNumber(row['USD Value']);
            const zarValue = toNullableNumber(row['ZAR Value']);
            if (weightG !== null) {
                totalWeightG += weightG;
                hasWeightG = true;
            }
            if (usdValue !== null) {
                totalUsdValue += usdValue;
                hasUsdValue = true;
            }
            if (zarValue !== null) {
                totalZarValue += zarValue;
                hasZarValue = true;
            }
        }

        return {
            weightG: hasWeightG ? totalWeightG : null,
            usd: hasUsdValue ? totalUsdValue : null,
            zar: hasZarValue ? totalZarValue : null,
        };
    }, [tmData]);

    const normalizePairSymbol = (value: unknown): string => {
        const text = String(value ?? '').toUpperCase().replace(/\s+/g, '').replace('-', '');
        if (!text) return '';
        if (text.includes('/')) return text;
        if (text.length === 6) return `${text.slice(0, 3)}/${text.slice(3)}`;
        return text;
    };

    const summaryFallbackRow = useMemo<Row | null>(() => {
        if (!stData.length) return null;

        let goldFlowUsd = 0;
        let fxFlowZar = 0;
        let goldAbsQty = 0;
        let goldAbsPriceQty = 0;
        let fxAbsQty = 0;
        let fxAbsPriceQty = 0;
        let goldSignedOz = 0;
        let hasGoldFlow = false;
        let hasFxFlow = false;
        let hasGoldQty = false;

        for (const row of stData) {
            const symbol = normalizePairSymbol(row['Symbol']);
            const side = String(row['Side'] ?? '').trim().toUpperCase();
            const sign = side === 'SELL' ? 1 : side === 'BUY' ? -1 : 0;
            if (!sign) continue;

            const qty = toNullableNumber(row['Quantity']);
            const price = toNullableNumber(row['Price']);
            if (qty === null || price === null) continue;

            const flow = Math.abs(qty) * price * sign;
            if (symbol === 'XAU/USD') {
                goldFlowUsd += flow;
                hasGoldFlow = true;
                const absQty = Math.abs(qty);
                goldAbsQty += absQty;
                goldAbsPriceQty += absQty * price;
                const positionSign = side === 'BUY' ? 1 : side === 'SELL' ? -1 : 0;
                goldSignedOz += absQty * positionSign;
                hasGoldQty = true;
            } else if (symbol === 'USD/ZAR') {
                fxFlowZar += flow;
                hasFxFlow = true;
                const absQty = Math.abs(qty);
                fxAbsQty += absQty;
                fxAbsPriceQty += absQty * price;
            }
        }

        const out: Row = {};
        if (hasFxFlow) out['StoneX ZAR Flow'] = fxFlowZar;

        const goldRef = goldAbsQty > 1e-9 ? (goldAbsPriceQty / goldAbsQty) : null;
        const fxRef = fxAbsQty > 1e-9 ? (fxAbsPriceQty / fxAbsQty) : null;
        const tmWeightG = tmValueTotals.weightG;
        let controlAccountOz: number | null = null;
        let controlAccountZar: number | null = null;
        if (tmWeightG !== null && hasGoldQty) {
            const controlAccountG = tmWeightG + (goldSignedOz * 31.1035);
            controlAccountOz = controlAccountG / 31.1035;
            out['Control Account (g)'] = controlAccountG;
            out['Control Account (oz)'] = controlAccountOz;
            if (goldRef !== null && fxRef !== null) {
                controlAccountZar = ((goldRef * fxRef) / 31.1035) * controlAccountG;
                out['Control Account (ZAR)'] = controlAccountZar;
            }
        }

        const tmUsd = tmValueTotals.usd;
        const tmZar = tmValueTotals.zar;
        const isClientShort = tmWeightG !== null && tmWeightG < 0;
        const tmSideUsd = tmUsd !== null ? Math.abs(tmUsd) : null;
        const tmSideZar = tmZar !== null ? Math.abs(tmZar) : null;
        const controlAccountUsd = (controlAccountOz !== null && goldRef !== null)
            ? (controlAccountOz * goldRef)
            : null;
        const stonexLegUsd = (hasGoldFlow || controlAccountUsd !== null)
            ? (hasGoldFlow ? goldFlowUsd : 0) + (controlAccountUsd ?? 0)
            : null;
        const stonexLegZar = (hasFxFlow || controlAccountZar !== null)
            ? (hasFxFlow ? fxFlowZar : 0) + (controlAccountZar ?? 0)
            : null;
        const sellSideUsd = isClientShort
            ? tmSideUsd
            : (stonexLegUsd !== null ? Math.abs(stonexLegUsd) : null);
        const buySideUsd = isClientShort
            ? (stonexLegUsd !== null ? Math.abs(stonexLegUsd) : null)
            : tmSideUsd;
        const sellSideZar = isClientShort
            ? tmSideZar
            : (stonexLegZar !== null ? Math.abs(stonexLegZar) : null);
        const buySideZar = isClientShort
            ? (stonexLegZar !== null ? Math.abs(stonexLegZar) : null)
            : tmSideZar;
        if (sellSideUsd !== null) out['Sell Side (USD)'] = sellSideUsd;
        if (buySideUsd !== null) out['Buy Side (USD)'] = buySideUsd;
        if (sellSideZar !== null) out['Sell Side (ZAR)'] = sellSideZar;
        if (buySideZar !== null) out['Buy Side (ZAR)'] = buySideZar;

        let profitUsd: number | null = null;
        if (sellSideUsd !== null && buySideUsd !== null) {
            profitUsd = sellSideUsd - buySideUsd;
            out['Profit (USD)'] = profitUsd;
        }
        if (sellSideZar !== null && buySideZar !== null) {
            const profitZar = sellSideZar - buySideZar;
            out['Profit (ZAR)'] = profitZar;
            if (Math.abs(buySideZar) > 1e-9) {
                out['Profit % (ZAR Spot Cost)'] = (profitZar / Math.abs(buySideZar)) * 100;
            }
        } else if (profitUsd !== null && fxRef !== null) {
            const profitZar = profitUsd * fxRef;
            out['Profit (ZAR)'] = profitZar;
        }

        return Object.keys(out).length ? out : null;
    }, [stData, tmValueTotals]);

    const pmxTradeSummaryRow = useMemo<Row | null>(() => {
        if (!stData.length) return null;

        const isMetalSymbol = (value: unknown): boolean => {
            const normalized = normalizePairSymbol(value);
            return normalized === 'XAU/USD' || normalized === 'XAG/USD' || normalized === 'XPT/USD' || normalized === 'XPD/USD';
        };

        let netOz = 0;
        let hasMetalQty = false;

        for (const row of stData) {
            if (!isMetalSymbol(row['Symbol'])) continue;
            const qty = toNullableNumber(row['Quantity']);
            if (qty === null) continue;
            const side = String(row['Side'] ?? '').trim().toUpperCase();
            const sign = side === 'BUY' ? 1 : side === 'SELL' ? -1 : 0;
            if (!sign) continue;
            netOz += Math.abs(qty) * sign;
            hasMetalQty = true;
        }

        if (!hasMetalQty) return null;
        return {
            'Total Traded (oz)': netOz,
            'Total Traded (g)': netOz * 31.1035,
        };
    }, [stData]);
    const summaryDisplayRow = useMemo<Row | null>(() => {
        if (!summaryRow && !pmxTradeSummaryRow && !summaryFallbackRow) return null;
        const mergedSummary: Row = { ...(summaryRow || {}) };
        if (summaryFallbackRow) {
            for (const [key, value] of Object.entries(summaryFallbackRow)) {
                const existing = mergedSummary[key];
                if (existing === null || existing === undefined || existing === '') {
                    mergedSummary[key] = value;
                }
            }
        }
        const out: Row = {
            ...mergedSummary,
            ...(pmxTradeSummaryRow || {}),
        };
        // Keep profit fields strictly consistent with displayed sell/buy sides.
        const sellUsd = toNullableNumber(out['Sell Side (USD)']);
        const buyUsd = toNullableNumber(out['Buy Side (USD)']);
        const sellUsdAbs = sellUsd === null ? null : Math.abs(sellUsd);
        const buyUsdAbs = buyUsd === null ? null : Math.abs(buyUsd);
        if (sellUsdAbs !== null && buyUsdAbs !== null) {
            out['Sell Side (USD)'] = sellUsdAbs;
            out['Buy Side (USD)'] = buyUsdAbs;
            out['Profit (USD)'] = sellUsdAbs - buyUsdAbs;
        }

        const sellZar = toNullableNumber(out['Sell Side (ZAR)']);
        const buyZar = toNullableNumber(out['Buy Side (ZAR)']);
        const sellZarAbs = sellZar === null ? null : Math.abs(sellZar);
        const buyZarAbs = buyZar === null ? null : Math.abs(buyZar);
        if (sellZarAbs !== null && buyZarAbs !== null) {
            out['Sell Side (ZAR)'] = sellZarAbs;
            out['Buy Side (ZAR)'] = buyZarAbs;
        }

        const fxWa = toNullableNumber(out['FX WA USD/ZAR']);
        const profitUsd = toNullableNumber(out['Profit (USD)']);
        if (sellZarAbs !== null && buyZarAbs !== null) {
            const profitZar = sellZarAbs - buyZarAbs;
            out['Profit (ZAR)'] = profitZar;
            if (buyZarAbs > 1e-9) {
                out['Profit % (ZAR Spot Cost)'] = (profitZar / buyZarAbs) * 100;
            }
        } else if (profitUsd !== null && fxWa !== null) {
            const profitZar = profitUsd * fxWa;
            out['Profit (ZAR)'] = profitZar;
        }
        return out;
    }, [summaryRow, pmxTradeSummaryRow, summaryFallbackRow]);

    const downloadPdf = async () => {
        if (!ticketTradeNum) return;
        setDownloading(true);
        try {
            const res = await api.getTicketPdf(ticketTradeNum);
            if (!res.ok) {
                const ct = res.headers.get('content-type') || '';
                if (ct.includes('application/json')) {
                    const err = await res.json().catch(() => ({}));
                    show(String((err as { error?: string }).error || 'Failed to generate PDF'), 'error');
                } else {
                    show(`Failed to generate PDF (HTTP ${res.status})`, 'error');
                }
                return;
            }
            const blob = await res.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `trading_ticket_${ticketTradeNum}.pdf`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        } catch (e: unknown) {
            show(String(e), 'error');
        } finally {
            setDownloading(false);
        }
    };

    const summaryCols: { key: string; label: string; decimals?: number; headerClass?: string; suffix?: string }[] = [
        { key: 'Summary', label: 'Summary' },
        { key: 'Gold WA $/oz', label: 'Gold WA $/oz', decimals: 4, headerClass: 'highlight-gold' },
        { key: 'FX WA USD/ZAR', label: 'FX WA USD/ZAR', decimals: 4, headerClass: 'highlight-fx' },
        { key: 'Total Traded (g)', label: 'PMX Total (g)', decimals: 2 },
        { key: 'Control Account (g)', label: 'Control Account (g)', decimals: 2 },
        { key: 'Control Account (ZAR)', label: 'Control Account (ZAR)', decimals: 2 },
        { key: 'Spot ZAR/g', label: 'Spot ZAR/g', decimals: 4 },
        { key: 'StoneX ZAR Flow', label: 'StoneX ZAR Flow', decimals: 2 },
        { key: 'Profit (ZAR)', label: 'Profit (ZAR)', decimals: 2 },
        { key: 'Profit % (ZAR Spot Cost)', label: 'Profit % (ZAR)', decimals: 3, suffix: '%' },
    ];
    const summaryDisplayRows: Row[] = (() => {
        if (!summaryDisplayRow) return [];
        const controlKeys = ['Control Account (g)', 'Control Account (ZAR)'];
        const primaryRow: Row = { Summary: 'StoneX Summary', ...summaryDisplayRow };
        for (const key of controlKeys) primaryRow[key] = '';
        const hasControl = controlKeys.some((key) => {
            const value = summaryDisplayRow[key];
            return !(value === null || value === undefined || value === '');
        });
        if (!hasControl) return [primaryRow];

        const controlRow: Row = { Summary: 'Trading Control Account' };
        for (const col of summaryCols) {
            if (col.key === 'Summary') continue;
            controlRow[col.key] = controlKeys.includes(col.key) ? summaryDisplayRow[col.key] : '';
        }
        return [primaryRow, controlRow];
    })();

    const onBookCheckUpload = async (event: ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        event.currentTarget.value = '';
        if (!file) return;
        if (!ticketTradeNum) {
            show('Load a Trading Ticket first before running the trade book checker.', 'error');
            return;
        }
        setBookCheckRunning(true);
        setBookCheckResult(null);
        setBookCheckFileName(file.name);
        try {
            const lookupTradeNum = String(tradeNum || '').trim() || ticketTradeNum;
            const truthRows = tmData.map((row) => ({
                Supplier: row['Company'],
                'Weight (g)': row['Weight (g)'],
                'Gold Rate ($/oz)': row['$/oz Booked'],
                'Exchange Rate': row['FX Rate'],
            }));
            const result = await api.checkTradeBookScreenshot(lookupTradeNum, file, truthRows);
            setBookCheckResult(result);
            const counts = (result.counts as Record<string, unknown>) || {};
            const discrepancyCount = Number(counts.manual_not_in_trademc || 0) + Number(counts.missing_from_manual_book || 0);
            if (discrepancyCount > 0) show(`Discrepancies found: ${discrepancyCount}`, 'error');
            else show('No discrepancies found against TradeMC source-of-truth rows.', 'success');
        } catch (e: unknown) {
            show(String(e), 'error');
            setBookCheckResult(null);
        } finally {
            setBookCheckRunning(false);
        }
    };

    const bookCheckCounts = ((bookCheckResult?.counts as Record<string, unknown>) || {});
    const manualNotInTradeMC = (Array.isArray(bookCheckResult?.manual_not_in_trademc) ? (bookCheckResult?.manual_not_in_trademc as Row[]) : []);
    const missingFromManualBook = (Array.isArray(bookCheckResult?.missing_from_manual_book) ? (bookCheckResult?.missing_from_manual_book as Row[]) : []);
    const parseWarnings = (Array.isArray(bookCheckResult?.parse_warnings) ? (bookCheckResult?.parse_warnings as string[]) : []);
    const diffKeys = ['Supplier', 'Weight (g)', 'Gold Rate ($/oz)', 'Exchange Rate'] as const;
    type DiffKey = typeof diffKeys[number];
    type DiffPair = { manual?: Row; correct?: Row };
    const normalizeSupplierForDiff = (value: unknown): string => String(value ?? '')
        .toUpperCase()
        .replace(/\(PTY\)/g, 'PTY')
        .replace(/\bLTD\b/g, 'LIMITED')
        .replace(/[^A-Z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    const normalizeNumericForDiff = (value: unknown): string => {
        const n = toNullableNumber(value);
        if (n === null) return '';
        return Number(n).toFixed(4);
    };
    const equalDiffValue = (key: DiffKey, a: unknown, b: unknown): boolean => {
        if (key === 'Supplier') return normalizeSupplierForDiff(a) === normalizeSupplierForDiff(b);
        return normalizeNumericForDiff(a) === normalizeNumericForDiff(b);
    };
    const diffScore = (manual: Row, correct: Row): number => {
        const supplierPenalty = normalizeSupplierForDiff(manual['Supplier']) === normalizeSupplierForDiff(correct['Supplier']) ? 0 : 1000;
        let mismatchPenalty = 0;
        for (const key of diffKeys) {
            if (!equalDiffValue(key, manual[key], correct[key])) mismatchPenalty += 10;
        }
        const numDistance = (Math.abs((toNullableNumber(manual['Weight (g)']) ?? 0) - (toNullableNumber(correct['Weight (g)']) ?? 0))
            + Math.abs((toNullableNumber(manual['Gold Rate ($/oz)']) ?? 0) - (toNullableNumber(correct['Gold Rate ($/oz)']) ?? 0))
            + Math.abs((toNullableNumber(manual['Exchange Rate']) ?? 0) - (toNullableNumber(correct['Exchange Rate']) ?? 0)));
        return supplierPenalty + mismatchPenalty + numDistance;
    };
    const discrepancyPairs = useMemo<DiffPair[]>(() => {
        const pairs: DiffPair[] = [];
        const remaining = missingFromManualBook.map((row) => ({ row, used: false }));
        for (const manual of manualNotInTradeMC) {
            let bestIdx = -1;
            let bestScore = Number.POSITIVE_INFINITY;
            for (let i = 0; i < remaining.length; i += 1) {
                if (remaining[i].used) continue;
                const score = diffScore(manual, remaining[i].row);
                if (score < bestScore) {
                    bestScore = score;
                    bestIdx = i;
                }
            }
            if (bestIdx >= 0) {
                remaining[bestIdx].used = true;
                pairs.push({ manual, correct: remaining[bestIdx].row });
            } else {
                pairs.push({ manual });
            }
        }
        for (const item of remaining) {
            if (!item.used) pairs.push({ correct: item.row });
        }
        return pairs;
    }, [manualNotInTradeMC, missingFromManualBook]);
    const formatDiffValue = (key: DiffKey, value: unknown): string => {
        if (key === 'Supplier') return String(value ?? '');
        const n = toNullableNumber(value);
        return n === null ? '--' : fmt(n, 4);
    };

    return (
        <div>
            <div className="page-header"><h2>Trading Ticket</h2></div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Trade #</label>
                    <input placeholder="e.g. 9847" value={tradeNum} onChange={e => setTradeNum(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && search()} />
                </div>
                <button className="btn btn-primary btn-sm" onClick={search} style={{ alignSelf: 'flex-end' }}>Load Ticket</button>
            </div>

            {loading && <Loading />}

            {ticket && (
                <>
                    <div className="section">
                        <div className="section-title">TradeMC Booking</div>
                        {tmData.length === 0 ? (
                            <Empty title={`No TradeMC rows found for trade ${ticketTradeNum}`} />
                        ) : (
                            <DataTable
                                columns={[
                                    { key: 'Company', label: 'Company' },
                                    { key: 'Weight (g)', label: 'Weight (g)' },
                                    { key: 'Weight (oz)', label: 'Weight (oz)' },
                                    { key: '$/oz Booked', label: '$/oz Booked' },
                                    { key: 'FX Rate', label: 'FX Rate' },
                                    { key: 'USD Value', label: 'USD Value' },
                                    { key: 'ZAR Value', label: 'ZAR Value' },
                                ]}
                                data={tmData}
                                numericCols={['Weight (g)', 'Weight (oz)', '$/oz Booked', 'FX Rate', 'USD Value', 'ZAR Value']}
                                formatters={{
                                    'Weight (g)': { decimals: 2 },
                                    'Weight (oz)': { decimals: 2 },
                                    '$/oz Booked': { decimals: 4 },
                                    'FX Rate': { decimals: 4 },
                                    'USD Value': { decimals: 2 },
                                    'ZAR Value': { decimals: 2 },
                                }}
                            />
                        )}
                    </div>

                    {tmBookingSummaryRow && (
                        <div className="section mt-3">
                            <div className="section-title">TradeMC Booking Summary</div>
                            <DataTable
                                columns={[
                                    { key: 'Total Weight (g)', label: 'Total Weight (g)' },
                                    { key: 'TradeMC WA $/oz', label: 'WA $/oz' },
                                    { key: 'TradeMC WA FX', label: 'WA FX Rate' },
                                    { key: 'Total Buy ZAR', label: 'Total Buy ZAR' },
                                ]}
                                data={[tmBookingSummaryRow]}
                                numericCols={['Total Weight (g)', 'TradeMC WA $/oz', 'TradeMC WA FX', 'Total Buy ZAR']}
                                formatters={{
                                    'Total Weight (g)': { decimals: 2 },
                                    'TradeMC WA $/oz': { decimals: 4 },
                                    'TradeMC WA FX': { decimals: 4 },
                                    'Total Buy ZAR': { decimals: 2 },
                                }}
                                cellClassName={(_row, _key, _value, isNumeric) =>
                                    isNumeric ? 'num tm-booking-summary-figure' : 'tm-booking-summary-figure'
                                }
                            />
                        </div>
                    )}

                    <div className="section mt-3">
                        <div className="section-title">Trade Book Checker (TradeMC Source of Truth)</div>
                        <div className="trade-book-checker-upload-row">
                            <label className="btn btn-sm btn-primary trade-book-checker-upload-btn">
                                {bookCheckRunning ? 'Checking Screenshot...' : 'Upload Trading Book Screenshot'}
                                <input
                                    type="file"
                                    accept="image/png,image/jpeg,image/jpg"
                                    onChange={onBookCheckUpload}
                                    disabled={bookCheckRunning || !ticketTradeNum}
                                    className="trade-book-checker-file-input"
                                />
                            </label>
                            {bookCheckFileName && <span className="trade-book-checker-file-name">{bookCheckFileName}</span>}
                        </div>

                        {bookCheckResult && (
                            <>
                                <div className="trade-book-checker-summary-grid">
                                    <div className="trade-book-checker-summary-card">
                                        <div className="trade-book-checker-summary-label">Parsed Manual Rows</div>
                                        <div className="trade-book-checker-summary-value">{String(bookCheckCounts.parsed_rows ?? 0)}</div>
                                    </div>
                                    <div className="trade-book-checker-summary-card">
                                        <div className="trade-book-checker-summary-label">TradeMC Rows</div>
                                        <div className="trade-book-checker-summary-value">{String(bookCheckCounts.trademc_rows ?? 0)}</div>
                                    </div>
                                    <div className="trade-book-checker-summary-card">
                                        <div className="trade-book-checker-summary-label">Exact Matches</div>
                                        <div className="trade-book-checker-summary-value">{String(bookCheckCounts.matched_rows ?? 0)}</div>
                                    </div>
                                    <div className="trade-book-checker-summary-card trade-book-checker-summary-card-discrepancy">
                                        <div className="trade-book-checker-summary-label">Total Discrepancies</div>
                                        <div className="trade-book-checker-summary-value">
                                            {String((Number(bookCheckCounts.manual_not_in_trademc || 0) + Number(bookCheckCounts.missing_from_manual_book || 0)))}
                                        </div>
                                    </div>
                                </div>

                                {parseWarnings.length > 0 && (
                                    <div className="trade-book-checker-warning">
                                        {parseWarnings.join(' ')}
                                    </div>
                                )}

                                <div className="section mt-3">
                                    <div className="section-title">Discrepancy Diff View</div>
                                    {discrepancyPairs.length === 0 ? (
                                        <Empty title="None" />
                                    ) : (
                                        <div className="table-container">
                                            <table className="data-table trade-book-diff-table">
                                                <thead>
                                                    <tr>
                                                        <th>Row Type</th>
                                                        <th>Supplier</th>
                                                        <th>Weight (g)</th>
                                                        <th>Gold Rate ($/oz)</th>
                                                        <th>Exchange Rate</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {discrepancyPairs.map((pair, idx) => {
                                                        const pairKey = `diff-${idx}`;
                                                        const renderRow = (kind: 'manual' | 'correct', row: Row | undefined) => {
                                                            if (!row) return null;
                                                            const opposite = kind === 'manual' ? pair.correct : pair.manual;
                                                            return (
                                                                <tr key={`${pairKey}-${kind}`} className={kind === 'manual' ? 'diff-row-manual' : 'diff-row-correct'}>
                                                                    <td className="trade-book-diff-type">{kind === 'manual' ? 'Trade Book (Uploaded)' : 'TradeMC (Correct)'}</td>
                                                                    {diffKeys.map((key) => {
                                                                        const differs = opposite ? !equalDiffValue(key, row[key], opposite[key]) : true;
                                                                        const cls = differs
                                                                            ? (kind === 'manual' ? 'trade-book-diff-bad' : 'trade-book-diff-good')
                                                                            : '';
                                                                        return (
                                                                            <td key={`${pairKey}-${kind}-${key}`} className={cls}>
                                                                                {formatDiffValue(key, row[key])}
                                                                            </td>
                                                                        );
                                                                    })}
                                                                </tr>
                                                            );
                                                        };
                                                        return (
                                                            <Fragment key={pairKey}>
                                                                {renderRow('manual', pair.manual)}
                                                                {renderRow('correct', pair.correct)}
                                                            </Fragment>
                                                        );
                                                    })}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </div>
                            </>
                        )}
                    </div>

                    <div className="section mt-3">
                        <div className="section-title">PMX Trades</div>
                        {stData.length === 0 ? (
                            <Empty title="No PMX trades found in the PMX Ledger for this trade" />
                        ) : (
                            <DataTable
                                columns={[{ key: 'Trade Date', label: 'Trade Date' },
                                { key: 'Value Date', label: 'Value Date' },
                                { key: 'Symbol', label: 'Symbol' },
                                { key: 'Side', label: 'Side' },
                                { key: 'Narration', label: 'Narration' },
                                { key: 'Quantity', label: 'Quantity' },
                                { key: 'Price', label: 'Price' },
                                ]}
                                data={stData}
                                numericCols={['Quantity', 'Price']}
                                dateCols={['Trade Date', 'Value Date']}
                                formatters={{
                                    'Quantity': { decimals: 2 },
                                    'Price': { decimals: 4 },
                                }}
                                cellClassName={(row, key, value, isNumeric) => {
                                    if (key === 'Quantity') {
                                        const side = String(row['Side'] ?? '').trim().toUpperCase();
                                        if (side === 'SELL') return 'num negative';
                                        if (side === 'BUY') return 'num positive';
                                        return 'num';
                                    }
                                    if (key === 'Price') return 'num';
                                    return isNumeric ? numClass(value) : '';
                                }}
                            />
                        )}
                    </div>

                    {summaryDisplayRows.length > 0 && (
                        <div className="section mt-3">
                            <div className="section-title">Ticket Summary</div>
                            <div className="table-container">
                                <table className="data-table ticket-summary">
                                    <thead>
                                        <tr>
                                            {summaryCols.map(col => (
                                                <th key={col.key} className={col.headerClass || ''}>
                                                    {col.label}
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {summaryDisplayRows.map((row, rowIndex) => (
                                            <tr key={`summary-${rowIndex}`}>
                                                {summaryCols.map(col => {
                                                    const raw = row[col.key];
                                                    const isBlank = raw === null || raw === undefined || raw === '';
                                                    const num = isBlank ? NaN : Number(raw);
                                                    const isNum = Number.isFinite(num) && col.key !== 'Summary';
                                                    const decimals = col.decimals ?? 2;
                                                    const suffix = col.suffix ?? '';
                                                    const formatted = col.key === 'Summary'
                                                        ? String(raw ?? '')
                                                        : (isNum ? fmt(num, decimals) + suffix : String(raw ?? ''));
                                                    let extraClass = '';
                                                    if (col.key === 'Profit % (ZAR Spot Cost)') {
                                                        if (isNum && num > 0) extraClass = 'profit-positive';
                                                        else if (isNum && num < 0) extraClass = 'profit-negative';
                                                        else if (!isBlank) extraClass = 'profit-neutral';
                                                    }
                                                    const baseClass = col.key === 'Summary' ? 'ticket-summary-row-label' : 'num';
                                                    return (
                                                        <td key={`${col.key}-${rowIndex}`} className={`${baseClass} ${extraClass}`.trim()}>
                                                            {formatted}
                                                        </td>
                                                    );
                                                })}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    <div className="section mt-3">
                        <div className="section-title">Download Trading Ticket</div>
                        <button className="btn btn-sm btn-primary" onClick={downloadPdf} disabled={downloading}>
                            {downloading ? 'Generating PDF...' : 'Download Trading Ticket (PDF)'}
                        </button>
                    </div>

                    {tmData.length === 0 && stData.length === 0 && (
                        <Empty title={`No data found for trade ${ticketTradeNum}`} />
                    )}
                </>
            )}

            {!loading && !ticket && <Empty title="Enter a trade number to view the consolidated ticket" />}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: TRADE BREAKDOWN
// ===================================================================
function TradeBreakdownTab() {
    const [tradeNum, setTradeNum] = usePersistentState('filters:trade_breakdown:trade_num', '');
    const [ticket, setTicket] = useState<Record<string, unknown> | null>(null);
    const [loading, setLoading] = useState(false);
    const [downloadingPdf, setDownloadingPdf] = useState(false);
    const [downloadingCsv, setDownloadingCsv] = useState(false);
    const { show, Toast } = useToast();

    const search = async () => {
        if (!tradeNum.trim()) return;
        setLoading(true);
        try {
            const res = await api.getTicket(tradeNum.trim());
            setTicket(res);
        } catch (e: unknown) {
            show(String(e), 'error');
            setTicket(null);
        } finally {
            setLoading(false);
        }
    };

    const tmData = (ticket?.trademc as Row[]) || [];
    const stData = (ticket?.stonex as Row[]) || [];
    const summaryData = (ticket?.summary as Row[]) || [];
    const summaryRow = summaryData[0] || {};
    const ticketTradeNum = ticket?.trade_num ? String(ticket.trade_num) : '';

    const asPair = (value: unknown): string => {
        const text = String(value ?? '').toUpperCase().replace(/\s+/g, '').replace('-', '');
        if (!text) return '';
        if (text.includes('/')) return text;
        if (text.length === 6) return `${text.slice(0, 3)}/${text.slice(3)}`;
        return text;
    };
    const n = (value: unknown): number | null => toNullableNumber(value);
    const money = (value: number | null, prefix: string, decimals = 2): string =>
        value === null ? '--' : `${prefix}${fmt(value, decimals)}`;
    const grams = (value: number | null): string => value === null ? '--' : `${fmt(value, 2)} g`;
    const ounces = (value: number | null, decimals = 4): string => value === null ? '--' : `${fmt(value, decimals)} oz`;
    const pct = (value: number | null, decimals = 2): string => value === null ? '--' : `${fmt(value, decimals)}%`;
    const numText = (value: number | null, decimals = 2): string => value === null ? '--' : fmt(value, decimals);

    const tmBookings = useMemo(() => {
        return tmData.map((row) => {
            const weightG = n(row['Weight (g)']) ?? 0;
            const weightOz = n(row['Weight (oz)']) ?? (weightG / 31.1035);
            const bookedPrice = n(row['$/oz Booked']);
            const fxRate = n(row['FX Rate']);
            const usdValueRaw = n(row['USD Value']);
            const usdValue = usdValueRaw ?? ((bookedPrice !== null) ? weightOz * bookedPrice : null);
            const zarGrossRaw = n(row['ZAR Value']);
            const zarGross = zarGrossRaw ?? ((usdValue !== null && fxRate !== null) ? usdValue * fxRate : null);
            const refiningRate = n(row['company_refining_rate']) ?? 0;
            const refiningDeduction = zarGross !== null ? zarGross * (refiningRate / 100) : null;
            const zarNetRaw = n(row['zar_value_less_refining']);
            const zarNet = zarNetRaw ?? (zarGross !== null ? zarGross * (1 - (refiningRate / 100)) : null);
            return {
                company: asText(row.Company || row.company_name || 'Unknown Company'),
                weightG,
                weightOz,
                bookedPrice,
                fxRate,
                usdValue,
                zarGross,
                refiningRate,
                refiningDeduction,
                zarNet,
            };
        });
    }, [tmData]);

    const tmTotals = useMemo(() => {
        const totals = {
            weightG: 0,
            weightOz: 0,
            usdValue: 0,
            zarGross: 0,
            zarNet: 0,
            count: tmBookings.length,
        };
        for (const booking of tmBookings) {
            totals.weightG += booking.weightG;
            totals.weightOz += booking.weightOz;
            totals.usdValue += booking.usdValue ?? 0;
            totals.zarGross += booking.zarGross ?? 0;
            totals.zarNet += booking.zarNet ?? 0;
        }
        return totals;
    }, [tmBookings]);

    const pmxTrades = useMemo(() => {
        return stData.map((row) => {
            const symbol = asPair(row.Symbol);
            const side = String(row.Side ?? '').toUpperCase().trim();
            const qty = Math.abs(n(row.Quantity) ?? 0);
            const price = n(row.Price);
            const notional = (price !== null) ? qty * price : null;
            return {
                tradeDate: asText(row['Trade Date']),
                valueDate: asText(row['Value Date']),
                symbol,
                side,
                qty,
                price,
                notional,
            };
        }).filter(row => row.qty > 0);
    }, [stData]);

    const xauTrades = useMemo(
        () => pmxTrades.filter(row => row.symbol === 'XAU/USD' && row.price !== null),
        [pmxTrades]
    );
    const fxTrades = useMemo(
        () => pmxTrades.filter(row => row.symbol === 'USD/ZAR' && row.price !== null),
        [pmxTrades]
    );

    const goldWaCalc = useMemo(() => {
        const rows = xauTrades.map((trade) => ({
            ...trade,
            qty: Math.abs(trade.qty),
            notional: trade.notional ?? ((trade.price !== null) ? Math.abs(trade.qty) * trade.price : null),
        }));
        const totalQty = rows.reduce((sum, row) => sum + row.qty, 0);
        const totalNotional = rows.reduce((sum, row) => sum + (row.notional ?? 0), 0);
        const value = totalQty > 1e-9 ? (totalNotional / totalQty) : null;
        return { rows, totalQty, totalNotional, value };
    }, [xauTrades]);

    const fxWaCalc = useMemo(() => {
        const rows = fxTrades.map((trade) => ({
            ...trade,
            qty: Math.abs(trade.qty),
            notional: trade.notional ?? ((trade.price !== null) ? Math.abs(trade.qty) * trade.price : null),
        }));
        const totalQty = rows.reduce((sum, row) => sum + row.qty, 0);
        const totalNotional = rows.reduce((sum, row) => sum + (row.notional ?? 0), 0);
        const value = totalQty > 1e-9 ? (totalNotional / totalQty) : null;
        return { rows, totalQty, totalNotional, value };
    }, [fxTrades]);

    const goldWa = goldWaCalc.value;
    const fxWa = fxWaCalc.value;

    const spotZarPerG = useMemo(() => {
        if (goldWa === null || fxWa === null) return null;
        return (goldWa * fxWa) / 31.1035;
    }, [goldWa, fxWa]);

    const xauCashFlowRows = useMemo(() => {
        return xauTrades.map((trade) => {
            const signed = trade.notional === null
                ? null
                : (trade.side === 'SELL' ? trade.notional : trade.side === 'BUY' ? -trade.notional : null);
            return { ...trade, signed };
        });
    }, [xauTrades]);

    const netStoneXUsdFlow = useMemo(
        () => xauCashFlowRows.reduce((sum, row) => sum + (row.signed ?? 0), 0),
        [xauCashFlowRows]
    );

    const summaryMetrics = useMemo(() => {
        const sellSideUsd = n(summaryRow['Sell Side (USD)']) ?? Math.abs(netStoneXUsdFlow);
        const buySideUsd = n(summaryRow['Buy Side (USD)']) ?? Math.abs(tmTotals.usdValue);
        const profitUsd = n(summaryRow['Profit (USD)']) ?? ((sellSideUsd !== null && buySideUsd !== null) ? sellSideUsd - buySideUsd : null);

        const sellSideZar = n(summaryRow['Sell Side (ZAR)']) ?? n(summaryRow['StoneX ZAR Flow']);
        const buySideZar = n(summaryRow['Buy Side (ZAR)']) ?? Math.abs(tmTotals.zarNet);
        const profitZar = n(summaryRow['Profit (ZAR)']) ?? ((sellSideZar !== null && buySideZar !== null) ? sellSideZar - buySideZar : null);
        const profitMargin = n(summaryRow['Profit % (ZAR Spot Cost)'])
            ?? ((profitZar !== null && buySideZar !== null && Math.abs(buySideZar) > 1e-9) ? (profitZar / Math.abs(buySideZar)) * 100 : null);

        const controlG = n(summaryRow['Control Account (g)']);
        const controlOz = n(summaryRow['Control Account (oz)']) ?? (controlG !== null ? controlG / 31.1035 : null);
        const controlZar = n(summaryRow['Control Account (ZAR)']);

        const totalTradedG = n(summaryRow['Total Traded (g)']) ?? (Math.abs(tmTotals.weightG) > 1e-9 ? Math.abs(tmTotals.weightG) : null);
        const totalTradedOz = n(summaryRow['Total Traded (oz)']) ?? (totalTradedG !== null ? totalTradedG / 31.1035 : null);
        const stonexZarFlow = n(summaryRow['StoneX ZAR Flow']);

        return {
            sellSideUsd,
            buySideUsd,
            profitUsd,
            sellSideZar,
            buySideZar,
            profitZar,
            controlG,
            controlOz,
            controlZar,
            totalTradedG,
            totalTradedOz,
            stonexZarFlow,
            profitMargin,
        };
    }, [summaryRow, netStoneXUsdFlow, tmTotals]);

    const downloadPdf = async () => {
        if (!ticketTradeNum) return;
        setDownloadingPdf(true);
        try {
            const res = await api.getTicketPdf(ticketTradeNum);
            if (!res.ok) {
                const ct = res.headers.get('content-type') || '';
                if (ct.includes('application/json')) {
                    const err = await res.json().catch(() => ({}));
                    show(String((err as { error?: string }).error || 'Failed to generate PDF'), 'error');
                } else {
                    show(`Failed to generate PDF (HTTP ${res.status})`, 'error');
                }
                return;
            }
            const blob = await res.blob();
            triggerBlobDownload(blob, `trade_breakdown_${ticketTradeNum}.pdf`);
        } catch (e: unknown) {
            show(String(e), 'error');
        } finally {
            setDownloadingPdf(false);
        }
    };

    const downloadAuditCsv = async () => {
        if (!ticketTradeNum) return;
        setDownloadingCsv(true);
        try {
            const rows: string[][] = [];
            rows.push(['Section', 'Item', 'Formula', 'Value']);

            rows.push(['Profit Summary', 'Sell Side (StoneX) USD', '', money(summaryMetrics.sellSideUsd, '$')]);
            rows.push(['Profit Summary', 'Buy Side (TradeMC) USD', '', money(summaryMetrics.buySideUsd, '$')]);
            rows.push(['Profit Summary', 'Profit (USD)', '', money(summaryMetrics.profitUsd, '$')]);
            rows.push(['Profit Summary', 'Sell Side (StoneX) ZAR', '', money(summaryMetrics.sellSideZar, 'R ')]);
            rows.push(['Profit Summary', 'Buy Side (TradeMC) ZAR', '', money(summaryMetrics.buySideZar, 'R ')]);
            rows.push(['Profit Summary', 'Profit (ZAR)', '', money(summaryMetrics.profitZar, 'R ')]);
            rows.push(['Profit Summary', 'Control Account (g)', '', grams(summaryMetrics.controlG)]);
            rows.push(['Profit Summary', 'Control Account (oz)', '', ounces(summaryMetrics.controlOz)]);
            rows.push(['Profit Summary', 'Control Account (ZAR)', '', money(summaryMetrics.controlZar, 'R ')]);
            rows.push(['Profit Summary', 'Total Traded (g)', '', grams(summaryMetrics.totalTradedG)]);
            rows.push(['Profit Summary', 'Total Traded (oz)', '', ounces(summaryMetrics.totalTradedOz)]);
            rows.push(['Profit Summary', 'StoneX ZAR Flow', '', money(summaryMetrics.stonexZarFlow, 'R ')]);
            rows.push(['Profit Summary', 'Profit Margin', '', pct(summaryMetrics.profitMargin)]);

            rows.push(['Input Data Summary', 'TradeMC bookings (buy side)', '', String(tmBookings.length)]);
            rows.push(['Input Data Summary', 'StoneX/PMX trades (sell side)', '', String(pmxTrades.length)]);
            rows.push(['Input Data Summary', 'XAU/USD trades', '', String(xauTrades.length)]);
            rows.push(['Input Data Summary', 'USD/ZAR trades', '', String(fxTrades.length)]);

            tmBookings.forEach((booking, idx) => {
                const pfx = `TradeMC Booking ${idx + 1} (${booking.company})`;
                rows.push([pfx, 'Weight in troy ounces', `${fmt(booking.weightG, 2)} / 31.1035`, ounces(booking.weightOz, 6)]);
                rows.push([pfx, 'USD value', `${ounces(booking.weightOz, 6)} x ${money(booking.bookedPrice, '$', 2)}/oz`, money(booking.usdValue, '$')]);
                rows.push([pfx, 'ZAR gross value', `${money(booking.usdValue, '$')} x ${numText(booking.fxRate, 4)}`, money(booking.zarGross, 'R ')]);
                rows.push([pfx, 'Refining deduction', `${money(booking.zarGross, 'R ')} x ${numText(booking.refiningRate, 2)}%`, money(booking.refiningDeduction, 'R ')]);
                rows.push([pfx, 'ZAR net of refining', `${money(booking.zarGross, 'R ')} x (1 - ${numText(booking.refiningRate, 2)}%)`, money(booking.zarNet, 'R ')]);
            });

            xauTrades.forEach((trade, idx) => {
                rows.push(['StoneX Gold Trades', `Trade ${idx + 1} ${trade.side}`, `${ounces(trade.qty, 4)} x ${money(trade.price, '$', 4)}`, money(trade.notional, '$')]);
            });
            fxTrades.forEach((trade, idx) => {
                rows.push(['StoneX FX Trades', `Trade ${idx + 1} ${trade.side}`, `${money(trade.qty, '$', 2)} x ${money(trade.price, 'R ', 4)}`, money(trade.notional, 'R ')]);
            });

            rows.push(['Weighted Averages', 'Gold WA ($/oz)', `${money(goldWaCalc.totalNotional, '$')} / ${ounces(goldWaCalc.totalQty, 4)}`, money(goldWa, '$', 4)]);
            rows.push(['Weighted Averages', 'FX WA (ZAR/USD)', `${money(fxWaCalc.totalNotional, 'R ')} / ${money(fxWaCalc.totalQty, '$')}`, money(fxWa, 'R ', 4)]);
            rows.push(['Spot Derivation', 'Spot ZAR/g', `(${money(goldWa, '$', 4)} x ${money(fxWa, 'R ', 4)}) / 31.1035`, money(spotZarPerG, 'R ', 4)]);

            rows.push(['Profit/Loss', 'Profit (USD)', `${money(summaryMetrics.sellSideUsd, '$')} - ${money(summaryMetrics.buySideUsd, '$')}`, money(summaryMetrics.profitUsd, '$')]);
            rows.push(['Profit/Loss', 'Profit (ZAR)', `${money(summaryMetrics.sellSideZar, 'R ')} - ${money(summaryMetrics.buySideZar, 'R ')}`, money(summaryMetrics.profitZar, 'R ')]);
            rows.push(['Profit/Loss', 'Profit Margin', `(${money(summaryMetrics.profitZar, 'R ')} / ${money(summaryMetrics.buySideZar, 'R ')}) x 100`, pct(summaryMetrics.profitMargin)]);

            const csvEscape = (text: string) => `"${String(text ?? '').replace(/"/g, '""')}"`;
            const csv = rows.map(row => row.map(csvEscape).join(',')).join('\r\n');
            const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
            triggerBlobDownload(blob, `ticket_audit_trail_${ticketTradeNum}.csv`);
        } catch (e: unknown) {
            show(String(e), 'error');
        } finally {
            setDownloadingCsv(false);
        }
    };

    return (
        <div>
            <div className="page-header"><h2>Trade Breakdown</h2></div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Trade #</label>
                    <input
                        placeholder="e.g. 9885"
                        value={tradeNum}
                        onChange={e => setTradeNum(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && search()}
                    />
                </div>
                <button className="btn btn-primary btn-sm" onClick={search} style={{ alignSelf: 'flex-end' }}>
                    Load Trade Breakdown
                </button>
            </div>

            {loading && <Loading />}

            {ticket && (
                <>
                    <div className="section">
                        <div className="section-title">Trading Ticket</div>
                        <div className="table-container">
                            <DataTable
                                columns={[
                                    { key: 'Company', label: 'Company' },
                                    { key: 'Weight (g)', label: 'Weight (g)' },
                                    { key: 'Weight (oz)', label: 'Weight (oz)' },
                                    { key: '$/oz Booked', label: '$/oz Booked' },
                                    { key: 'FX Rate', label: 'FX Rate' },
                                    { key: 'USD Value', label: 'USD Value' },
                                    { key: 'ZAR Value', label: 'ZAR Value' },
                                ]}
                                data={tmData}
                                numericCols={['Weight (g)', 'Weight (oz)', '$/oz Booked', 'FX Rate', 'USD Value', 'ZAR Value']}
                                formatters={{
                                    'Weight (g)': { decimals: 2 },
                                    'Weight (oz)': { decimals: 6 },
                                    '$/oz Booked': { decimals: 2 },
                                    'FX Rate': { decimals: 4 },
                                    'USD Value': { decimals: 2 },
                                    'ZAR Value': { decimals: 2 },
                                }}
                            />
                        </div>
                    </div>

                    <div className="section mt-3">
                        <div className="section-title">PMX Trades</div>
                        <DataTable
                            columns={[
                                { key: 'Trade Date', label: 'Trade Date' },
                                { key: 'Value Date', label: 'Value Date' },
                                { key: 'Symbol', label: 'Symbol' },
                                { key: 'Side', label: 'Side' },
                                { key: 'Narration', label: 'Narration' },
                                { key: 'Quantity', label: 'Quantity' },
                                { key: 'Price', label: 'Price' },
                            ]}
                            data={stData}
                            numericCols={['Quantity', 'Price']}
                            dateCols={['Trade Date', 'Value Date']}
                            formatters={{ Quantity: { decimals: 4 }, Price: { decimals: 4 } }}
                        />
                    </div>

                    <div className="section mt-3">
                        <div className="section-title">Calculation Audit Trail</div>
                        <div className="audit-toolbar">
                            <div className="audit-toolbar-title">Trade Breakdown Exports</div>
                            <div className="audit-toolbar-actions">
                                <button className="btn btn-sm" onClick={downloadAuditCsv} disabled={downloadingCsv}>
                                    {downloadingCsv ? 'Building CSV...' : 'CSV'}
                                </button>
                                <button className="btn btn-sm btn-primary" onClick={downloadPdf} disabled={downloadingPdf}>
                                    {downloadingPdf ? 'Generating PDF...' : 'PDF'}
                                </button>
                            </div>
                        </div>
                        <div className="audit-blurb">
                            Step-by-step calculation trace for Trade {ticketTradeNum || '--'}. Each section shows source values, formulas, and outputs for reconciliation.
                        </div>
                        <div className="audit-legend">
                            <strong>Format:</strong> Input x Rate = Result. Monetary values are shown as `$` (USD) and `R` (ZAR).
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">0. Outcome Snapshot</div>
                            <div className="audit-summary-grid">
                                <div className="audit-summary-card">
                                    <div className="audit-summary-title">USD Position</div>
                                    <div className="audit-summary-row"><span>Sell Side (USD)</span><span className="audit-summary-value">{money(summaryMetrics.sellSideUsd, '$')}</span></div>
                                    <div className="audit-summary-row"><span>Buy Side (USD)</span><span className="audit-summary-value">{money(summaryMetrics.buySideUsd, '$')}</span></div>
                                    <div className="audit-summary-row"><span>Profit (USD)</span><span className="audit-summary-value audit-summary-value-key">{money(summaryMetrics.profitUsd, '$')}</span></div>
                                </div>
                                <div className="audit-summary-card">
                                    <div className="audit-summary-title">ZAR Position</div>
                                    <div className="audit-summary-row"><span>Sell Side (ZAR)</span><span className="audit-summary-value">{money(summaryMetrics.sellSideZar, 'R ')}</span></div>
                                    <div className="audit-summary-row"><span>Buy Side (ZAR)</span><span className="audit-summary-value">{money(summaryMetrics.buySideZar, 'R ')}</span></div>
                                    <div className="audit-summary-row"><span>Profit (ZAR)</span><span className="audit-summary-value audit-summary-value-key">{money(summaryMetrics.profitZar, 'R ')}</span></div>
                                </div>
                                <div className="audit-summary-card">
                                    <div className="audit-summary-title">Control Account</div>
                                    <div className="audit-summary-row"><span>Control Account (g)</span><span className="audit-summary-value">{grams(summaryMetrics.controlG)}</span></div>
                                    <div className="audit-summary-row"><span>Control Account (oz)</span><span className="audit-summary-value">{ounces(summaryMetrics.controlOz, 4)}</span></div>
                                    <div className="audit-summary-row"><span>Control Account (ZAR)</span><span className="audit-summary-value audit-summary-value-key">{money(summaryMetrics.controlZar, 'R ')}</span></div>
                                </div>
                                <div className="audit-summary-card">
                                    <div className="audit-summary-title">Trade Totals</div>
                                    <div className="audit-summary-row"><span>Total Traded (g / oz)</span><span className="audit-summary-value">{grams(summaryMetrics.totalTradedG)} / {ounces(summaryMetrics.totalTradedOz, 4)}</span></div>
                                    <div className="audit-summary-row"><span>StoneX ZAR Flow</span><span className="audit-summary-value">{money(summaryMetrics.stonexZarFlow, 'R ')}</span></div>
                                    <div className="audit-summary-row"><span>Profit Margin (%)</span><span className="audit-summary-value audit-summary-value-key">{pct(summaryMetrics.profitMargin, 2)}</span></div>
                                </div>
                            </div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">1. Input Data Summary</div>
                            <div className="audit-step-sub">Raw data loaded from the database for this ticket.</div>
                            <div className="audit-summary-row"><span>TradeMC bookings (buy side)</span><strong>{tmBookings.length}</strong></div>
                            <div className="audit-summary-row"><span>StoneX/PMX trades (sell side)</span><strong>{pmxTrades.length}</strong></div>
                            <div className="audit-summary-row"><span>XAU/USD trades</span><strong>{xauTrades.length}</strong></div>
                            <div className="audit-summary-row"><span>USD/ZAR trades</span><strong>{fxTrades.length}</strong></div>
                            <div className="audit-summary-row"><span>Conversion constant</span><strong>31.1035 g/troy oz</strong></div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">2. TradeMC Buy-Side Valuation</div>
                            <div className="audit-step-sub">Each booking is valued from grams to ounces, priced in USD, converted to ZAR, then reduced by refining rate.</div>
                            {tmBookings.map((booking, idx) => (
                                <div className="audit-booking" key={`audit-booking-${idx}`}>
                                    <div className="audit-booking-title">Booking {idx + 1}: {booking.company}</div>
                                    <div className="audit-summary-row"><span>Weight</span><strong>{grams(booking.weightG)}</strong></div>
                                    <div className="audit-summary-row"><span>Weight in troy ounces</span><strong>{numText(booking.weightG, 2)} / 31.1035 = {ounces(booking.weightOz, 6)}</strong></div>
                                    <div className="audit-summary-row"><span>Booked gold price</span><strong>{money(booking.bookedPrice, '$')} /oz</strong></div>
                                    <div className="audit-summary-row"><span>USD value</span><strong>{ounces(booking.weightOz, 6)} x {money(booking.bookedPrice, '$')} = {money(booking.usdValue, '$')}</strong></div>
                                    <div className="audit-summary-row"><span>FX rate (ZAR/USD)</span><strong>{numText(booking.fxRate, 4)}</strong></div>
                                    <div className="audit-summary-row"><span>ZAR value (gross)</span><strong>{money(booking.usdValue, '$')} x {numText(booking.fxRate, 4)} = {money(booking.zarGross, 'R ')}</strong></div>
                                    <div className="audit-summary-row"><span>Refining rate</span><strong>{pct(booking.refiningRate, 2)}</strong></div>
                                    <div className="audit-summary-row"><span>Refining deduction</span><strong>{money(booking.zarGross, 'R ')} x {pct(booking.refiningRate, 2)} = {money(booking.refiningDeduction, 'R ')}</strong></div>
                                    <div className="audit-summary-row"><span>ZAR value (net of refining)</span><strong>{money(booking.zarNet, 'R ')}</strong></div>
                                </div>
                            ))}
                            <div className="audit-summary-row"><span>Total weight</span><strong>{grams(tmTotals.weightG)} ({ounces(tmTotals.weightOz, 6)})</strong></div>
                            <div className="audit-summary-row"><span>Total USD value</span><strong>{money(tmTotals.usdValue, '$')}</strong></div>
                            <div className="audit-summary-row"><span>Total ZAR (gross)</span><strong>{money(tmTotals.zarGross, 'R ')}</strong></div>
                            <div className="audit-summary-row"><span>Total ZAR (net of refining)</span><strong>{money(tmTotals.zarNet, 'R ')}</strong></div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">3. StoneX Weighted Average Rates</div>
                            <div className="audit-step-sub">Weighted average = Sum(qty x price) / Sum(qty).</div>
                            <div className="audit-booking">
                                <div className="audit-booking-title">Gold Weighted Average ($/oz)</div>
                                {goldWaCalc.rows.length === 0 ? (
                                    <div className="audit-summary-row"><span>XAU/USD trades</span><strong>--</strong></div>
                                ) : (
                                    <>
                                        {goldWaCalc.rows.map((trade, idx) => (
                                            <div className="audit-summary-row" key={`gold-wa-row-${idx}`}>
                                                <span>{trade.side || 'TRADE'} {ounces(trade.qty, 4)} @ {money(trade.price, '$', 4)}</span>
                                                <strong>{ounces(trade.qty, 4)} x {money(trade.price, '$', 4)} = {money(trade.notional, '$')}</strong>
                                            </div>
                                        ))}
                                        <div className="audit-summary-row"><span>Sum of notional values</span><strong>{money(goldWaCalc.totalNotional, '$')}</strong></div>
                                        <div className="audit-summary-row"><span>Sum of quantities</span><strong>{ounces(goldWaCalc.totalQty, 4)}</strong></div>
                                        <div className="audit-summary-row"><span>Gold Weighted Average</span><strong>{money(goldWaCalc.totalNotional, '$')} / {ounces(goldWaCalc.totalQty, 4)} = {money(goldWa, '$', 4)}</strong></div>
                                    </>
                                )}
                            </div>
                            <div className="audit-booking">
                                <div className="audit-booking-title">FX Weighted Average (ZAR/USD)</div>
                                {fxWaCalc.rows.length === 0 ? (
                                    <div className="audit-summary-row"><span>USD/ZAR trades</span><strong>--</strong></div>
                                ) : (
                                    <>
                                        {fxWaCalc.rows.map((trade, idx) => (
                                            <div className="audit-summary-row" key={`fx-wa-row-${idx}`}>
                                                <span>{trade.side || 'TRADE'} {money(trade.qty, '$', 2)} @ {money(trade.price, 'R ', 4)}</span>
                                                <strong>{money(trade.qty, '$', 2)} x {money(trade.price, 'R ', 4)} = {money(trade.notional, 'R ')}</strong>
                                            </div>
                                        ))}
                                        <div className="audit-summary-row"><span>Sum of notional values</span><strong>{money(fxWaCalc.totalNotional, 'R ')}</strong></div>
                                        <div className="audit-summary-row"><span>Sum of quantities</span><strong>{money(fxWaCalc.totalQty, '$', 2)}</strong></div>
                                        <div className="audit-summary-row"><span>FX Weighted Average</span><strong>{money(fxWaCalc.totalNotional, 'R ')} / {money(fxWaCalc.totalQty, '$', 2)} = {money(fxWa, 'R ', 4)}</strong></div>
                                    </>
                                )}
                            </div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">4. Spot Rate Derivation</div>
                            <div className="audit-summary-row">
                                <span>Spot ZAR per gram</span>
                                <strong>({money(goldWa, '$', 4)} x {money(fxWa, 'R ', 4)}) / 31.1035 = {money(spotZarPerG, 'R ', 4)}</strong>
                            </div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">5. StoneX USD Cash Flow (Sell Side)</div>
                            {xauCashFlowRows.map((row, idx) => (
                                <div className="audit-summary-row" key={`xau-cash-${idx}`}>
                                    <span>{row.side} {ounces(row.qty, 4)} @ {money(row.price, '$', 4)}</span>
                                    <strong>{row.signed === null ? '--' : `${row.signed >= 0 ? '+' : '-'}${money(Math.abs(row.signed), '$')}`}</strong>
                                </div>
                            ))}
                            <div className="audit-summary-row"><span>Net StoneX USD cash flow</span><strong>{money(netStoneXUsdFlow, '$')}</strong></div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">6. Control Account Check (Metal Exposure)</div>
                            <div className="audit-summary-row"><span>Control Account (grams)</span><strong>{grams(summaryMetrics.controlG)}</strong></div>
                            <div className="audit-summary-row"><span>Control Account (oz)</span><strong>{ounces(summaryMetrics.controlOz, 4)}</strong></div>
                            <div className="audit-summary-row"><span>Control Account (ZAR)</span><strong>{money(summaryMetrics.controlZar, 'R ')}</strong></div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">7. ZAR Sell-Side Valuation</div>
                            <div className="audit-summary-row"><span>StoneX ZAR flow</span><strong>{money(summaryMetrics.stonexZarFlow, 'R ')}</strong></div>
                            <div className="audit-summary-row"><span>Sell Side ZAR</span><strong>{money(summaryMetrics.sellSideZar, 'R ')}</strong></div>
                            <div className="audit-summary-row"><span>Buy Side ZAR (TradeMC net)</span><strong>{money(summaryMetrics.buySideZar, 'R ')}</strong></div>
                        </div>

                        <div className="audit-step">
                            <div className="audit-step-num">8. Final Profit / Loss</div>
                            <div className="audit-summary-row"><span>Profit (USD)</span><strong>{money(summaryMetrics.sellSideUsd, '$')} - {money(summaryMetrics.buySideUsd, '$')} = {money(summaryMetrics.profitUsd, '$')}</strong></div>
                            <div className="audit-summary-row"><span>Profit (ZAR)</span><strong>{money(summaryMetrics.sellSideZar, 'R ')} - {money(summaryMetrics.buySideZar, 'R ')} = {money(summaryMetrics.profitZar, 'R ')}</strong></div>
                            <div className="audit-summary-row"><span>Profit Margin</span><strong>{pct(summaryMetrics.profitMargin, 2)}</strong></div>
                        </div>
                    </div>
                </>
            )}

            {!loading && !ticket && <Empty title="Enter a trade number to view trade breakdown and audit trail" />}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: PROFIT
// ===================================================================
function ProfitTab() {
    const DEFAULT_METAL_TOLERANCE_G = 32.0;
    const GRAMS_PER_TROY_OUNCE = 31.1035;
    const MONTHLY_TARGET_RATE = 0.0015;
    const MONTHLY_TARGET_RAND_PER_GRAM = 1.3;
    const [payload, setPayload] = useState<{ months: Row[]; summary: Row }>({ months: [], summary: {} });
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [loadError, setLoadError] = useState('');
    const [expandedMonths, setExpandedMonths] = useState<Record<string, boolean>>({});
    const [expandedTrades, setExpandedTrades] = useState<Record<string, boolean>>({});
    const [hedgeStatusByTrade, setHedgeStatusByTrade] = useState<Record<string, string>>({});
    const [metalTolerance] = usePersistentState('filters:hedging:metal_tolerance', DEFAULT_METAL_TOLERANCE_G);
    const [usdTolerance] = usePersistentState('filters:hedging:usd_tolerance', 1.0);
    const [monthlyProfitPeriod, setMonthlyProfitPeriod] = usePersistentState<string>('filters:profit:monthly_period', 'all');
    const { show, Toast } = useToast();

    const load = useCallback(async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);
        try {
            const [res, hedgingRes] = await Promise.all([
                api.getProfitMonthly(),
                api.getHedging().catch(() => [] as Row[]),
            ]);
            const months = Array.isArray((res as { months?: unknown }).months) ? ((res as { months: Row[] }).months) : [];
            const summary = (((res as { summary?: unknown }).summary) && typeof (res as { summary?: unknown }).summary === 'object')
                ? ((res as { summary: Row }).summary)
                : {};
            const hedgingRows = Array.isArray(hedgingRes) ? (hedgingRes as Row[]) : [];
            const metalTol = Number.isFinite(metalTolerance) ? Math.abs(metalTolerance) : DEFAULT_METAL_TOLERANCE_G;
            const usdTol = Number.isFinite(usdTolerance) ? Math.abs(usdTolerance) : 1.0;
            const toNumber = (val: unknown): number => {
                if (val === null || val === undefined) return NaN;
                const s = String(val).replace(/,/g, '').trim();
                if (!s || s === '--') return NaN;
                const n = Number(s);
                return Number.isFinite(n) ? n : NaN;
            };
            const nextStatusByTrade: Record<string, string> = {};
            for (const row of hedgingRows) {
                const tn = normalizeTradeNumberValue((row as Row).trade_num);
                if (!tn) continue;
                const hedgeNeedG = toNumber((row as Row).hedge_need_g);
                const usdToCutRaw = toNumber((row as Row).usd_to_cut ?? (row as Row).usd_need);
                const pmxNetUsd = toNumber((row as Row).pmx_net_usd);
                const usdToCut = Number.isFinite(usdToCutRaw)
                    ? usdToCutRaw
                    : (Number.isFinite(pmxNetUsd) ? Math.abs(pmxNetUsd) : NaN);
                const metalHedged = Number.isFinite(hedgeNeedG) && Math.abs(hedgeNeedG) <= (metalTol + 1e-9);
                const usdHedged = Number.isFinite(usdToCut) && Math.abs(usdToCut) <= (usdTol + 1e-9);
                nextStatusByTrade[tn] = (metalHedged && usdHedged) ? 'Hedged' : 'Unhedged';
            }
            setPayload({ months, summary });
            setHedgeStatusByTrade(nextStatusByTrade);
            setLoadError('');
        } catch (e: unknown) {
            const msg = String(e);
            setPayload({ months: [], summary: {} });
            setHedgeStatusByTrade({});
            setLoadError(msg);
            show(`Failed to load monthly profit: ${msg}`, 'error');
        } finally {
            if (isRefresh) setRefreshing(false);
            else setLoading(false);
        }
    }, [metalTolerance, show, usdTolerance]);

    useEffect(() => { void load(false); }, [load]);

    const months = payload.months;
    const summary = payload.summary || {};
    const monthKeyOf = (month: Row, idx: number) => asText(month.month_key, `month-${idx}`);
    const n = (v: unknown) => typeof v === 'number' ? v : Number(v) || 0;
    const normalizeSymbol = (value: unknown): string =>
        String(value ?? '').toUpperCase().replace(/[\/\-\s]/g, '');
    const monthProfitPctWeightedAvg = (trades: Row[]): number => {
        const weighted = trades.reduce<{ weightedPct: number; totalWeight: number }>((acc, trade) => {
            const pct = toNullableNumber(trade.profit_pct);
            const tradedG = toNullableNumber(trade.stonex_traded_g);
            if (pct === null || tradedG === null) return acc;
            const weight = Math.abs(tradedG);
            if (!(weight > 0)) return acc;
            acc.weightedPct += pct * weight;
            acc.totalWeight += weight;
            return acc;
        }, { weightedPct: 0, totalWeight: 0 });
        if (!(weighted.totalWeight > 0)) return 0;
        return weighted.weightedPct / weighted.totalWeight;
    };
    const monthNetTradedG = (trades: Row[]): number =>
        trades.reduce((sum, trade) => {
            const pmxTx = Array.isArray(trade.pmx_transactions) ? (trade.pmx_transactions as Row[]) : [];
            const tradeNetG = pmxTx.reduce((sub, tx) => {
                if (normalizeSymbol(tx['Symbol']) !== 'XAUUSD') return sub;
                const qty = toNullableNumber(tx['Quantity']);
                if (qty === null) return sub;
                const side = String(tx['Side'] ?? '').toUpperCase().trim();
                if (side === 'BUY') return sub + (Math.abs(qty) * GRAMS_PER_TROY_OUNCE);
                if (side === 'SELL') return sub - (Math.abs(qty) * GRAMS_PER_TROY_OUNCE);
                return sub;
            }, 0);
            return sum + tradeNetG;
        }, 0);

    const monthlyChartDataAll = months
        .map((month, monthIdx) => {
            const monthKey = monthKeyOf(month, monthIdx);
            const monthLabel = asText(month.month_label, monthKey);
            const trades = Array.isArray(month.trades) ? (month.trades as Row[]) : [];
            const monthNetProfit = Number.isFinite(Number(month.total_profit_zar))
                ? n(month.total_profit_zar)
                : trades.reduce((sum, t) => sum + n(t.total_profit_zar), 0);

            let tmBuyAbsZar = 0;
            let tmSellAbsZar = 0;
            let tmAbsWeightG = 0;

            for (const t of trades) {
                const tradeWeightG = n(t.client_weight_g);
                let tradeBuyAbsZar = 0;
                let tradeSellAbsZar = 0;
                let tradeAbsWeightG = 0;

                const tmTx = Array.isArray((t as { trademc_transactions?: Row[] }).trademc_transactions)
                    ? ((t as { trademc_transactions: Row[] }).trademc_transactions)
                    : [];
                for (const tx of tmTx) {
                    const txWeightG = n(tx['Weight (g)']);
                    const txZarAbs = Math.abs(n(tx['ZAR Value']));
                    if (txZarAbs <= 1e-12) continue;
                    tradeAbsWeightG += Math.abs(txWeightG);
                    if (txWeightG >= 0) tradeBuyAbsZar += txZarAbs;
                    else tradeSellAbsZar += txZarAbs;
                }

                if (tradeBuyAbsZar <= 1e-12 && tradeSellAbsZar <= 1e-12) {
                    const buySideAbs = Math.abs(n(t.buy_side_zar));
                    const sellSideAbs = Math.abs(n(t.sell_side_zar));
                    if (tradeWeightG >= 0) tradeBuyAbsZar = buySideAbs;
                    else tradeSellAbsZar = sellSideAbs;
                }
                if (tradeAbsWeightG <= 1e-12) tradeAbsWeightG = Math.abs(tradeWeightG);

                tmBuyAbsZar += tradeBuyAbsZar;
                tmSellAbsZar += tradeSellAbsZar;
                tmAbsWeightG += tradeAbsWeightG;
            }

            const targetBaseAbsZar = tmBuyAbsZar + tmSellAbsZar;
            const monthlyTarget = targetBaseAbsZar * MONTHLY_TARGET_RATE;
            const netProfitPerGram = tmAbsWeightG > 1e-9 ? monthNetProfit / tmAbsWeightG : 0;
            return {
                monthKey,
                monthLabel,
                netProfit: monthNetProfit,
                monthlyTarget,
                targetDelta: monthNetProfit - monthlyTarget,
                hitTarget: monthlyTarget > 0 ? monthNetProfit >= monthlyTarget : false,
                tmAbsWeightG,
                netProfitPerGram,
                monthlyTargetPerGram: MONTHLY_TARGET_RAND_PER_GRAM,
                targetDeltaPerGram: netProfitPerGram - MONTHLY_TARGET_RAND_PER_GRAM,
                hitTargetPerGram: netProfitPerGram >= MONTHLY_TARGET_RAND_PER_GRAM,
            };
        })
        .sort((a, b) => String(a.monthKey).localeCompare(String(b.monthKey)));

    const monthlyChartData = (() => {
        if (monthlyProfitPeriod === 'all') return monthlyChartDataAll;
        const monthsCount = parseInt(monthlyProfitPeriod, 10) || 6;
        return monthlyChartDataAll.slice(-monthsCount);
    })();

    const totalMonthlyNetProfit = monthlyChartData.reduce((sum, row) => sum + row.netProfit, 0);
    const totalMonthlyTarget = monthlyChartData.reduce((sum, row) => sum + row.monthlyTarget, 0);
    const totalMonthlyTargetGap = totalMonthlyNetProfit - totalMonthlyTarget;
    const totalMonthlyAbsWeightG = monthlyChartData.reduce((sum, row) => sum + row.tmAbsWeightG, 0);
    const weightedMonthlyNetProfitPerGram = totalMonthlyAbsWeightG > 1e-9 ? (totalMonthlyNetProfit / totalMonthlyAbsWeightG) : 0;
    const totalMonthlyTargetGapPerGram = weightedMonthlyNetProfitPerGram - MONTHLY_TARGET_RAND_PER_GRAM;
    const totalMonthlyPerGramTargetZar = MONTHLY_TARGET_RAND_PER_GRAM * totalMonthlyAbsWeightG;
    const totalMonthlyPerGramGapZar = totalMonthlyNetProfit - totalMonthlyPerGramTargetZar;

    const renderProfitHurdleBarShape = (props: { x?: number; y?: number; width?: number; height?: number }) => {
        const { x, y, width, height } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        const topY = height >= 0 ? y : y + height;
        const widerWidth = width * 1.5;
        const offsetX = x - ((widerWidth - width) / 2);
        return (
            <rect
                x={offsetX}
                y={topY}
                width={widerWidth}
                height={absHeight}
                fill="rgba(180,114,61,0.20)"
                stroke="#b4723d"
                strokeWidth={1.5}
                strokeDasharray="4 3"
                rx={2}
                ry={2}
            />
        );
    };

    const renderProfitAchievedBarShape = (props: { x?: number; y?: number; width?: number; height?: number; payload?: { netProfit?: number } }) => {
        const { x, y, width, height, payload } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        if (absHeight <= 0.5) return null;
        const topY = height >= 0 ? y : y + height;
        const innerWidth = Math.max(3, width * 0.58);
        const innerX = x + ((width - innerWidth) / 2);
        const netProfit = Number(payload?.netProfit ?? 0);
        const fill = netProfit >= 0 ? "#10b981" : "#ef4444";
        return <rect x={innerX} y={topY} width={innerWidth} height={absHeight} fill={fill} stroke={fill} strokeWidth={0} rx={1.5} ry={1.5} />;
    };

    const renderProfitPerGramAchievedBarShape = (props: { x?: number; y?: number; width?: number; height?: number; payload?: { netProfitPerGram?: number } }) => {
        const { x, y, width, height, payload } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        if (absHeight <= 0.5) return null;
        const topY = height >= 0 ? y : y + height;
        const innerWidth = Math.max(3, width * 0.58);
        const innerX = x + ((width - innerWidth) / 2);
        const netProfitPerGram = Number(payload?.netProfitPerGram ?? 0);
        const fill = netProfitPerGram >= 0 ? "#10b981" : "#ef4444";
        return <rect x={innerX} y={topY} width={innerWidth} height={absHeight} fill={fill} stroke={fill} strokeWidth={0} rx={1.5} ry={1.5} />;
    };

    const renderNetDot = (props: { cx?: number; cy?: number; payload?: { netProfit?: number; monthlyTarget?: number } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return null;
        const netProfit = Number(payload?.netProfit ?? 0);
        const hurdle = Number(payload?.monthlyTarget ?? 0);
        const color = netProfit >= hurdle ? '#10b981' : '#111111';
        return <circle cx={cx} cy={cy} r={5} fill={color} stroke="#000000" strokeWidth={1.5} />;
    };

    const renderNetPerGramDot = (props: { cx?: number; cy?: number; payload?: { netProfitPerGram?: number; monthlyTargetPerGram?: number } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return null;
        const netProfitPerGram = Number(payload?.netProfitPerGram ?? 0);
        const hurdlePerGram = Number(payload?.monthlyTargetPerGram ?? MONTHLY_TARGET_RAND_PER_GRAM);
        const color = netProfitPerGram >= hurdlePerGram ? '#10b981' : '#111111';
        return <circle cx={cx} cy={cy} r={5} fill={color} stroke="#000000" strokeWidth={1.5} />;
    };

    const renderMonthlyProfitTooltip = (props: any) => {
        const active = Boolean(props?.active);
        const payload = Array.isArray(props?.payload) ? props.payload : [];
        if (!active || payload.length === 0) return null;
        const row = (payload[0]?.payload || {}) as Row;
        const monthLabel = String((row as Row).monthLabel || '--');
        const netProfit = n((row as Row).netProfit);
        const monthlyTarget = n((row as Row).monthlyTarget);
        return (
            <div className="dashboard-tooltip">
                <div className="dashboard-tooltip-title">{monthLabel}</div>
                <div className="dashboard-tooltip-row"><span>Net Profit</span><strong>R{fmt(netProfit, 2)}</strong></div>
                <div className="dashboard-tooltip-row"><span>Hurdle (0.15%)</span><strong>R{fmt(monthlyTarget, 2)}</strong></div>
            </div>
        );
    };

    const renderMonthlyProfitPerGramTooltip = (props: any) => {
        const active = Boolean(props?.active);
        const payload = Array.isArray(props?.payload) ? props.payload : [];
        if (!active || payload.length === 0) return null;
        const row = (payload[0]?.payload || {}) as Row;
        const monthLabel = String((row as Row).monthLabel || '--');
        const netProfitPerGram = n((row as Row).netProfitPerGram);
        const monthlyTargetPerGram = n((row as Row).monthlyTargetPerGram);
        return (
            <div className="dashboard-tooltip">
                <div className="dashboard-tooltip-title">{monthLabel}</div>
                <div className="dashboard-tooltip-row"><span>Net Profit (R/g)</span><strong>R{fmt(netProfitPerGram, 2)}/g</strong></div>
                <div className="dashboard-tooltip-row"><span>Hurdle</span><strong>R{fmt(monthlyTargetPerGram, 2)}/g</strong></div>
            </div>
        );
    };

    const toggleMonth = (monthKey: string) => {
        setExpandedMonths(prev => ({ ...prev, [monthKey]: !prev[monthKey] }));
    };
    const toggleTrade = (monthKey: string, tradeNum: string) => {
        const key = `${monthKey}::${tradeNum}`;
        setExpandedTrades(prev => ({ ...prev, [key]: !prev[key] }));
    };

    if (loading) return <><Loading text="Loading monthly profit..." />{Toast}</>;
    if (loadError) return <><Empty title="Could not load monthly profit" sub={loadError} />{Toast}</>;

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Profit</h2>
                    <div className="page-subtitle">Monthly profit summary with trade and transaction drill-down</div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(true); }} disabled={refreshing}>
                        {refreshing ? 'Refreshing...' : 'Refresh'}
                    </button>
                </div>
            </div>

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Months</div>
                    <div className="stat-value">{fmt(summary.months ?? 0, 0)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Trades</div>
                    <div className="stat-value">{fmt(summary.trades ?? 0, 0)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Exchange Profit (ZAR)</div>
                    <div className={`stat-value ${numClass(summary.exchange_profit_zar).replace('num ', '')}`}>R{fmt(summary.exchange_profit_zar)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Metal Profit (ZAR)</div>
                    <div className={`stat-value ${numClass(summary.metal_profit_zar).replace('num ', '')}`}>R{fmt(summary.metal_profit_zar)}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Total Profit (ZAR)</div>
                    <div className={`stat-value ${numClass(summary.total_profit_zar).replace('num ', '')}`}>R{fmt(summary.total_profit_zar)}</div>
                </div>
            </div>

            {months.length === 0 ? (
                <Empty title="No monthly profit data available" />
            ) : (
                <>
                <div className="table-container">
                    <table className="data-table">
                        <thead>
                            <tr>
                                <th>Month</th>
                                <th>Trades</th>
                                <th>Exchange Profit (ZAR)</th>
                                <th>Metal Profit (ZAR)</th>
                                <th>Total Profit (ZAR)</th>
                                <th>Profit % (WAvg)</th>
                                <th>Traded (g)</th>
                                <th>ABS Traded (g)</th>
                                <th>Profit (R/g)</th>
                                <th>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {months.map((month, monthIdx) => {
                                const monthKey = monthKeyOf(month, monthIdx);
                                const expandedMonth = Boolean(expandedMonths[monthKey]);
                                const trades = Array.isArray(month.trades) ? (month.trades as Row[]) : [];
                                const tradesSorted = [...trades].sort((a, b) => {
                                    const aTs = toTimestampMs(a.trade_date);
                                    const bTs = toTimestampMs(b.trade_date);
                                    const aValid = Number.isFinite(aTs);
                                    const bValid = Number.isFinite(bTs);
                                    if (aValid && bValid && Math.abs(bTs - aTs) > 1e-9) return bTs - aTs;
                                    if (aValid !== bValid) return aValid ? -1 : 1;
                                    const ta = normalizeTradeNumberValue(a.trade_num);
                                    const tb = normalizeTradeNumberValue(b.trade_num);
                                    return ta.localeCompare(tb, undefined, { numeric: true, sensitivity: 'base' });
                                });
                                const pctWeightedAvg = monthProfitPctWeightedAvg(trades);
                                const netTradedG = monthNetTradedG(trades);
                                const absTradedG = trades.reduce((s, t) => s + (Number(t.stonex_traded_g) || 0), 0);
                                const profitPerG = absTradedG > 0 ? (Number(month.total_profit_zar) || 0) / absTradedG : 0;
                                return (
                                    <Fragment key={monthKey}>
                                        <tr>
                                            <td>{asText(month.month_label, monthKey)}</td>
                                            <td className="num">{fmt(month.trade_count ?? trades.length, 0)}</td>
                                            <td className={numClass(month.exchange_profit_zar)}>R{fmt(month.exchange_profit_zar)}</td>
                                            <td className={numClass(month.metal_profit_zar)}>R{fmt(month.metal_profit_zar)}</td>
                                            <td className={numClass(month.total_profit_zar)}>R{fmt(month.total_profit_zar)}</td>
                                            <td className={numClass(pctWeightedAvg)}>{fmt(pctWeightedAvg, 3)}%</td>
                                            <td className={numClass(netTradedG)}>{fmt(netTradedG, 2)}</td>
                                            <td className="num">{fmt(absTradedG, 2)}</td>
                                            <td className={numClass(profitPerG)}>R{fmt(profitPerG, 2)}</td>
                                            <td>
                                                <button className="btn btn-sm" onClick={() => toggleMonth(monthKey)}>
                                                    {expandedMonth ? 'Collapse' : 'Expand'}
                                                </button>
                                            </td>
                                        </tr>
                                        {expandedMonth && (
                                            <tr>
                                                <td colSpan={12} style={{ padding: '0.75rem' }}>
                                                    <div className="table-container">
                                                        <table className="data-table">
                                                            <thead>
                                                                <tr>
                                                                    <th>Trade #</th>
                                                                    <th>Trade Date</th>
                                                                    <th>Client Weight (g)</th>
                                                                    <th>StoneX Traded (g)</th>
                                                                    <th>Exchange Profit (ZAR)</th>
                                                                    <th>Metal Profit (ZAR)</th>
                                                                    <th>Total Profit (ZAR)</th>
                                                                    <th>Profit %</th>
                                                                    <th>ABS Traded (g)</th>
                                                                    <th>Profit (R/g)</th>
                                                                    <th>Hedge Status</th>
                                                                    <th>Transactions</th>
                                                                </tr>
                                                            </thead>
                                                            <tbody>
                                                                {tradesSorted.length === 0 && (
                                                                    <tr><td colSpan={12} style={{ textAlign: 'left', padding: '1rem', color: 'var(--text-muted)' }}>No trades for this month</td></tr>
                                                                )}
                                                                {tradesSorted.map((trade, tradeIdx) => {
                                                                    const tradeNum = asText(trade.trade_num, `trade-${tradeIdx}`);
                                                                    const tradeExpandKey = `${monthKey}::${tradeNum}`;
                                                                    const expandedTrade = Boolean(expandedTrades[tradeExpandKey]);
                                                                    const tmTx = Array.isArray(trade.trademc_transactions) ? (trade.trademc_transactions as Row[]) : [];
                                                                    const pmxTx = Array.isArray(trade.pmx_transactions) ? (trade.pmx_transactions as Row[]) : [];
                                                                    const hedgeStatus = hedgeStatusByTrade[normalizeTradeNumberValue(tradeNum)]
                                                                        ?? asText(trade.hedge_status, Boolean(trade.hedged) ? 'Hedged' : 'Unhedged');
                                                                    const tradeAbsG = Number(trade.stonex_traded_g) || 0;
                                                                    const tradeProfitPerG = tradeAbsG > 0 ? (Number(trade.total_profit_zar) || 0) / tradeAbsG : 0;
                                                                    return (
                                                                        <Fragment key={tradeExpandKey}>
                                                                            <tr>
                                                                                <td>{tradeNum}</td>
                                                                                <td>{fmtDate(trade.trade_date)}</td>
                                                                                <td className={numClass(trade.client_weight_g)}>{fmt(trade.client_weight_g)}</td>
                                                                                <td className={numClass(trade.stonex_traded_g)}>{fmt(trade.stonex_traded_g, 2)}</td>
                                                                                <td className={numClass(trade.exchange_profit_zar)}>R{fmt(trade.exchange_profit_zar)}</td>
                                                                                <td className={numClass(trade.metal_profit_zar)}>R{fmt(trade.metal_profit_zar)}</td>
                                                                                <td className={numClass(trade.total_profit_zar)}>R{fmt(trade.total_profit_zar)}</td>
                                                                                <td className={numClass(trade.profit_pct)}>{fmt(trade.profit_pct, 3)}%</td>
                                                                                <td className="num">{fmt(tradeAbsG, 2)}</td>
                                                                                <td className={numClass(tradeProfitPerG)}>R{fmt(tradeProfitPerG, 2)}</td>
                                                                                <td>{hedgeStatus}</td>
                                                                                <td>
                                                                                    <button className="btn btn-sm" onClick={() => toggleTrade(monthKey, tradeNum)}>
                                                                                        {expandedTrade ? 'Collapse' : 'Expand'}
                                                                                    </button>
                                                                                </td>
                                                                            </tr>
                                                                            {expandedTrade && (
                                                                                <tr>
                                                                                    <td colSpan={12} style={{ padding: '0.75rem' }}>
                                                                                        {(() => {
                                                                                            const tmWaGold = toNullableNumber(trade.trademc_wa_gold_usd_oz);
                                                                                            const tmWaFx = toNullableNumber(trade.trademc_wa_usdzar);
                                                                                            return (
                                                                                                <div className="stat-sub" style={{ marginBottom: '0.5rem' }}>
                                                                                                    {`TradeMC WA: ${tmWaGold !== null ? `$${fmt(tmWaGold, 4)}/oz` : '--'}`}
                                                                                                    {` | FX ${tmWaFx !== null ? fmt(tmWaFx, 4) : '--'}`}
                                                                                                </div>
                                                                                            );
                                                                                        })()}
                                                                                        <div className="section-title">TradeMC Transactions</div>
                                                                                        <DataTable
                                                                                            columns={[
                                                                                                { key: 'Date', label: 'Date' },
                                                                                                { key: 'Company', label: 'Company' },
                                                                                                { key: 'Weight (g)', label: 'Weight (g)' },
                                                                                                { key: 'USD/oz', label: 'USD/oz' },
                                                                                                { key: 'FX Rate', label: 'FX Rate' },
                                                                                                { key: 'ZAR Value', label: 'ZAR Value' },
                                                                                                { key: 'ID', label: 'ID' },
                                                                                            ]}
                                                                                            data={tmTx}
                                                                                            numericCols={['Weight (g)', 'USD/oz', 'FX Rate', 'ZAR Value']}
                                                                                            dateCols={['Date']}
                                                                                            formatters={{
                                                                                                'Weight (g)': { decimals: 2 },
                                                                                                'USD/oz': { decimals: 4 },
                                                                                                'FX Rate': { decimals: 4 },
                                                                                                'ZAR Value': { decimals: 2 },
                                                                                            }}
                                                                                        />
                                                                                        {(() => {
                                                                                            const pmxWaGold = toNullableNumber(trade.pmx_wa_gold_usd_oz);
                                                                                            const pmxWaFx = toNullableNumber(trade.pmx_wa_usdzar);
                                                                                            return (
                                                                                                <div className="stat-sub" style={{ marginTop: '0.65rem', marginBottom: '0.5rem' }}>
                                                                                                    {`PMX WA: ${pmxWaGold !== null ? `$${fmt(pmxWaGold, 4)}/oz` : '--'}`}
                                                                                                    {` | FX ${pmxWaFx !== null ? fmt(pmxWaFx, 4) : '--'}`}
                                                                                                </div>
                                                                                            );
                                                                                        })()}
                                                                                        <div className="section-title mt-3">PMX Transactions</div>
                                                                                        <DataTable
                                                                                            columns={[
                                                                                                { key: 'Trade Date', label: 'Trade Date' },
                                                                                                { key: 'Value Date', label: 'Value Date' },
                                                                                                { key: 'FNC #', label: 'FNC #' },
                                                                                                { key: 'Symbol', label: 'Symbol' },
                                                                                                { key: 'Side', label: 'Side' },
                                                                                                { key: 'Quantity', label: 'Quantity' },
                                                                                                { key: 'Price', label: 'Price' },
                                                                                                { key: 'Narration', label: 'Narration' },
                                                                                            ]}
                                                                                            data={pmxTx}
                                                                                            numericCols={['Quantity', 'Price']}
                                                                                            dateCols={['Trade Date', 'Value Date']}
                                                                                            formatters={{
                                                                                                'Quantity': { decimals: 2 },
                                                                                                'Price': { decimals: 4 },
                                                                                            }}
                                                                                        />
                                                                                    </td>
                                                                                </tr>
                                                                            )}
                                                                        </Fragment>
                                                                    );
                                                                })}
                                                            </tbody>
                                                        </table>
                                                    </div>
                                                </td>
                                            </tr>
                                        )}
                                    </Fragment>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
                <div className="section">
                    <div className="dashboard-chart dashboard-chart-elevated" style={{ marginTop: 14 }}>
                        <div className="dashboard-chart-head">
                            <div>
                                <div className="dashboard-chart-title">Net Profit vs Monthly Hurdle</div>
                                <div className="dashboard-chart-subtitle">Monthly target = 0.15% × (|TradeMC Buy ZAR| + |TradeMC Sell ZAR|).</div>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                                <select
                                    value={monthlyProfitPeriod}
                                    onChange={e => setMonthlyProfitPeriod(e.target.value)}
                                    className="input"
                                    style={{ width: 'auto', minWidth: 130, fontSize: 12, padding: '4px 8px', margin: 0 }}
                                >
                                    <option value="3">Last 3 months</option>
                                    <option value="6">Last 6 months</option>
                                    <option value="12">Last 12 months</option>
                                    <option value="24">Last 24 months</option>
                                    <option value="all">All time</option>
                                </select>
                                <div className="dashboard-chart-kpis">
                                    <div className="dashboard-kpi">
                                        <div className="dashboard-kpi-label">Total Net Profit</div>
                                        <div className={`dashboard-kpi-value ${totalMonthlyNetProfit >= 0 ? 'positive' : 'negative'}`}>R{fmt(totalMonthlyNetProfit, 2)}</div>
                                    </div>
                                    <div className="dashboard-kpi">
                                        <div className="dashboard-kpi-label">Net vs Target</div>
                                        <div className={`dashboard-kpi-value ${totalMonthlyTargetGap >= 0 ? 'positive' : 'negative'}`}>
                                            {totalMonthlyTargetGap >= 0 ? '+' : ''}R{fmt(totalMonthlyTargetGap, 2)}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <ResponsiveContainer width="100%" height={360}>
                            <ComposedChart
                                data={monthlyChartData}
                                barSize={16}
                                barGap={-16}
                                margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                            >
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                <XAxis
                                    dataKey="monthLabel"
                                    tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                    tickLine={false}
                                    axisLine={{ stroke: 'rgba(28,28,28,0.2)' }}
                                    interval="preserveStartEnd"
                                />
                                <YAxis
                                    tickFormatter={(v: number) => `R${fmt(v / 1000, 0)}k`}
                                    tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                    tickLine={false}
                                    axisLine={false}
                                    width={54}
                                />
                                <Tooltip content={renderMonthlyProfitTooltip} cursor={{ fill: 'rgba(180,114,61,0.08)' }} />
                                <ReferenceLine y={0} stroke="rgba(28,28,28,0.25)" strokeDasharray="2 2" />
                                <Bar dataKey="monthlyTarget" shape={renderProfitHurdleBarShape} isAnimationActive={false} />
                                <Bar dataKey="netProfit" shape={renderProfitAchievedBarShape} isAnimationActive={false} />
                                <Line
                                    type="monotone"
                                    dataKey="netProfit"
                                    name="Net Profit (ZAR)"
                                    stroke="#111111"
                                    strokeWidth={2}
                                    dot={renderNetDot}
                                    activeDot={renderNetDot}
                                    isAnimationActive={false}
                                />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </div>
                    <div className="dashboard-chart dashboard-chart-elevated" style={{ marginTop: 14 }}>
                        <div className="dashboard-chart-head">
                            <div>
                                <div className="dashboard-chart-title">Net Profit (R/g) vs Monthly Hurdle</div>
                                <div className="dashboard-chart-subtitle">Monthly hurdle = R1.3/g.</div>
                            </div>
                            <div className="dashboard-chart-kpis">
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">Total Net Profit</div>
                                    <div className={`dashboard-kpi-value ${totalMonthlyNetProfit >= 0 ? 'positive' : 'negative'}`}>R{fmt(totalMonthlyNetProfit, 2)}</div>
                                </div>
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">Weighted Net R/g</div>
                                    <div className={`dashboard-kpi-value ${weightedMonthlyNetProfitPerGram >= 0 ? 'positive' : 'negative'}`}>R{fmt(weightedMonthlyNetProfitPerGram, 2)}/g</div>
                                </div>
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">R/g vs Hurdle</div>
                                    <div className={`dashboard-kpi-value ${totalMonthlyTargetGapPerGram >= 0 ? 'positive' : 'negative'}`}>
                                        {totalMonthlyTargetGapPerGram >= 0 ? '+' : ''}R{fmt(totalMonthlyTargetGapPerGram, 2)}/g
                                    </div>
                                </div>
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">Net Over R/g Target</div>
                                    <div className={`dashboard-kpi-value ${totalMonthlyPerGramGapZar >= 0 ? 'positive' : 'negative'}`}>
                                        {totalMonthlyPerGramGapZar >= 0 ? '+' : ''}R{fmt(totalMonthlyPerGramGapZar, 2)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        <ResponsiveContainer width="100%" height={360}>
                            <ComposedChart
                                data={monthlyChartData}
                                barSize={16}
                                barGap={-16}
                                margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                            >
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                <XAxis
                                    dataKey="monthLabel"
                                    tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                    tickLine={false}
                                    axisLine={{ stroke: 'rgba(28,28,28,0.2)' }}
                                    interval="preserveStartEnd"
                                />
                                <YAxis
                                    tickFormatter={(v: number) => `R${fmt(v, 2)}/g`}
                                    tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                    tickLine={false}
                                    axisLine={false}
                                    width={64}
                                />
                                <Tooltip content={renderMonthlyProfitPerGramTooltip} cursor={{ fill: 'rgba(180,114,61,0.08)' }} />
                                <ReferenceLine y={0} stroke="rgba(28,28,28,0.25)" strokeDasharray="2 2" />
                                <Bar dataKey="monthlyTargetPerGram" shape={renderProfitHurdleBarShape} isAnimationActive={false} />
                                <Bar dataKey="netProfitPerGram" shape={renderProfitPerGramAchievedBarShape} isAnimationActive={false} />
                                <Line
                                    type="monotone"
                                    dataKey="netProfitPerGram"
                                    name="Net Profit (R/g)"
                                    stroke="#111111"
                                    strokeWidth={2}
                                    dot={renderNetPerGramDot}
                                    activeDot={renderNetPerGramDot}
                                    isAnimationActive={false}
                                />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </div>
                </div>
                </>
            )}

            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: EXPORT TRADES
// ===================================================================
function ExportTrades() {
    type ExportTradeRow = {
        trade_num: string;
        tm_weight_g: number | null;
        xau_usd_wa: number | null;
        fx_wa: number | null;
        fnc_numbers: string[];
        fnc_count: number;
        ledger_rows: Row[];
    };

    const [rows, setRows] = useState<ExportTradeRow[]>([]);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [downloading, setDownloading] = useState(false);
    const [selectedTrades, setSelectedTrades] = useState<Record<string, boolean>>({});
    const [expandedTrades, setExpandedTrades] = useState<Record<string, boolean>>({});
    const [tradeSearch, setTradeSearch] = usePersistentState('filters:export_trades:trade_search', '');
    const { show, Toast } = useToast();

    const load = useCallback(async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);

        try {
            const [hedgingRaw, ledgerRaw] = await Promise.all([
                api.getHedging().catch(() => [] as Row[]),
                api.getPmxLedger().catch(() => [] as Row[]),
            ]);
            const hedgingRows = Array.isArray(hedgingRaw) ? hedgingRaw : [];
            const ledgerRows = Array.isArray(ledgerRaw) ? ledgerRaw : [];

            // Any allocated trade (non-empty Trade #) should appear on this tab.
            const ledgerByTrade = new Map<string, Row[]>();
            for (const raw of ledgerRows) {
                const row = raw as Row;
                const tradeNum = normalizeTradeNumberValue(row['Trade #']);
                if (!tradeNum) continue;
                const current = ledgerByTrade.get(tradeNum);
                if (current) current.push(row);
                else ledgerByTrade.set(tradeNum, [row]);
            }

            const tmWeightByTrade = new Map<string, number | null>();
            for (const raw of hedgingRows) {
                const row = raw as Row;
                const tradeNum = normalizeTradeNumberValue(row.trade_num);
                if (!tradeNum || tmWeightByTrade.has(tradeNum)) continue;
                tmWeightByTrade.set(tradeNum, toNullableNumber(row.tm_weight_g));
            }

            const candidates = Array.from(ledgerByTrade.keys()).sort((a, b) =>
                a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
            );
            const details = await Promise.all(candidates.map(async (tradeNum) => {
                const waRaw = await api.getWeightedAverage(tradeNum).catch(() => null);
                const tradeLedgerRows = ledgerByTrade.get(tradeNum) || [];

                const fncNumbers = Array.from(new Set(
                    tradeLedgerRows
                        .flatMap(row => extractPmxSupportDocs((row as Row)['FNC #']))
                        .filter(text => text !== '')
                ));

                return {
                    trade_num: tradeNum,
                    tm_weight_g: tmWeightByTrade.has(tradeNum) ? (tmWeightByTrade.get(tradeNum) ?? null) : null,
                    xau_usd_wa: toNullableNumber((waRaw as Row | null)?.xau_usd_wa_price),
                    fx_wa: toNullableNumber((waRaw as Row | null)?.usd_zar_wa_price),
                    fnc_numbers: fncNumbers,
                    fnc_count: fncNumbers.length,
                    ledger_rows: tradeLedgerRows,
                } as ExportTradeRow;
            }));

            const exportRows = details
                .sort((a, b) => a.trade_num.localeCompare(b.trade_num, undefined, { numeric: true, sensitivity: 'base' }));

            setRows(exportRows);
            setSelectedTrades(prev => {
                const next: Record<string, boolean> = {};
                for (const row of exportRows) {
                    if (prev[row.trade_num]) next[row.trade_num] = true;
                }
                return next;
            });
            setExpandedTrades(prev => {
                const next: Record<string, boolean> = {};
                for (const row of exportRows) {
                    if (prev[row.trade_num]) next[row.trade_num] = true;
                }
                return next;
            });
        } catch (e: unknown) {
            setRows([]);
            setSelectedTrades({});
            setExpandedTrades({});
            show(`Failed to load export trades: ${String(e)}`, 'error');
        } finally {
            if (isRefresh) setRefreshing(false);
            else setLoading(false);
        }
    }, [show]);

    useEffect(() => { void load(false); }, [load]);

    const tradeSearchNorm = normalizeTradeNumberValue(tradeSearch);
    const filteredRows = tradeSearchNorm
        ? rows.filter(row => normalizeTradeNumberValue(row.trade_num).includes(tradeSearchNorm))
        : rows;

    const selected = rows.filter(row => selectedTrades[row.trade_num]);
    const selectedCount = selected.length;

    const setAllSelected = (checked: boolean) => {
        if (!checked) {
            setSelectedTrades({});
            return;
        }
        setSelectedTrades(prev => {
            const next: Record<string, boolean> = { ...prev };
            for (const row of filteredRows) next[row.trade_num] = true;
            return next;
        });
    };

    const toggleSelected = (tradeNum: string, checked: boolean) => {
        setSelectedTrades(prev => ({ ...prev, [tradeNum]: checked }));
    };
    const toggleExpanded = (tradeNum: string) => {
        setExpandedTrades(prev => ({ ...prev, [tradeNum]: !prev[tradeNum] }));
    };

    const downloadSelected = async () => {
        if (selectedCount === 0 || downloading) return;
        setDownloading(true);
        const targetDir = 'T:\\Platform Doc Testing';
        try {
            const payload = {
                trades: selected.map(row => ({
                    trade_num: row.trade_num,
                    fnc_numbers: row.fnc_numbers,
                })),
                output_dir: targetDir,
            };
            const result = await api.saveExportTradesToFolder(payload);
            const completeTrades = Number((result as Row).complete_trades ?? 0) || 0;
            const partialTrades = Number((result as Row).partial_trades ?? 0) || 0;
            const failedTrades = Number((result as Row).failed_trades ?? 0) || 0;
            const savedFiles = Number((result as Row).saved_file_count ?? 0) || 0;
            const outputDir = String((result as Row).output_dir ?? targetDir);
            const errorsRaw = (result as Row).errors;
            const errors = Array.isArray(errorsRaw) ? errorsRaw.map(v => String(v)) : [];
            const warningsRaw = (result as Row).warnings;
            const warnings = Array.isArray(warningsRaw) ? warningsRaw.map(v => String(v)) : [];
            const firstError = errors.length > 0 ? errors[0] : '';
            const firstWarning = warnings.length > 0 ? warnings[0] : '';

            if (failedTrades === 0 && errors.length === 0) {
                show(
                    `Saved ${savedFiles.toLocaleString()} file(s) to ${outputDir}. `
                    + `Complete trades: ${completeTrades.toLocaleString()}.`
                    + (firstWarning ? ` Note: ${firstWarning}` : ''),
                    'success'
                );
            } else {
                show(
                    `Saved ${savedFiles.toLocaleString()} file(s) to ${outputDir}. `
                    + `Complete: ${completeTrades.toLocaleString()}, Partial: ${partialTrades.toLocaleString()}, Failed: ${failedTrades.toLocaleString()}.`
                    + (firstError ? ` First error: ${firstError}` : ''),
                    'error'
                );
            }
        } catch (e: unknown) {
            show(
                `Export to ${targetDir} failed: ${String(e)}`,
                'error'
            );
        } finally {
            setDownloading(false);
        }
    };

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Export Trades</h2>
                    <div className="page-subtitle">
                        {rows.length.toLocaleString()} allocated trade(s) with at least one tagged PMX row
                    </div>
                    <div className="page-subtitle">
                        Showing {filteredRows.length.toLocaleString()} trade(s)
                    </div>
                    <div className="page-subtitle">Save path: T:\Platform Doc Testing</div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(true); }} disabled={loading || refreshing}>
                        {refreshing ? 'Refreshing...' : 'Refresh'}
                    </button>
                    <button className="btn btn-sm" onClick={() => setAllSelected(true)} disabled={filteredRows.length === 0}>
                        Select All
                    </button>
                    <button className="btn btn-sm" onClick={() => setAllSelected(false)} disabled={selectedCount === 0}>
                        Clear
                    </button>
                    <button className="btn btn-sm btn-primary" onClick={downloadSelected} disabled={selectedCount === 0 || downloading}>
                        {downloading ? 'Saving...' : `Save Selected (${selectedCount})`}
                    </button>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Trade #</label>
                    <input
                        placeholder="Search trade #"
                        value={tradeSearch}
                        onChange={e => setTradeSearch(e.target.value)}
                    />
                </div>
            </div>

            {loading ? (
                <Loading text="Loading allocated tagged trades..." />
            ) : rows.length === 0 ? (
                <Empty title="No tagged trades" sub="Trades appear here when at least one PMX row has a Trade # value." />
            ) : filteredRows.length === 0 ? (
                <Empty title="No matching trades" sub={`No tagged trades match "${tradeSearch.trim()}".`} />
            ) : (
                <div className="table-container">
                    <table className="data-table">
                        <thead>
                            <tr>
                                <th>Trade #</th>
                                <th>Weight Traded (g)</th>
                                <th>Gold WA $/oz</th>
                                <th>FX WA USD/ZAR</th>
                                <th>FNC Count</th>
                                <th>Details</th>
                            </tr>
                        </thead>
                        <tbody>
                            {filteredRows.map((row) => {
                                const expanded = !!expandedTrades[row.trade_num];
                                return (
                                    <Fragment key={row.trade_num}>
                                        <tr>
                                            <td>
                                                <label style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
                                                    <input
                                                        type="checkbox"
                                                        checked={!!selectedTrades[row.trade_num]}
                                                        onChange={e => toggleSelected(row.trade_num, e.target.checked)}
                                                    />
                                                    <span>{row.trade_num}</span>
                                                </label>
                                            </td>
                                            <td className="num">{fmt(row.tm_weight_g, 2)}</td>
                                            <td className="num">{fmt(row.xau_usd_wa, 4)}</td>
                                            <td className="num">{fmt(row.fx_wa, 4)}</td>
                                            <td className="num" title={row.fnc_numbers.join(', ') || 'No FNC'}>
                                                {row.fnc_count.toLocaleString()}
                                            </td>
                                            <td>
                                                <button className="btn btn-sm" onClick={() => toggleExpanded(row.trade_num)}>
                                                    {expanded ? 'Collapse' : 'Expand'}
                                                </button>
                                            </td>
                                        </tr>
                                        {expanded && (
                                            <tr>
                                                <td colSpan={6} style={{ padding: '0.75rem' }}>
                                                    {row.ledger_rows.length === 0 ? (
                                                        <Empty title="No PMX rows for this trade" />
                                                    ) : (
                                                        <DataTable
                                                            columns={[
                                                                { key: 'Trade Date', label: 'Trade Date' },
                                                                { key: 'Value Date', label: 'Value Date' },
                                                                { key: 'Symbol', label: 'Symbol' },
                                                                { key: 'Side', label: 'Side' },
                                                                { key: 'Narration', label: 'Narration' },
                                                                { key: 'Quantity', label: 'Quantity' },
                                                                { key: 'Price', label: 'Price' },
                                                                { key: 'FNC #', label: 'FNC #' },
                                                            ]}
                                                            data={row.ledger_rows}
                                                            numericCols={['Quantity', 'Price']}
                                                            dateCols={['Trade Date', 'Value Date']}
                                                            formatters={{
                                                                Quantity: { decimals: 2 },
                                                                Price: { decimals: 4 },
                                                            }}
                                                        />
                                                    )}
                                                </td>
                                            </tr>
                                        )}
                                    </Fragment>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
            {Toast}
        </div>
    );
}

function UserManagement({ currentUserId }: { currentUserId: number | null }) {
    type UserFormState = {
        username: string;
        display_name: string;
        password: string;
        role: string;
        can_read: boolean;
        can_write: boolean;
        is_admin: boolean;
        is_active: boolean;
    };

    const { show, Toast } = useToast();
    const [users, setUsers] = useState<AdminUser[]>([]);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [creating, setCreating] = useState(false);
    const [savingId, setSavingId] = useState<number | null>(null);
    const [deletingId, setDeletingId] = useState<number | null>(null);
    const [editingId, setEditingId] = useState<number | null>(null);
    const [editDraft, setEditDraft] = useState<UserFormState | null>(null);
    const [createDraft, setCreateDraft] = useState<UserFormState>({
        username: '',
        display_name: '',
        password: '',
        role: 'viewer',
        can_read: true,
        can_write: false,
        is_admin: false,
        is_active: true,
    });

    const applyRules = (draft: UserFormState): UserFormState => {
        const next = { ...draft };
        if (next.is_admin) {
            next.can_read = true;
            next.can_write = true;
        }
        if (next.can_write) next.can_read = true;
        return next;
    };

    const sortUsers = (rows: AdminUser[]): AdminUser[] =>
        [...rows].sort((a, b) => String(a.username || '').localeCompare(String(b.username || ''), undefined, { sensitivity: 'base' }));

    const loadUsers = useCallback(async (silent = false) => {
        if (silent) setRefreshing(true);
        else setLoading(true);
        try {
            const res = await api.authUsers();
            setUsers(sortUsers((res.users || []) as AdminUser[]));
        } catch (e: unknown) {
            show(`Failed to load users: ${String(e)}`, 'error');
            if (!silent) setUsers([]);
        } finally {
            if (silent) setRefreshing(false);
            else setLoading(false);
        }
    }, [show]);

    useEffect(() => {
        void loadUsers(false);
    }, [loadUsers]);

    const onCreate = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        const draft = applyRules(createDraft);
        const username = draft.username.trim();
        const password = draft.password.trim();
        if (!username) {
            show('Username is required.', 'error');
            return;
        }
        if (!password) {
            show('Password is required.', 'error');
            return;
        }
        setCreating(true);
        try {
            await api.authCreateUser({
                username,
                password,
                display_name: draft.display_name.trim() || username,
                role: draft.role || 'viewer',
                can_read: draft.can_read,
                can_write: draft.can_write,
                is_admin: draft.is_admin,
                is_active: draft.is_active,
            });
            setCreateDraft({
                username: '',
                display_name: '',
                password: '',
                role: 'viewer',
                can_read: true,
                can_write: false,
                is_admin: false,
                is_active: true,
            });
            await loadUsers(true);
            show(`User ${username} created.`, 'success');
        } catch (e: unknown) {
            show(`Create failed: ${String(e)}`, 'error');
        } finally {
            setCreating(false);
        }
    };

    const startEdit = (user: AdminUser) => {
        setEditingId(Number(user.id));
        setEditDraft({
            username: String(user.username || ''),
            display_name: String(user.display_name || ''),
            password: '',
            role: String(user.role || 'viewer'),
            can_read: !!user.can_read,
            can_write: !!user.can_write,
            is_admin: !!user.is_admin,
            is_active: !!user.is_active,
        });
    };

    const cancelEdit = () => {
        setEditingId(null);
        setEditDraft(null);
    };

    const saveEdit = async (userId: number) => {
        if (!editDraft || editingId !== userId) return;
        const draft = applyRules(editDraft);
        const username = draft.username.trim();
        if (!username) {
            show('Username is required.', 'error');
            return;
        }
        setSavingId(userId);
        try {
            const payload: {
                username: string;
                display_name: string;
                role: string;
                can_read: boolean;
                can_write: boolean;
                is_admin: boolean;
                is_active: boolean;
                password?: string;
            } = {
                username,
                display_name: draft.display_name.trim() || username,
                role: draft.role || 'viewer',
                can_read: draft.can_read,
                can_write: draft.can_write,
                is_admin: draft.is_admin,
                is_active: draft.is_active,
            };
            if (draft.password.trim()) payload.password = draft.password.trim();
            const res = await api.authUpdateUser(userId, payload);
            const updated = res.user as AdminUser;
            setUsers(prev => sortUsers(prev.map(u => (Number(u.id) === userId ? updated : u))));
            cancelEdit();
            show(`User ${updated.username} updated.`, 'success');
        } catch (e: unknown) {
            show(`Update failed: ${String(e)}`, 'error');
        } finally {
            setSavingId(null);
        }
    };

    const deleteUser = async (user: AdminUser) => {
        const userId = Number(user.id);
        if (!Number.isFinite(userId) || userId <= 0) {
            show('Invalid user id.', 'error');
            return;
        }
        if (currentUserId !== null && userId === currentUserId) {
            show('You cannot delete your own account.', 'error');
            return;
        }
        const display = String(user.display_name || user.username || userId);
        if (!window.confirm(`Delete user "${display}"? This cannot be undone.`)) return;
        setDeletingId(userId);
        try {
            await api.authDeleteUser(userId);
            setUsers(prev => prev.filter(u => Number(u.id) !== userId));
            if (editingId === userId) cancelEdit();
            show(`User ${display} deleted.`, 'success');
        } catch (e: unknown) {
            show(`Delete failed: ${String(e)}`, 'error');
        } finally {
            setDeletingId(null);
        }
    };

    return (
        <div>
            <div className="page-header">
                <h2>User Management</h2>
                <button className="btn btn-sm" onClick={() => { void loadUsers(true); }} disabled={loading || refreshing}>
                    {refreshing ? 'Refreshing...' : 'Refresh'}
                </button>
            </div>

            <div className="section">
                <div className="section-title">Create User</div>
                <form onSubmit={onCreate}>
                    <div className="filter-bar">
                        <div className="filter-group">
                            <label>Username</label>
                            <input
                                value={createDraft.username}
                                onChange={e => setCreateDraft(prev => ({ ...prev, username: e.target.value }))}
                                placeholder="e.g. j.kress"
                            />
                        </div>
                        <div className="filter-group">
                            <label>Display Name</label>
                            <input
                                value={createDraft.display_name}
                                onChange={e => setCreateDraft(prev => ({ ...prev, display_name: e.target.value }))}
                                placeholder="e.g. Joshua Kress"
                            />
                        </div>
                        <div className="filter-group">
                            <label>Password</label>
                            <input
                                type="password"
                                value={createDraft.password}
                                onChange={e => setCreateDraft(prev => ({ ...prev, password: e.target.value }))}
                                placeholder="Password"
                            />
                        </div>
                        <div className="filter-group">
                            <label>Role</label>
                            <select
                                value={createDraft.role}
                                onChange={e => setCreateDraft(prev => ({ ...prev, role: e.target.value }))}
                            >
                                <option value="viewer">viewer</option>
                                <option value="admin">admin</option>
                            </select>
                        </div>
                    </div>
                    <div className="filter-bar">
                        <label style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={createDraft.can_read}
                                onChange={e => setCreateDraft(prev => applyRules({ ...prev, can_read: e.target.checked }))}
                            />
                            Read
                        </label>
                        <label style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={createDraft.can_write}
                                onChange={e => setCreateDraft(prev => applyRules({ ...prev, can_write: e.target.checked }))}
                            />
                            Write
                        </label>
                        <label style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={createDraft.is_admin}
                                onChange={e => setCreateDraft(prev => applyRules({ ...prev, is_admin: e.target.checked }))}
                            />
                            Admin
                        </label>
                        <label style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={createDraft.is_active}
                                onChange={e => setCreateDraft(prev => ({ ...prev, is_active: e.target.checked }))}
                            />
                            Active
                        </label>
                        <button className="btn btn-sm btn-primary" type="submit" disabled={creating}>
                            {creating ? 'Creating...' : 'Create User'}
                        </button>
                    </div>
                </form>
            </div>

            <div className="section mt-3">
                <div className="section-title">Users</div>
                {loading ? (
                    <Loading text="Loading users..." />
                ) : (
                    <div className="table-container">
                        <table className="data-table">
                            <thead>
                                <tr>
                                    <th>Username</th>
                                    <th>Display Name</th>
                                    <th>Role</th>
                                    <th>Read</th>
                                    <th>Write</th>
                                    <th>Admin</th>
                                    <th>Active</th>
                                    <th>Created</th>
                                    <th>Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {users.length === 0 && (
                                    <tr>
                                        <td colSpan={9} style={{ textAlign: 'left', padding: '1.5rem', color: 'var(--text-muted)' }}>
                                            No users found.
                                        </td>
                                    </tr>
                                )}
                                {users.map(user => {
                                    const isEditing = editingId === Number(user.id) && editDraft !== null;
                                    const isCurrentUser = currentUserId !== null && Number(user.id) === currentUserId;
                                    return (
                                        <tr key={String(user.id)}>
                                            <td>
                                                {isEditing ? (
                                                    <input
                                                        value={editDraft.username}
                                                        onChange={e => setEditDraft(prev => (prev ? { ...prev, username: e.target.value } : prev))}
                                                    />
                                                ) : (
                                                    user.username
                                                )}
                                            </td>
                                            <td>
                                                {isEditing ? (
                                                    <input
                                                        value={editDraft.display_name}
                                                        onChange={e => setEditDraft(prev => (prev ? { ...prev, display_name: e.target.value } : prev))}
                                                    />
                                                ) : (
                                                    user.display_name
                                                )}
                                            </td>
                                            <td>
                                                {isEditing ? (
                                                    <select
                                                        value={editDraft.role}
                                                        onChange={e => setEditDraft(prev => (prev ? { ...prev, role: e.target.value } : prev))}
                                                    >
                                                        <option value="viewer">viewer</option>
                                                        <option value="admin">admin</option>
                                                    </select>
                                                ) : (
                                                    user.role
                                                )}
                                            </td>
                                            <td className="num">
                                                {isEditing ? (
                                                    <input
                                                        type="checkbox"
                                                        checked={editDraft.can_read}
                                                        onChange={e => setEditDraft(prev => (prev ? applyRules({ ...prev, can_read: e.target.checked }) : prev))}
                                                    />
                                                ) : (
                                                    user.can_read ? 'Yes' : 'No'
                                                )}
                                            </td>
                                            <td className="num">
                                                {isEditing ? (
                                                    <input
                                                        type="checkbox"
                                                        checked={editDraft.can_write}
                                                        onChange={e => setEditDraft(prev => (prev ? applyRules({ ...prev, can_write: e.target.checked }) : prev))}
                                                    />
                                                ) : (
                                                    user.can_write ? 'Yes' : 'No'
                                                )}
                                            </td>
                                            <td className="num">
                                                {isEditing ? (
                                                    <input
                                                        type="checkbox"
                                                        checked={editDraft.is_admin}
                                                        onChange={e => setEditDraft(prev => (prev ? applyRules({ ...prev, is_admin: e.target.checked }) : prev))}
                                                    />
                                                ) : (
                                                    user.is_admin ? 'Yes' : 'No'
                                                )}
                                            </td>
                                            <td className="num">
                                                {isEditing ? (
                                                    <input
                                                        type="checkbox"
                                                        checked={editDraft.is_active}
                                                        onChange={e => setEditDraft(prev => (prev ? { ...prev, is_active: e.target.checked } : prev))}
                                                    />
                                                ) : (
                                                    user.is_active ? 'Yes' : 'No'
                                                )}
                                            </td>
                                            <td>{fmtDateTime(user.created_at)}</td>
                                            <td>
                                                {isEditing ? (
                                                    <div style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                                                        <input
                                                            type="password"
                                                            value={editDraft.password}
                                                            onChange={e => setEditDraft(prev => (prev ? { ...prev, password: e.target.value } : prev))}
                                                            placeholder="New password (optional)"
                                                            style={{ minWidth: '180px' }}
                                                        />
                                                        <button
                                                            className="btn btn-sm btn-primary"
                                                            onClick={() => { void saveEdit(Number(user.id)); }}
                                                            disabled={savingId === Number(user.id) || deletingId === Number(user.id)}
                                                        >
                                                            {savingId === Number(user.id) ? 'Saving...' : 'Save'}
                                                        </button>
                                                        <button className="btn btn-sm" onClick={cancelEdit} disabled={savingId === Number(user.id) || deletingId === Number(user.id)}>
                                                            Cancel
                                                        </button>
                                                    </div>
                                                ) : (
                                                    <div style={{ display: 'inline-flex', gap: '0.4rem', alignItems: 'center' }}>
                                                        <button className="btn btn-sm" onClick={() => startEdit(user)} disabled={deletingId === Number(user.id)}>
                                                            Edit
                                                        </button>
                                                        <button
                                                            className="btn btn-sm btn-danger"
                                                            onClick={() => { void deleteUser(user); }}
                                                            disabled={deletingId === Number(user.id) || isCurrentUser}
                                                            title={isCurrentUser ? 'You cannot delete your own account.' : 'Delete user'}
                                                        >
                                                            {deletingId === Number(user.id) ? 'Deleting...' : 'Delete'}
                                                        </button>
                                                    </div>
                                                )}
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: XAU RECONCILIATION
// ===================================================================
function XAUReconciliation() {
    const today = new Date().toISOString().slice(0, 10);
    const [startDate, setStartDate] = usePersistentState('recon:start_date', '2026-03-02');
    const [endDate, setEndDate] = usePersistentState('recon:end_date', today);
    const [baselineXau, setBaselineXau] = usePersistentState('recon:baseline_xau', '');
    const [rows, setRows] = useState<Row[]>([]);
    const [summary, setSummary] = useState<Record<string, unknown>>({});
    const [loading, setLoading] = useState(false);
    const [initialLoad, setInitialLoad] = useState(true);
    const [loadError, setLoadError] = useState('');
    const { show, Toast } = useToast();

    const load = useCallback(async () => {
        setLoading(true);
        setLoadError('');
        try {
            const params: Record<string, string> = {
                start_date: startDate || '2026-03-02',
                end_date: endDate || today,
                baseline_date: '2026-03-01',
            };
            const bl = parseFloat(baselineXau);
            if (baselineXau.trim() !== '' && Number.isFinite(bl)) params.baseline_xau = String(bl);
            const res = await api.getPmxReconciliation(params);
            if (!res.ok) throw new Error(String((res as unknown as Record<string, unknown>).error || 'Request failed'));
            setRows((res.rows || []) as Row[]);
            setSummary((res.summary || {}) as Record<string, unknown>);
        } catch (e: unknown) {
            const msg = String(e);
            setLoadError(msg);
            show(`Reconciliation load failed: ${msg}`, 'error');
        } finally {
            setLoading(false);
            setInitialLoad(false);
        }
    }, [startDate, endDate, baselineXau, show, today]);

    useEffect(() => {
        if (initialLoad) void load();
    }, [initialLoad, load]);

    const downloadCsv = async () => {
        const params: Record<string, string> = {
            start_date: startDate || '2026-03-02',
            end_date: endDate || today,
        };
        const bl = parseFloat(baselineXau);
        if (baselineXau.trim() !== '' && Number.isFinite(bl)) params.baseline_xau = String(bl);
        try {
            const res = await api.getPmxLedgerFullCsv(params, readStoredPmxHeaders());
            if (!res.ok) {
                const errText = await res.text().catch(() => '');
                throw new Error(errText || `HTTP ${res.status}`);
            }
            const blob = await res.blob();
            const fallback = `pmx_xau_recon_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.csv`;
            const filename = parseFilenameFromDisposition(res.headers.get('content-disposition') || '', fallback);
            triggerBlobDownload(blob, filename);
        } catch (e: unknown) {
            show(`CSV download failed: ${String(e)}`, 'error');
        }
    };

    const blXau = toNullableNumber(summary.baseline_xau);
    const totalNet = toNullableNumber(summary.total_net_oz);
    const expectedXau = toNullableNumber(summary.expected_xau);
    const acctXau = toNullableNumber(summary.account_xau);
    const deltaAcct = toNullableNumber(summary.delta_to_account);
    const rowCount = Number(summary.row_count || 0);
    const deltaOk = deltaAcct !== null && Math.abs(deltaAcct) < 0.01;

    const RECON_COLS = [
        { key: 'doc_number', label: 'Doc #' },
        { key: 'row_type', label: 'Type' },
        { key: 'trade_number', label: 'Trade #' },
        { key: 'date', label: 'Date' },
        { key: 'symbol', label: 'Symbol' },
        { key: 'side', label: 'Side' },
        { key: 'net_oz', label: 'Net Oz' },
        { key: 'running_net_oz', label: 'Running XAU Movement' },
        { key: 'expected_xau', label: 'Expected XAU' },
        { key: 'narration', label: 'Narration' },
    ];

    const fmtReconNum = (val: unknown, decs = 4) => {
        const n = toNullableNumber(val);
        if (n === null) return '';
        return fmt(n, decs);
    };

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>XAU Reconciliation</h2>
                    <div className="page-subtitle">
                        PMX statement vs account balance � {rowCount} JRV/MER/FNC rows
                    </div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm btn-primary" onClick={() => { void load(); }} disabled={loading}>
                        {loading ? 'Loading...' : 'Refresh'}
                    </button>
                    <button className="btn btn-sm" onClick={() => { void downloadCsv(); }} disabled={rows.length === 0}>
                        Download CSV
                    </button>
                </div>
            </div>

            <div className="filter-bar">
                <div className="filter-group">
                    <label>Start Date</label>
                    <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} />
                </div>
                <div className="filter-group">
                    <label>End Date</label>
                    <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} />
                </div>
                <div className="filter-group">
                    <label>Baseline XAU Override</label>
                    <input
                        type="text"
                        value={baselineXau}
                        onChange={e => setBaselineXau(e.target.value)}
                        style={{ width: '130px' }}
                        placeholder="Auto (from stmt)"
                    />
                </div>
                <div className="filter-group">
                    <label>&nbsp;</label>
                    <button className="btn btn-sm btn-primary" onClick={() => { void load(); }} disabled={loading}>
                        {loading ? 'Loading...' : 'Load'}
                    </button>
                </div>
            </div>

            {loadError && (
                <div className="section" style={{ color: 'var(--danger)', marginBottom: '0.75rem' }}>
                    {loadError}
                </div>
            )}

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="stat-label">Opening Balance (XAU oz)</div>
                    <div className={`stat-value ${numClass(blXau).replace('num ', '')}`}>
                        {blXau !== null ? fmt(blXau, 4) : '--'}
                    </div>
                    <div className="stat-sub">As at {String(summary.baseline_date || '2026-03-01')}</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Net Oz Movement</div>
                    <div className={`stat-value ${numClass(totalNet).replace('num ', '')}`}>
                        {totalNet !== null ? fmt(totalNet, 4) : '--'}
                    </div>
                    <div className="stat-sub">Sum of JRV/MER/FNC net oz</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Expected Closing XAU</div>
                    <div className={`stat-value ${numClass(expectedXau).replace('num ', '')}`}>
                        {expectedXau !== null ? fmt(expectedXau, 4) : '--'}
                    </div>
                    <div className="stat-sub">Opening + Net Movement</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Account XAU (Live)</div>
                    <div className={`stat-value ${numClass(acctXau).replace('num ', '')}`}>
                        {acctXau !== null ? fmt(acctXau, 4) : '--'}
                    </div>
                    <div className="stat-sub">From Open Positions Reval</div>
                </div>
                <div className="stat-card">
                    <div className="stat-label">Delta (Expected - Account)</div>
                    <div className="stat-value" style={{ color: deltaAcct === null ? 'var(--text-muted)' : deltaOk ? 'var(--success)' : 'var(--danger)' }}>
                        {deltaAcct !== null ? fmt(deltaAcct, 4) : '--'}
                    </div>
                    <div className="stat-sub" style={{ color: deltaAcct === null ? 'var(--text-muted)' : deltaOk ? 'var(--success)' : 'var(--danger)' }}>
                        {deltaAcct === null ? '' : deltaOk ? '? Reconciled' : '? Variance'}
                    </div>
                </div>
            </div>

            {loading ? <Loading text="Fetching statement data..." /> : (
                <div className="section mt-3">
                    <div className="section-title">Statement Transactions</div>
                    <div className="table-container">
                        <table className="data-table">
                            <thead>
                                <tr>
                                    {RECON_COLS.map(c => <th key={c.key}>{c.label}</th>)}
                                </tr>
                            </thead>
                            <tbody>
                                {rows.length === 0 && (
                                    <tr><td colSpan={RECON_COLS.length} style={{ textAlign: 'left', padding: '2.5rem', color: 'var(--text-muted)' }}>No rows returned</td></tr>
                                )}
                                {rows.map((row, idx) => {
                                    return (
                                        <tr key={idx}>
                                            <td>{asText(row.doc_number)}</td>
                                            <td><span className={`badge badge-${String(row.row_type || '').toLowerCase()}`}>{asText(row.row_type)}</span></td>
                                            <td>{asText(row.trade_number)}</td>
                                            <td>{fmtDate(row.date)}</td>
                                            <td>{asText(row.symbol)}</td>
                                            <td>{asText(row.side)}</td>
                                            <td className="num">{fmtReconNum(row.net_oz)}</td>
                                            <td className="num">{fmtReconNum(row.running_net_oz)}</td>
                                            <td className="num">{fmtReconNum(row.expected_xau)}</td>
                                            <td style={{ maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={asText(row.narration)}>
                                                {asText(row.narration)}
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                </div>
            )}
            {Toast}
        </div>
    );
}

// ===================================================================
// TAB: ACCOUNT BALANCES
// ===================================================================
type ReconCurrency = {
    opening_balance: number;
    transaction_total: number | null;
    expected_balance: number | null;
    actual_balance: number | null;
    delta: number | null;
};

type ReconData = {
    start_date: string;
    end_date: string;
    month: string;
    currencies: Record<string, ReconCurrency>;
    actual_balances_ok: boolean;
    transactions_ok: boolean;
    error: string;
};

function EditableOpeningBalance({ value, month, currency, dp, onSaved, onError }: {
    value: number;
    month: string;
    currency: string;
    dp: number;
    onSaved: (newVal: number) => void;
    onError: (msg: string) => void;
}) {
    const [editing, setEditing] = useState(false);
    const [draft, setDraft] = useState(String(value));
    const [saving, setSaving] = useState(false);

    useEffect(() => { setDraft(fmt(value, dp)); }, [value, dp]);

    const save = async () => {
        const parsed = parseFloat(draft.replace(/,/g, ''));
        if (isNaN(parsed)) { setEditing(false); setDraft(fmt(value, dp)); return; }
        if (parsed === value) { setEditing(false); return; }
        setSaving(true);
        try {
            const res = await api.setOpeningBalance(month, currency, parsed);
            if (!(res as Record<string, unknown>).ok) {
                throw new Error(asText((res as Record<string, unknown>).error, 'Save failed'));
            }
            onSaved(parsed);
            setEditing(false);
        } catch (e: unknown) {
            onError(String(e));
            setDraft(fmt(value, dp));
            setEditing(false);
        }
        setSaving(false);
    };

    if (!editing) {
        return (
            <span
                className="editable-cell"
                onClick={() => { setDraft(String(value)); setEditing(true); }}
                title="Click to edit opening balance"
                style={{ display: 'inline-block', minWidth: '80px', padding: '2px 6px', cursor: 'pointer' }}
            >
                {fmt(value, dp)}
            </span>
        );
    }

    return (
        <input
            className="editable-input"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onBlur={save}
            onKeyDown={e => { if (e.key === 'Enter') void save(); if (e.key === 'Escape') { setDraft(fmt(value, dp)); setEditing(false); } }}
            autoFocus
            disabled={saving}
            style={{ width: '110px' }}
        />
    );
}

function AccountBalances() {
    const [balances, setBalances] = useState<Record<string, unknown>>({});
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [loadError, setLoadError] = useState('');

    // Recon state
    const today = new Date();
    const defaultStartDate = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-01`;
    const defaultEndDate = today.toISOString().slice(0, 10);
    const [startDate, setStartDate] = usePersistentState('acct-recon:start_date', defaultStartDate);
    const [endDate, setEndDate] = usePersistentState('acct-recon:end_date', defaultEndDate);
    const [recon, setRecon] = useState<ReconData | null>(null);
    const [reconLoading, setReconLoading] = useState(false);
    const [reconError, setReconError] = useState('');
    // Local opening balance state (updated optimistically on save)
    const [openingOverrides, setOpeningOverrides] = useState<Record<string, number>>({});

    const { show, Toast } = useToast();

    const load = useCallback(async (isRefresh = false) => {
        if (isRefresh) setRefreshing(true);
        else setLoading(true);
        try {
            const res = await api.getAccountBalances();
            setBalances(res as unknown as Record<string, unknown>);
            setLoadError('');
        } catch (e: unknown) {
            const msg = String(e);
            setBalances({});
            setLoadError(msg);
            show(`Failed to load account balances: ${msg}`, 'error');
        } finally {
            if (isRefresh) setRefreshing(false);
            else setLoading(false);
        }
    }, [show]);

    const loadRecon = useCallback(async (sd: string, ed: string) => {
        setReconLoading(true);
        setReconError('');
        try {
            const res = await api.getAccountRecon({ start_date: sd, end_date: ed });
            setRecon(res as unknown as ReconData);
            setOpeningOverrides({});
        } catch (e: unknown) {
            setReconError(String(e));
        } finally {
            setReconLoading(false);
        }
    }, []);

    useEffect(() => { void load(false); }, [load]);
    useEffect(() => { void loadRecon(startDate, endDate); }, [loadRecon, startDate, endDate]);

    if (loading) return <><Loading text="Loading account balances..." />{Toast}</>;
    if (loadError) return <><Empty title="Could not load account balances" sub={loadError} />{Toast}</>;

    const xau = toNullableNumber(balances.xau);
    const xag = toNullableNumber(balances.xag);
    const usd = toNullableNumber(balances.usd);
    const zar = toNullableNumber(balances.zar);
    const asOf = asText(balances.as_of_date || balances.fetched_at, '');
    const accountCode = asText(balances.account_code, '');
    const balError = asText(balances.error, '');

    const reconMonth = recon?.month ?? startDate.slice(0, 7);

    const reconRows: { ccy: string; label: string; dp: number }[] = [
        { ccy: 'XAU', label: 'XAU (oz)', dp: 4 },
        { ccy: 'USD', label: 'USD', dp: 2 },
        { ccy: 'ZAR', label: 'ZAR', dp: 2 },
    ];

    return (
        <div>
            <div className="page-header">
                <div>
                    <h2>Account Balances</h2>
                    <div className="page-subtitle">
                        Live PMX account balances for XAU, XAG, USD and ZAR
                        {accountCode ? ` — Account ${accountCode}` : ''}
                    </div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(true); void loadRecon(startDate, endDate); }} disabled={refreshing}>
                        {refreshing ? 'Refreshing...' : 'Refresh'}
                    </button>
                </div>
            </div>

            {balError && (
                <div className="section" style={{ color: 'var(--danger)', marginBottom: '0.75rem' }}>
                    {balError}
                </div>
            )}

            <div className="section">
                <div className="section-title">Metal &amp; Currency Balances</div>
                {asOf && (
                    <div className="stat-sub" style={{ marginBottom: '0.75rem' }}>
                        As of {fmtDateTime(asOf)}
                    </div>
                )}
                <div className="stat-grid">
                    <div className="stat-card">
                        <div className="stat-label">XAU (Gold, oz)</div>
                        <div className={`stat-value ${numClass(xau).replace('num ', '')}`}>
                            {xau !== null ? fmt(xau, 4) : '--'}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">XAG (Silver, oz)</div>
                        <div className={`stat-value ${numClass(xag).replace('num ', '')}`}>
                            {xag !== null ? fmt(xag, 4) : '--'}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">USD</div>
                        <div className={`stat-value ${numClass(usd).replace('num ', '')}`}>
                            {usd !== null ? `$${fmt(usd, 2)}` : '--'}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">ZAR</div>
                        <div className={`stat-value ${numClass(zar).replace('num ', '')}`}>
                            {zar !== null ? `R${fmt(zar, 2)}` : '--'}
                        </div>
                    </div>
                </div>
            </div>

            <div className="section">
                <div className="section-title">Balance Reconciliation</div>
                <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', marginBottom: '1rem', flexWrap: 'wrap' }}>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontSize: '0.875rem' }}>
                        From
                        <input
                            type="date"
                            className="filter-input"
                            value={startDate}
                            onChange={e => setStartDate(e.target.value)}
                            style={{ padding: '3px 6px' }}
                        />
                    </label>
                    <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', fontSize: '0.875rem' }}>
                        To
                        <input
                            type="date"
                            className="filter-input"
                            value={endDate}
                            onChange={e => setEndDate(e.target.value)}
                            style={{ padding: '3px 6px' }}
                        />
                    </label>
                    {reconLoading && <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>Loading...</span>}
                </div>

                {reconError && (
                    <div style={{ color: 'var(--danger)', marginBottom: '0.75rem', fontSize: '0.875rem' }}>
                        {reconError}
                    </div>
                )}
                {recon?.error && (
                    <div style={{ color: 'var(--warning, orange)', marginBottom: '0.75rem', fontSize: '0.875rem' }}>
                        {recon.error}
                    </div>
                )}

                <div className="table-wrapper">
                    <table className="data-table">
                        <thead>
                            <tr>
                                <th>Currency</th>
                                <th className="num">Opening Balance</th>
                                <th className="num">Transactions</th>
                                <th className="num">Expected Balance</th>
                                <th className="num">Actual Balance</th>
                                <th className="num">Delta</th>
                            </tr>
                        </thead>
                        <tbody>
                            {reconRows.map(({ ccy, label, dp }) => {
                                const ccyData = recon?.currencies?.[ccy];
                                const openingVal = openingOverrides[ccy] ?? ccyData?.opening_balance ?? 0;
                                const txTotal = ccyData?.transaction_total ?? null;
                                const expectedBal = ccyData?.expected_balance ?? null;
                                const actualBal = ccyData?.actual_balance ?? null;
                                const delta = ccyData?.delta ?? null;

                                const deltaThreshold = ccy === 'XAU' ? 0.0001 : 0.01;
                                const deltaClass = delta === null ? '' : (Math.abs(delta) < deltaThreshold ? 'positive' : 'negative');

                                return (
                                    <tr key={ccy}>
                                        <td><strong>{label}</strong></td>
                                        <td className="num">
                                            <EditableOpeningBalance
                                                value={openingVal}
                                                month={reconMonth}
                                                currency={ccy}
                                                dp={dp}
                                                onSaved={newVal => {
                                                    setOpeningOverrides(prev => ({ ...prev, [ccy]: newVal }));
                                                    void loadRecon(startDate, endDate);
                                                }}
                                                onError={msg => show(msg, 'error')}
                                            />
                                        </td>
                                        <td className={`num ${numClass(txTotal).replace('num ', '')}`}>
                                            {txTotal !== null ? fmt(txTotal, dp) : '--'}
                                        </td>
                                        <td className={`num ${numClass(expectedBal).replace('num ', '')}`}>
                                            {expectedBal !== null ? fmt(expectedBal, dp) : '--'}
                                        </td>
                                        <td className={`num ${numClass(actualBal).replace('num ', '')}`}>
                                            {actualBal !== null ? fmt(actualBal, dp) : '--'}
                                        </td>
                                        <td className={`num ${deltaClass}`}>
                                            {delta !== null ? fmt(delta, dp) : '--'}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.5rem' }}>
                    Opening balance is editable — click any value to change it. Delta = Actual − Expected.
                    {recon && ` Statement period: ${recon.start_date} → ${recon.end_date}.`}
                </div>
            </div>
            {Toast}
        </div>
    );
}

// ===================================================================
// DASHBOARD
// ===================================================================
function Dashboard() {
    const [loading, setLoading] = useState(true);
    const [balances, setBalances] = useState<Row>({});
    const [openPos, setOpenPos] = useState<{ summary: Row; market: Row }>({ summary: {}, market: {} });
    const [profitSummary, setProfitSummary] = useState<Row>({});
    const [profitMonths, setProfitMonths] = useState<Row[]>([]);
    const [hedgingRows, setHedgingRows] = useState<Row[]>([]);
    const [hedgePeriod, setHedgePeriod] = usePersistentState<string>('dash_hedge_period', '30');
    const [profitPeriod, setProfitPeriod] = usePersistentState<string>('dash_profit_period', '30');
    const { show, Toast } = useToast();

    const load = useCallback(async () => {
        setLoading(true);
        try {
            const [bal, opr, profit, hedging] = await Promise.all([
                api.getAccountBalances().catch(() => ({} as Row)),
                api.getPmxOpenPositionsReval().catch(() => ({ rows: [], summary: {}, market: {} })),
                api.getProfitMonthly().catch(() => ({ months: [] as Row[], summary: {} as Row })),
                api.getHedging().catch(() => [] as Row[]),
            ]);
            setBalances(bal as Row);
            setOpenPos({ summary: (opr as { summary: Row }).summary || {}, market: (opr as { market: Row }).market || {} });
            const pm = (profit as { months: Row[]; summary: Row });
            setProfitSummary(pm.summary || {});
            setProfitMonths(pm.months || []);
            setHedgingRows(hedging as Row[]);
        } catch (e) {
            show(String(e), 'error');
        } finally {
            setLoading(false);
        }
    }, [show]);

    useEffect(() => { void load(); }, [load]);

    if (loading) return <><Loading text="Loading dashboard..." />{Toast}</>;

    const n = (v: unknown) => typeof v === 'number' ? v : Number(v) || 0;
    const xau = n(balances.xau);
    const usd = n(balances.usd);
    const zar = n(balances.zar);

    const openTradeCount = n(openPos.summary.open_trades);
    const totalProfitZar = n(profitSummary.total_profit_zar);
    const hedgingTrades = hedgingRows.filter(r => normalizeTradeNumberValue(r.trade_num) !== '');
    const UNHEDGED_METAL_TOL_OZ = 1.0;                        // 1 oz tolerance
    const UNHEDGED_METAL_TOL_G = UNHEDGED_METAL_TOL_OZ * 31.1035; // ~31.1 g
    const unhedgedTradeNums = new Set<string>();
    for (const row of hedgingTrades) {
        const tradeNum = normalizeTradeNumberValue(row.trade_num);
        if (!tradeNum) continue;
        const tmG = n(row.tm_weight_g);
        const needG = n(row.hedge_need_g);
        // Under-hedged = hedge_need has same sign as tm position (and exceeds tolerance).
        // Over-hedged (opposite sign) is NOT flagged — the position is covered.
        const isUnderHedged =
            (tmG > 0 && needG > UNHEDGED_METAL_TOL_G) ||
            (tmG < 0 && needG < -UNHEDGED_METAL_TOL_G);
        if (isUnderHedged) {
            unhedgedTradeNums.add(tradeNum);
        }
    }
    // Remaining hedge: only count the shortfall on genuinely under-hedged trades.
    const remainingHedgeG = hedgingTrades.reduce((sum, r) => {
        const tmG = n(r.tm_weight_g);
        const needG = n(r.hedge_need_g);
        if (tmG > 0 && needG > UNHEDGED_METAL_TOL_G) return sum + needG;           // long, under-hedged
        if (tmG < 0 && needG < -UNHEDGED_METAL_TOL_G) return sum + Math.abs(needG); // short, under-hedged
        return sum; // hedged or over-hedged — no remaining need
    }, 0);
    const remainingHedgeOz = remainingHedgeG / 31.1035;

    const GRAMS_PER_TROY_OUNCE = 31.1035;
    const DAILY_TARGET_RATE = 0.0015;
    const DAILY_TARGET_RAND_PER_GRAM = 1.3;
    const DASHBOARD_PROFIT_MIN_DATE = '2026-03-01';

    // --- Daily dashboard chart data ---
    const dailyProfitMap: Record<string, {
        metalProfit: number;
        exchangeProfit: number;
        netProfit: number;
        tmBuyAbsZar: number;
        tmSellAbsZar: number;
        tmAbsWeightG: number;
    }> = {};
    for (const month of profitMonths) {
        const trades = Array.isArray((month as { trades?: Row[] }).trades) ? (month as { trades: Row[] }).trades : [];
        for (const t of trades) {
            const date = String(t.trade_date || '').slice(0, 10);
            if (!date) continue;

            if (!dailyProfitMap[date]) {
                dailyProfitMap[date] = { metalProfit: 0, exchangeProfit: 0, netProfit: 0, tmBuyAbsZar: 0, tmSellAbsZar: 0, tmAbsWeightG: 0 };
            }

            const metalProfit = n(t.metal_profit_zar);
            const exchangeProfit = n(t.exchange_profit_zar);
            const netProfit = n(t.total_profit_zar);
            const tmWeightG = n(t.client_weight_g);

            let tmBuyAbsZar = 0;
            let tmSellAbsZar = 0;
            let tmAbsWeightG = 0;
            const tmTx = Array.isArray((t as { trademc_transactions?: Row[] }).trademc_transactions)
                ? ((t as { trademc_transactions: Row[] }).trademc_transactions)
                : [];

            for (const tx of tmTx) {
                const txWeightG = n(tx['Weight (g)']);
                const txZarAbs = Math.abs(n(tx['ZAR Value']));
                if (txZarAbs <= 1e-12) continue;
                tmAbsWeightG += Math.abs(txWeightG);
                if (txWeightG >= 0) tmBuyAbsZar += txZarAbs;
                else tmSellAbsZar += txZarAbs;
            }

            if (tmBuyAbsZar <= 1e-12 && tmSellAbsZar <= 1e-12) {
                const buySideAbs = Math.abs(n(t.buy_side_zar));
                const sellSideAbs = Math.abs(n(t.sell_side_zar));
                if (tmWeightG >= 0) tmBuyAbsZar = buySideAbs;
                else tmSellAbsZar = sellSideAbs;
            }
            if (tmAbsWeightG <= 1e-12) {
                tmAbsWeightG = Math.abs(tmWeightG);
            }

            dailyProfitMap[date].metalProfit += metalProfit;
            dailyProfitMap[date].exchangeProfit += exchangeProfit;
            dailyProfitMap[date].netProfit += netProfit;
            dailyProfitMap[date].tmBuyAbsZar += tmBuyAbsZar;
            dailyProfitMap[date].tmSellAbsZar += tmSellAbsZar;
            dailyProfitMap[date].tmAbsWeightG += tmAbsWeightG;
        }
    }

    const dailyData = Object.entries(dailyProfitMap)
        .filter(([date]) => String(date || '') >= DASHBOARD_PROFIT_MIN_DATE)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, values]) => {
            const targetBaseAbsZar = values.tmBuyAbsZar + values.tmSellAbsZar;
            const targetBaseAbsG = values.tmAbsWeightG;
            const dailyTarget = targetBaseAbsZar * DAILY_TARGET_RATE;
            const netProfitPerGram = targetBaseAbsG > 1e-9 ? values.netProfit / targetBaseAbsG : 0;
            return {
                date: date.slice(5), // MM-DD for compact x-axis labels
                fullDate: date,
                metalProfit: values.metalProfit,
                exchangeProfit: values.exchangeProfit,
                netProfit: values.netProfit,
                tmBuyAbsZar: values.tmBuyAbsZar,
                tmSellAbsZar: values.tmSellAbsZar,
                targetBaseAbsZar,
                targetBaseAbsG,
                dailyTarget,
                dailyTargetPerGram: DAILY_TARGET_RAND_PER_GRAM,
                targetDelta: values.netProfit - dailyTarget,
                hitTarget: dailyTarget > 0 ? values.netProfit >= dailyTarget : false,
                netProfitPerGram,
                targetDeltaPerGram: netProfitPerGram - DAILY_TARGET_RAND_PER_GRAM,
                hitTargetPerGram: netProfitPerGram >= DAILY_TARGET_RAND_PER_GRAM,
            };
        });

    const profitChartData = (() => {
        if (profitPeriod === 'all') return dailyData;
        const days = parseInt(profitPeriod, 10) || 30;
        const cutoff = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
        return dailyData.filter(
            row => String(row.fullDate || '') >= DASHBOARD_PROFIT_MIN_DATE && String(row.fullDate || '') >= cutoff
        );
    })();

    const totalNetProfitDashboard = profitChartData.reduce((sum, row) => sum + row.netProfit, 0);
    const totalTargetZar = profitChartData.reduce((sum, row) => sum + row.dailyTarget, 0);
    const totalTargetGapZar = totalNetProfitDashboard - totalTargetZar;
    const totalAbsWeightDashboardG = profitChartData.reduce((sum, row) => sum + n((row as Row).targetBaseAbsG), 0);
    const weightedNetProfitPerGram = totalAbsWeightDashboardG > 1e-9 ? (totalNetProfitDashboard / totalAbsWeightDashboardG) : 0;
    const totalTargetGapPerGram = weightedNetProfitPerGram - DAILY_TARGET_RAND_PER_GRAM;
    const totalPerGramTargetZar = DAILY_TARGET_RAND_PER_GRAM * totalAbsWeightDashboardG;
    const totalPerGramGapZar = totalNetProfitDashboard - totalPerGramTargetZar;

    // --- Build trade_num → date lookup from profit data ---
    const tradeDateLookup = (() => {
        const lookup = new Map<string, string>();
        for (const month of profitMonths) {
            const trades = Array.isArray((month as { trades?: Row[] }).trades) ? (month as { trades: Row[] }).trades : [];
            for (const t of trades) {
                const tradeNum = normalizeTradeNumberValue(t.trade_num);
                const date = String(t.trade_date || '').slice(0, 10);
                if (tradeNum && date) lookup.set(tradeNum, date);
            }
        }
        return lookup;
    })();

    // --- Daily Metal Hedge chart data (aggregated by date, filtered by period) ---
    const hedgeChartData = (() => {
        // Determine cutoff date from hedgePeriod
        const now = new Date();
        let cutoffDate = '';
        if (hedgePeriod !== 'all') {
            const days = parseInt(hedgePeriod, 10) || 30;
            const cutoff = new Date(now.getTime() - days * 86400000);
            cutoffDate = cutoff.toISOString().slice(0, 10);
        }

        const makeBucket = () => ({ longTotal: 0, longHedge: 0, shortTotal: 0, shortHedge: 0, tmOz: 0, hedgeOz: 0, tradeCount: 0 });
        const dailyMap = new Map<string, ReturnType<typeof makeBucket>>();
        const tradeNumsByDate = new Map<string, Set<string>>();
        const registerTrade = (dateKey: string, tradeNumRaw: unknown) => {
            const tradeNum = normalizeTradeNumberValue(tradeNumRaw);
            if (!tradeNum) return;
            const set = tradeNumsByDate.get(dateKey) || new Set<string>();
            set.add(tradeNum);
            tradeNumsByDate.set(dateKey, set);
        };
        const resolveDateKey = (rawDate: string) => {
            if (cutoffDate && rawDate && rawDate < cutoffDate) return '';
            return rawDate || 'No Date';
        };

        const hedgingByTrade = new Map<string, { tmOz: number; sellOz: number; buyOz: number; tradeDate: string }>();
        for (const row of hedgingRows) {
            const tradeNum = normalizeTradeNumberValue(row.trade_num);
            if (!tradeNum) continue;
            const tmOz = n(row.tm_weight_oz);
            const sellOz = Math.abs(n(row.stonex_sell_oz));
            const buyOz = Math.abs(n(row.stonex_buy_oz));
            const tradeDate = String(row.trade_date || '').slice(0, 10) || tradeDateLookup.get(tradeNum) || '';
            hedgingByTrade.set(tradeNum, { tmOz, sellOz, buyOz, tradeDate });
        }

        // Preferred path: transaction-level dates from profit report so buys/sells land on exact days.
        for (const month of profitMonths) {
            const trades = Array.isArray((month as { trades?: Row[] }).trades) ? (month as { trades: Row[] }).trades : [];
            for (const t of trades) {
                const tradeNum = normalizeTradeNumberValue(t.trade_num);
                if (!tradeNum) continue;

                // Keep dashboard hedge chart aligned to /api/hedging universe (including fiscal cutoff).
                const hedgeBase = hedgingByTrade.get(tradeNum);
                if (!hedgeBase) continue;

                const fallbackTradeDate = String(t.trade_date || '').slice(0, 10) || hedgeBase.tradeDate || tradeDateLookup.get(tradeNum) || '';

                const tmTx = Array.isArray((t as { trademc_transactions?: Row[] }).trademc_transactions)
                    ? ((t as { trademc_transactions: Row[] }).trademc_transactions)
                    : [];
                for (const tx of tmTx) {
                    const tmWeightG = n(tx['Weight (g)']);
                    if (Math.abs(tmWeightG) <= 0.001) continue;
                    const tmOz = tmWeightG / GRAMS_PER_TROY_OUNCE;
                    const rawDate = String((tx as { Date?: unknown }).Date || '').slice(0, 10) || fallbackTradeDate;
                    const dateKey = resolveDateKey(rawDate);
                    if (!dateKey) continue;

                    const existing = dailyMap.get(dateKey) || makeBucket();
                    if (tmOz > 0) existing.longTotal += tmOz;
                    else existing.shortTotal += tmOz;
                    existing.tmOz += tmOz;
                    dailyMap.set(dateKey, existing);
                    registerTrade(dateKey, tradeNum);
                }

                // Distribute hedge volume proportionally across TM transaction dates.
                // PMX transactions may land on different dates than TM transactions,
                // so placing hedges on PMX dates would make hedged trades look unhedged
                // on TM days and show orphan hedge on PMX days (which gets capped to 0).
                // Instead, use the trade-level effective hedge and spread it across the
                // same date buckets where TM volume was placed.
                const tmNetOz = hedgeBase.tmOz;
                const isLongTrade = tmNetOz > 0.001;
                const isShortTrade = tmNetOz < -0.001;
                const hedgeCapOz = Math.abs(tmNetOz);
                if (hedgeCapOz <= 0.001 || (!isLongTrade && !isShortTrade)) continue;

                // Effective hedge for this trade = min(matching PMX side, TM volume)
                const effectiveHedgeOz = isLongTrade
                    ? Math.min(hedgeBase.sellOz, hedgeCapOz)
                    : Math.min(hedgeBase.buyOz, hedgeCapOz);

                if (effectiveHedgeOz <= 1e-9) continue;

                // Collect the TM transaction date buckets for this trade so we can
                // distribute the hedge proportionally by volume on each date.
                const tmDateBuckets: { dateKey: string; absOz: number }[] = [];
                let totalTmAbsOz = 0;
                for (const tx of tmTx) {
                    const tmWeightG = n(tx['Weight (g)']);
                    if (Math.abs(tmWeightG) <= 0.001) continue;
                    const absOz = Math.abs(tmWeightG) / GRAMS_PER_TROY_OUNCE;
                    const rawDate = String((tx as { Date?: unknown }).Date || '').slice(0, 10) || fallbackTradeDate;
                    const dateKey = resolveDateKey(rawDate);
                    if (!dateKey) continue;
                    tmDateBuckets.push({ dateKey, absOz });
                    totalTmAbsOz += absOz;
                }

                // Fallback: if no TM transactions had usable dates, place on trade date.
                if (tmDateBuckets.length === 0) {
                    const dateKey = resolveDateKey(fallbackTradeDate);
                    if (dateKey) {
                        tmDateBuckets.push({ dateKey, absOz: hedgeCapOz });
                        totalTmAbsOz = hedgeCapOz;
                    }
                }

                // Distribute hedge pro-rata across TM dates
                if (totalTmAbsOz > 1e-9) {
                    for (const bucket of tmDateBuckets) {
                        const proportion = bucket.absOz / totalTmAbsOz;
                        const allocOz = effectiveHedgeOz * proportion;
                        if (allocOz <= 1e-9) continue;

                        const existing = dailyMap.get(bucket.dateKey) || makeBucket();
                        if (isLongTrade) existing.longHedge += allocOz;
                        else existing.shortHedge -= allocOz;
                        existing.hedgeOz += allocOz;
                        dailyMap.set(bucket.dateKey, existing);
                        registerTrade(bucket.dateKey, tradeNum);
                    }
                }
            }
        }

        // Fallback path: aggregated hedging rows if transaction-level data is unavailable.
        if (dailyMap.size === 0) {
            hedgingRows
                .filter(r => r.trade_num && Math.abs(n(r.tm_weight_oz)) > 0.001)
                .forEach(r => {
                    const tmOz = n(r.tm_weight_oz);
                    const isLong = tmOz > 0;
                    const sellOz = Math.abs(n(r.stonex_sell_oz));
                    const buyOz = Math.abs(n(r.stonex_buy_oz));
                    const hedgeOz = isLong ? Math.min(sellOz, tmOz) : Math.min(buyOz, Math.abs(tmOz));
                    const tradeNum = normalizeTradeNumberValue(r.trade_num);
                    const rawDate = String(r.trade_date || '').slice(0, 10) || tradeDateLookup.get(tradeNum) || '';
                    const dateKey = resolveDateKey(rawDate);
                    if (!dateKey) return;

                    const existing = dailyMap.get(dateKey) || makeBucket();
                    existing.longTotal += isLong ? tmOz : 0;
                    existing.longHedge += isLong ? hedgeOz : 0;
                    existing.shortTotal += !isLong ? tmOz : 0;
                    existing.shortHedge += !isLong ? -hedgeOz : 0;
                    existing.tmOz += tmOz;
                    existing.hedgeOz += hedgeOz;
                    existing.tradeCount += 1;
                    dailyMap.set(dateKey, existing);
                    registerTrade(dateKey, tradeNum);
                });
        }

        // Sort by date, put 'No Date' at the end
        return [...dailyMap.entries()]
            .sort(([a], [b]) => {
                if (a === 'No Date') return 1;
                if (b === 'No Date') return -1;
                return a.localeCompare(b);
            })
            .map(([dateKey, d]) => {
                const absLongTm = Math.abs(d.longTotal);
                const absShortTm = Math.abs(d.shortTotal);
                const cappedLongHedgeAbs = Math.min(Math.abs(d.longHedge), absLongTm);
                const cappedShortHedgeAbs = Math.min(Math.abs(d.shortHedge), absShortTm);
                const longHedge = cappedLongHedgeAbs;
                const shortHedge = -cappedShortHedgeAbs;
                const hedgeOz = cappedLongHedgeAbs + cappedShortHedgeAbs;
                const tmNetOz = d.longTotal + d.shortTotal;
                const hedgeNetOz = longHedge + shortHedge;
                const residualAbsOz = Math.abs(tmNetOz - hedgeNetOz);
                const requiredAbsOz = Math.abs(tmNetOz);
                // Coverage = hedged oz / total gross position oz for this day.
                const dayGrossOz = absLongTm + absShortTm;
                const rawCoveragePct = dayGrossOz > 0.001
                    ? Math.max(0, Math.min(100, (hedgeOz / dayGrossOz) * 100))
                    : 100;
                const dayTradeNums = tradeNumsByDate.get(dateKey) || new Set<string>();
                let hasUnhedgedTrade = false;
                for (const tn of dayTradeNums) {
                    if (unhedgedTradeNums.has(tn)) {
                        hasUnhedgedTrade = true;
                        break;
                    }
                }
                const coveragePct = hasUnhedgedTrade ? rawCoveragePct : 100;
                return {
                    dateLabel: dateKey === 'No Date' ? 'No Date' : fmtDate(dateKey),
                    fullDate: dateKey,
                    longTotal: d.longTotal,
                    longHedge,
                    shortTotal: d.shortTotal,
                    shortHedge,
                    tmOz: d.tmOz,
                    hedgeOz,
                    coveragePct,
                    hasUnhedgedTrade,
                    tradeCount: tradeNumsByDate.get(dateKey)?.size ?? d.tradeCount,
                };
            });
    })();

    const totalLongOz = hedgeChartData.reduce((s, r) => s + r.longTotal, 0);
    const totalShortOz = hedgeChartData.reduce((s, r) => s + Math.abs(r.shortTotal), 0);
    // Coverage must be calculated on the same net-hedge basis as "Remaining to Hedge".
    // Using gross long+short turnover understates coverage when there is offsetting flow.
    const totalRequiredHedgeOz = hedgingTrades.reduce((s, r) => s + Math.abs(n(r.tm_weight_oz)), 0);
    const totalHedgedOz = Math.max(0, totalRequiredHedgeOz - remainingHedgeOz);
    const overallCoveragePct = totalRequiredHedgeOz > 0.001
        ? Math.max(0, Math.min(100, (totalHedgedOz / totalRequiredHedgeOz) * 100))
        : 100;
    const hedgeAxisMax = Math.max(1, ...hedgeChartData.map(r => Math.max(Math.abs(r.longTotal), Math.abs(r.shortTotal))));
    const hedgeChartVisualData = hedgeChartData.map((row) => {
        const tmNetOz = row.longTotal + row.shortTotal;
        const hedgeNetOz = row.longHedge + row.shortHedge;
        const residualOz = tmNetOz - hedgeNetOz;
        const dayHedged = !Boolean((row as Row).hasUnhedgedTrade);
        const dominantHedgeBar = Math.abs(row.longHedge) >= Math.abs(row.shortHedge) ? row.longHedge : row.shortHedge;
        const statusDotY = Math.abs(dominantHedgeBar) > 0.001 ? (dominantHedgeBar / 2) : 0;
        return {
            ...row,
            hedgeNetOz,
            residualOz,
            dayHedged,
            longHedgeGood: dayHedged ? row.longHedge : 0,
            shortHedgeGood: dayHedged ? row.shortHedge : 0,
            longHedgeBad: dayHedged ? 0 : row.longHedge,
            shortHedgeBad: dayHedged ? 0 : row.shortHedge,
            statusDotY,
        };
    });

    const formatProfitChartDate = (value: unknown): string => {
        const raw = String(value || '').slice(0, 10);
        if (!raw) return '--';
        const dt = new Date(`${raw}T00:00:00`);
        if (Number.isNaN(dt.getTime())) return raw;
        return dt.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
    };

    const renderNetDot = (props: { cx?: number; cy?: number; payload?: { netProfit?: number; dailyTarget?: number } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return null;
        const netProfit = Number(payload?.netProfit ?? 0);
        const hurdle = Number(payload?.dailyTarget ?? 0);
        const color = netProfit >= hurdle ? '#10b981' : '#111111';
        return <circle cx={cx} cy={cy} r={5} fill={color} stroke="#000000" strokeWidth={1.5} />;
    };

    const renderHurdleBarShape = (props: { x?: number; y?: number; width?: number; height?: number }) => {
        const { x, y, width, height } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        const topY = height >= 0 ? y : y + height;
        const widerWidth = width * 1.5;
        const offsetX = x - ((widerWidth - width) / 2);
        return (
            <rect
                x={offsetX}
                y={topY}
                width={widerWidth}
                height={absHeight}
                fill="transparent"
                stroke="#6b7280"
                strokeWidth={1.5}
                strokeDasharray="4 3"
                rx={2}
                ry={2}
            />
        );
    };

    const renderProfitHurdleBarShape = (
        props: { x?: number; y?: number; width?: number; height?: number; payload?: { netProfit?: number; dailyTarget?: number } }
    ) => {
        const { x, y, width, height } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        const topY = height >= 0 ? y : y + height;
        const widerWidth = width * 1.5;
        const offsetX = x - ((widerWidth - width) / 2);
        return (
            <rect
                x={offsetX}
                y={topY}
                width={widerWidth}
                height={absHeight}
                fill="rgba(180,114,61,0.20)"
                stroke="#b4723d"
                strokeWidth={1.5}
                strokeDasharray="4 3"
                rx={2}
                ry={2}
            />
        );
    };

    const renderProfitAchievedBarShape = (
        props: { x?: number; y?: number; width?: number; height?: number; payload?: { netProfit?: number } }
    ) => {
        const { x, y, width, height, payload } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        if (absHeight <= 0.5) return null;
        const topY = height >= 0 ? y : y + height;
        const innerWidth = Math.max(3, width * 0.58);
        const innerX = x + ((width - innerWidth) / 2);
        const netProfit = Number(payload?.netProfit ?? 0);
        const fill = netProfit >= 0 ? "#10b981" : "#ef4444";
        return (
            <rect
                x={innerX}
                y={topY}
                width={innerWidth}
                height={absHeight}
                fill={fill}
                stroke={fill}
                strokeWidth={0}
                rx={1.5}
                ry={1.5}
            />
        );
    };

    const renderProfitTooltip = (props: any) => {
        const active = Boolean(props?.active);
        const payload = Array.isArray(props?.payload) ? props.payload : [];
        if (!active || payload.length === 0) return null;
        const row = (payload[0]?.payload || {}) as Row;
        const fullDate = String(row.fullDate || '--');
        const netProfit = n(row.netProfit);
        const dailyTarget = n(row.dailyTarget);
        return (
            <div className="dashboard-tooltip">
                <div className="dashboard-tooltip-title">{formatProfitChartDate(fullDate)}</div>
                <div className="dashboard-tooltip-row"><span>Net Profit</span><strong>R{fmt(netProfit, 2)}</strong></div>
                <div className="dashboard-tooltip-row"><span>Hurdle (0.15%)</span><strong>R{fmt(dailyTarget, 2)}</strong></div>
            </div>
        );
    };

    const renderNetPerGramDot = (props: { cx?: number; cy?: number; payload?: { netProfitPerGram?: number; dailyTargetPerGram?: number } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return null;
        const netProfitPerGram = Number(payload?.netProfitPerGram ?? 0);
        const hurdlePerGram = Number(payload?.dailyTargetPerGram ?? DAILY_TARGET_RAND_PER_GRAM);
        const color = netProfitPerGram >= hurdlePerGram ? '#10b981' : '#111111';
        return <circle cx={cx} cy={cy} r={5} fill={color} stroke="#000000" strokeWidth={1.5} />;
    };

    const renderProfitPerGramAchievedBarShape = (
        props: { x?: number; y?: number; width?: number; height?: number; payload?: { netProfitPerGram?: number } }
    ) => {
        const { x, y, width, height, payload } = props;
        if (x == null || y == null || width == null || height == null || width <= 0 || height === 0) return null;
        const absHeight = Math.abs(height);
        if (absHeight <= 0.5) return null;
        const topY = height >= 0 ? y : y + height;
        const innerWidth = Math.max(3, width * 0.58);
        const innerX = x + ((width - innerWidth) / 2);
        const netProfitPerGram = Number(payload?.netProfitPerGram ?? 0);
        const fill = netProfitPerGram >= 0 ? "#10b981" : "#ef4444";
        return (
            <rect
                x={innerX}
                y={topY}
                width={innerWidth}
                height={absHeight}
                fill={fill}
                stroke={fill}
                strokeWidth={0}
                rx={1.5}
                ry={1.5}
            />
        );
    };

    const renderProfitPerGramTooltip = (props: any) => {
        const active = Boolean(props?.active);
        const payload = Array.isArray(props?.payload) ? props.payload : [];
        if (!active || payload.length === 0) return null;
        const row = (payload[0]?.payload || {}) as Row;
        const fullDate = String(row.fullDate || '--');
        const netProfitPerGram = n((row as Row).netProfitPerGram);
        const dailyTargetPerGram = n((row as Row).dailyTargetPerGram);
        return (
            <div className="dashboard-tooltip">
                <div className="dashboard-tooltip-title">{formatProfitChartDate(fullDate)}</div>
                <div className="dashboard-tooltip-row"><span>Net Profit (R/g)</span><strong>R{fmt(netProfitPerGram, 2)}/g</strong></div>
                <div className="dashboard-tooltip-row"><span>Hurdle</span><strong>R{fmt(dailyTargetPerGram, 2)}/g</strong></div>
            </div>
        );
    };

    const renderHedgeTooltip = (props: any) => {
        const active = Boolean(props?.active);
        const payload = Array.isArray(props?.payload) ? props.payload : [];
        if (!active || payload.length === 0) return null;
        const row = (payload[0]?.payload || {}) as Row;
        const dateLabel = String(row.dateLabel || '--');
        const longTotal = n(row.longTotal);
        const shortTotal = Math.abs(n(row.shortTotal));
        const longHedge = n(row.longHedge);
        const shortHedge = Math.abs(n(row.shortHedge));
        const hedgeOz = n(row.hedgeOz);
        const coveragePct = n(row.coveragePct);
        const tradeCount = n(row.tradeCount);
        return (
            <div className="dashboard-tooltip">
                <div className="dashboard-tooltip-title">{dateLabel}</div>
                <div className="dashboard-tooltip-row">
                    <span>Trades</span>
                    <strong>{tradeCount}</strong>
                </div>
                <div className="dashboard-tooltip-row">
                    <span>TradeMC Buy (Long)</span>
                    <strong>{fmt(longTotal, 3)} oz</strong>
                </div>
                <div className="dashboard-tooltip-row">
                    <span>TradeMC Sell (Short)</span>
                    <strong>{fmt(shortTotal, 3)} oz</strong>
                </div>
                <div className="dashboard-tooltip-row">
                    <span>PMX Hedged</span>
                    <strong>{fmt(hedgeOz, 3)} oz</strong>
                </div>
                <div className="dashboard-tooltip-row">
                    <span>Coverage</span>
                    <strong>{fmt(coveragePct, 1)}%</strong>
                </div>
            </div>
        );
    };

    const renderHedgeResidualDot = (props: { cx?: number; cy?: number; payload?: { dayHedged?: boolean } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return null;
        const color = Boolean(payload?.dayHedged) ? '#10b981' : '#111111';
        return <circle cx={cx} cy={cy} r={5} fill={color} stroke="#000000" strokeWidth={1.5} />;
    };

    const renderHedgeStatusDot = (props: { cx?: number; cy?: number; payload?: { dayHedged?: boolean; statusDotY?: number } }) => {
        const { cx, cy, payload } = props;
        if (cx == null || cy == null) return <circle r={0} />;
        if (Math.abs(Number(payload?.statusDotY ?? 0)) <= 0.001) return <circle r={0} />;
        const isHedged = Boolean(payload?.dayHedged);
        const fill = isHedged ? '#10b981' : '#ef4444';
        return <circle cx={cx} cy={cy} r={5} fill={fill} stroke="#000000" strokeWidth={1.5} />;
    };

    return (
        <div>
            {Toast}
            <div className="page-header">
                <div>
                    <h2>Dashboard</h2>
                    <div className="page-subtitle">Overview of key metrics and daily performance</div>
                </div>
                <div className="btn-group">
                    <button className="btn btn-sm" onClick={() => { void load(); }}>Refresh</button>
                </div>
            </div>

            {/* StoneX Balances */}
            <div className="section">
                <div className="section-title">StoneX Balances</div>
                <div className="stat-grid dashboard-stat-grid">
                    <div className="stat-card">
                        <div className="stat-label">Gold (XAU)</div>
                        <div className={`stat-value ${xau > 0.001 ? 'positive' : xau < -0.001 ? 'negative' : ''}`}>
                            {fmt(xau, 4)} oz
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">USD Balance</div>
                        <div className={`stat-value ${usd > 0.01 ? 'positive' : usd < -0.01 ? 'negative' : ''}`}>
                            ${fmt(usd, 2)}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">ZAR Balance</div>
                        <div className={`stat-value ${zar > 0.01 ? 'positive' : zar < -0.01 ? 'negative' : ''}`}>
                            R{fmt(zar, 2)}
                        </div>
                    </div>
                </div>
            </div>

            {/* Key Metrics */}
            <div className="section">
                <div className="section-title">Key Metrics</div>
                <div className="stat-grid dashboard-stat-grid">
                    <div className="stat-card">
                        <div className="stat-label">Open Pairs</div>
                        <div className="stat-value">{fmt(openTradeCount, 0)}</div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Total Profit (ZAR)</div>
                        <div className={`stat-value ${totalProfitZar > 0.01 ? 'positive' : totalProfitZar < -0.01 ? 'negative' : ''}`}>
                            R{fmt(totalProfitZar, 2)}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Remaining to Hedge</div>
                        <div className={`stat-value ${remainingHedgeG <= 0.01 ? 'positive' : 'warning'}`}>
                            {fmt(remainingHedgeG, 2)} g
                        </div>
                        <div className="stat-sub">{fmt(remainingHedgeOz, 3)} oz</div>
                    </div>
                </div>
            </div>

            <div className="section">
                <div className="section-title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span>Daily Metal Hedge Totals</span>
                    <select
                        value={hedgePeriod}
                        onChange={e => setHedgePeriod(e.target.value)}
                        className="input"
                        style={{ width: 'auto', minWidth: 130, fontSize: 12, padding: '4px 8px', margin: 0 }}
                    >
                        <option value="7">Last 7 days</option>
                        <option value="14">Last 14 days</option>
                        <option value="30">Last 30 days</option>
                        <option value="60">Last 60 days</option>
                        <option value="90">Last 90 days</option>
                        <option value="all">All time</option>
                    </select>
                </div>
                <div className="dashboard-chart dashboard-chart-elevated">
                    <div className="dashboard-chart-head">
                        <div>
                            <div className="dashboard-chart-title">Daily TradeMC Positions vs PMX Hedges</div>
                            <div className="dashboard-chart-subtitle">Outer bar = TradeMC position · Inner bar = PMX hedge coverage · Grouped by day</div>
                        </div>
                        <div className="dashboard-chart-kpis">
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Longs (Buy)</div>
                                <div className="dashboard-kpi-value positive">{fmt(totalLongOz, 3)} oz</div>
                            </div>
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Shorts (Sell)</div>
                                <div className="dashboard-kpi-value negative">{fmt(totalShortOz, 3)} oz</div>
                            </div>
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Hedged</div>
                                <div className="dashboard-kpi-value">{fmt(totalHedgedOz, 3)} oz</div>
                            </div>
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Coverage</div>
                                <div className={`dashboard-kpi-value ${overallCoveragePct >= 95 ? 'positive' : overallCoveragePct >= 75 ? 'warning' : 'negative'}`}>
                                    {fmt(overallCoveragePct, 1)}%
                                </div>
                            </div>
                        </div>
                    </div>
                    {hedgeChartVisualData.length > 0 ? (
                        <>
                            <ResponsiveContainer width="100%" height={360}>
                                <ComposedChart
                                    data={hedgeChartVisualData}
                                    barSize={16}
                                    barGap={-16}
                                    margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                                >
                                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                    <XAxis
                                        dataKey="fullDate"
                                        tickFormatter={formatProfitChartDate}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={{ stroke: 'rgba(28,28,28,0.2)' }}
                                        interval="preserveStartEnd"
                                    />
                                    <YAxis
                                        tickFormatter={(v: number) => `${fmt(v, 2)} oz`}
                                        domain={[-hedgeAxisMax * 1.15, hedgeAxisMax * 1.15]}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={false}
                                        width={54}
                                    />
                                    <Tooltip content={renderHedgeTooltip} cursor={{ fill: 'rgba(180,114,61,0.06)' }} />
                                    <ReferenceLine y={0} stroke="rgba(28,28,28,0.28)" strokeDasharray="2 2" />
                                    <Bar dataKey="longTotal" shape={renderHurdleBarShape} isAnimationActive={false} />
                                    <Bar dataKey="shortTotal" shape={renderHurdleBarShape} isAnimationActive={false} />
                                    <Bar dataKey="longHedgeGood" fill="#3b82f6" name="PMX Hedge (Hedged)" isAnimationActive={false} />
                                    <Bar dataKey="shortHedgeGood" fill="#3b82f6" isAnimationActive={false} />
                                    <Bar dataKey="longHedgeBad" fill="#ef4444" name="PMX Hedge (Unhedged)" isAnimationActive={false} />
                                    <Bar dataKey="shortHedgeBad" fill="#ef4444" isAnimationActive={false} />
                                    <Line
                                        type="monotone"
                                        dataKey="statusDotY"
                                        name="Hedge Status"
                                        stroke="#111111"
                                        strokeWidth={2}
                                        dot={renderHedgeStatusDot}
                                        activeDot={false}
                                        isAnimationActive={false}
                                    />
                                </ComposedChart>
                            </ResponsiveContainer>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, border: '1.5px dashed #6b7280', borderRadius: 2, display: 'inline-block' }} /> TradeMC Position
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: 2, background: '#3b82f6', display: 'inline-block' }} /> PMX Hedge (Hedged)
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: 2, background: '#ef4444', display: 'inline-block' }} /> PMX Hedge (Unhedged)
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#10b981', display: 'inline-block' }} /> Hedged Dot
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#ef4444', display: 'inline-block' }} /> Unhedged Dot
                                </span>
                            </div>
                        </>
                    ) : (
                        <div style={{ textAlign: 'center', padding: '3rem', color: 'rgba(0,0,0,0.4)' }}>
                            No hedging data available
                        </div>
                    )}
                </div>
                <div className="dashboard-chart dashboard-chart-elevated" style={{ marginTop: 14 }}>
                    <div className="dashboard-chart-head">
                        <div>
                            <div className="dashboard-chart-title">Net Profit (R/g) vs Daily Hurdle</div>
                            <div className="dashboard-chart-subtitle">Daily hurdle = R1.3/g. Achieved bar = realized net profit per gram (green) / loss per gram (red).</div>
                        </div>
                            <div className="dashboard-chart-kpis">
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">Total Net Profit</div>
                                    <div className={`dashboard-kpi-value ${totalNetProfitDashboard >= 0 ? 'positive' : 'negative'}`}>R{fmt(totalNetProfitDashboard, 2)}</div>
                                </div>
                                <div className="dashboard-kpi">
                                    <div className="dashboard-kpi-label">Net Over R/g Target</div>
                                    <div className={`dashboard-kpi-value ${totalPerGramGapZar >= 0 ? 'positive' : 'negative'}`}>
                                        {totalPerGramGapZar >= 0 ? '+' : ''}R{fmt(totalPerGramGapZar, 2)}
                                    </div>
                            </div>
                        </div>
                    </div>
                    {profitChartData.length > 0 ? (
                        <>
                            <ResponsiveContainer width="100%" height={360}>
                                <ComposedChart
                                    data={profitChartData}
                                    barSize={16}
                                    barGap={-16}
                                    margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                                >
                                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                    <XAxis
                                        dataKey="fullDate"
                                        tickFormatter={formatProfitChartDate}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={{ stroke: 'rgba(28,28,28,0.2)' }}
                                        interval="preserveStartEnd"
                                    />
                                    <YAxis
                                        tickFormatter={(v: number) => `R${fmt(v, 2)}/g`}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={false}
                                        width={64}
                                    />
                                    <Tooltip content={renderProfitPerGramTooltip} cursor={{ fill: 'rgba(180,114,61,0.08)' }} />
                                    <ReferenceLine y={0} stroke="rgba(28,28,28,0.25)" strokeDasharray="2 2" />
                                    <Bar dataKey="dailyTargetPerGram" shape={renderProfitHurdleBarShape} isAnimationActive={false} />
                                    <Bar dataKey="netProfitPerGram" shape={renderProfitPerGramAchievedBarShape} isAnimationActive={false} />
                                    <Line
                                        type="monotone"
                                        dataKey="netProfitPerGram"
                                        name="Net Profit (R/g)"
                                        stroke="#111111"
                                        strokeWidth={2}
                                        dot={renderNetPerGramDot}
                                        activeDot={renderNetPerGramDot}
                                        isAnimationActive={false}
                                    />
                                </ComposedChart>
                            </ResponsiveContainer>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#111111', display: 'inline-block' }} /> Net Profit (R/g)
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: 2, background: 'linear-gradient(90deg, #10b981 0%, #10b981 50%, #ef4444 50%, #ef4444 100%)', display: 'inline-block' }} /> Achieved (P/L)
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, border: '1.5px dashed #b4723d', background: 'rgba(180,114,61,0.24)', borderRadius: 2, display: 'inline-block' }} /> Hurdle (R1.3/g)
                                </span>
                            </div>
                        </>
                    ) : (
                        <div style={{ textAlign: 'center', padding: '3rem', color: 'rgba(0,0,0,0.4)' }}>
                            No daily profit data available
                        </div>
                    )}
                </div>
            </div>

            {/* Daily P&L Chart */}
            <div className="section">
                <div className="section-title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <span>Daily P&L and Profit Targets</span>
                    <select
                        value={profitPeriod}
                        onChange={e => setProfitPeriod(e.target.value)}
                        className="input"
                        style={{ width: 'auto', minWidth: 130, fontSize: 12, padding: '4px 8px', margin: 0 }}
                    >
                        <option value="7">Last 7 days</option>
                        <option value="14">Last 14 days</option>
                        <option value="30">Last 30 days</option>
                        <option value="60">Last 60 days</option>
                        <option value="90">Last 90 days</option>
                        <option value="all">All time</option>
                    </select>
                </div>
                <div className="dashboard-chart dashboard-chart-elevated">
                    <div className="dashboard-chart-head">
                        <div>
                            <div className="dashboard-chart-title">Net Profit vs Daily Hurdle</div>
                            <div className="dashboard-chart-subtitle">Daily target = 0.15% × (|TradeMC Buy ZAR| + |TradeMC Sell ZAR|). Achieved bar = net profit (green) / loss (red).</div>
                        </div>
                        <div className="dashboard-chart-kpis">
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Total Net Profit</div>
                                <div className={`dashboard-kpi-value ${totalNetProfitDashboard >= 0 ? 'positive' : 'negative'}`}>R{fmt(totalNetProfitDashboard, 2)}</div>
                            </div>
                            <div className="dashboard-kpi">
                                <div className="dashboard-kpi-label">Net vs Target</div>
                                <div className={`dashboard-kpi-value ${totalTargetGapZar >= 0 ? 'positive' : 'negative'}`}>
                                    {totalTargetGapZar >= 0 ? '+' : ''}R{fmt(totalTargetGapZar, 2)}
                                </div>
                            </div>
                        </div>
                    </div>
                    {profitChartData.length > 0 ? (
                        <>
                            <ResponsiveContainer width="100%" height={360}>
                                <ComposedChart
                                    data={profitChartData}
                                    barSize={16}
                                    barGap={-16}
                                    margin={{ top: 5, right: 10, left: 10, bottom: 5 }}
                                >
                                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                    <XAxis
                                        dataKey="fullDate"
                                        tickFormatter={formatProfitChartDate}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={{ stroke: 'rgba(28,28,28,0.2)' }}
                                        interval="preserveStartEnd"
                                    />
                                    <YAxis
                                        tickFormatter={(v: number) => `R${fmt(v / 1000, 0)}k`}
                                        tick={{ fontSize: 10, fill: 'var(--text-muted)' }}
                                        tickLine={false}
                                        axisLine={false}
                                        width={54}
                                    />
                                    <Tooltip content={renderProfitTooltip} cursor={{ fill: 'rgba(180,114,61,0.08)' }} />
                                    <ReferenceLine y={0} stroke="rgba(28,28,28,0.25)" strokeDasharray="2 2" />
                                    <Bar dataKey="dailyTarget" shape={renderProfitHurdleBarShape} isAnimationActive={false} />
                                    <Bar dataKey="netProfit" shape={renderProfitAchievedBarShape} isAnimationActive={false} />
                                    <Line
                                        type="monotone"
                                        dataKey="netProfit"
                                        name="Net Profit (ZAR)"
                                        stroke="#111111"
                                        strokeWidth={2}
                                        dot={renderNetDot}
                                        activeDot={renderNetDot}
                                        isAnimationActive={false}
                                    />
                                </ComposedChart>
                            </ResponsiveContainer>
                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#111111', display: 'inline-block' }} /> Net Profit
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, borderRadius: 2, background: 'linear-gradient(90deg, #10b981 0%, #10b981 50%, #ef4444 50%, #ef4444 100%)', display: 'inline-block' }} /> Achieved (P/L)
                                </span>
                                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
                                    <span style={{ width: 10, height: 10, border: '1.5px dashed #b4723d', background: 'rgba(180,114,61,0.24)', borderRadius: 2, display: 'inline-block' }} /> Hurdle (0.15%)
                                </span>
                            </div>
                        </>
                    ) : (
                        <div style={{ textAlign: 'center', padding: '3rem', color: 'rgba(0,0,0,0.4)' }}>
                            No daily profit data available
                        </div>
                    )}
                </div>
            </div>

        </div>
    );
}

type ForecastDay = {
    day: number;
    date: string;
    mean: number;
    p50: number;
    p5: number;
    p10: number;
    p25: number;
    p75: number;
    p90: number;
    p95: number;
    pct_change: number;
};

type ForecastResult = {
    ok: boolean;
    error?: string;
    pair: string;
    forecast_days: number;
    current_price: number;
    mu_annual: number;
    sigma_annual: number;
    prob_up: number;
    data_start: string;
    data_end: string;
    data_points: number;
    simulation_count: number;
    daily_summary: ForecastDay[];
    historical?: { date: string; close: number }[];
};

function ForecastPanel({ pair, label, decimals = 2 }: { pair: string; label: string; decimals?: number }) {
    const [data, setData] = useState<ForecastResult | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [currentPrice, setCurrentPrice] = useState<{ rate: number; last_refreshed: string } | null>(null);
    const [priceLoading, setPriceLoading] = useState(false);
    const [days, setDays] = useState(30);
    const [sims, setSims] = useState(10000);

    const loadForecast = useCallback(async (refresh = false) => {
        setLoading(true);
        setError('');
        try {
            const res = refresh
                ? await api.refreshForecast(pair, days, sims) as unknown as ForecastResult
                : await api.getForecast(pair, days, sims) as unknown as ForecastResult;
            if (!res.ok) throw new Error(res.error || 'Forecast failed');
            setData(res);
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setLoading(false);
        }
    }, [pair, days, sims]);

    const fetchPrice = useCallback(async () => {
        setPriceLoading(true);
        try {
            const res = await api.getForecastCurrentPrice(pair) as { ok: boolean; rate: number; last_refreshed: string; error?: string };
            if (!res.ok) throw new Error(res.error || 'Price fetch failed');
            setCurrentPrice({ rate: res.rate, last_refreshed: res.last_refreshed });
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setPriceLoading(false);
        }
    }, [pair]);

    useEffect(() => { void loadForecast(); }, [loadForecast]);

    const pctClass = (v: number) => v > 0 ? 'num positive' : v < 0 ? 'num negative' : 'num';

    return (
        <div className="card" style={{ marginBottom: '1.5rem' }}>
            <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.75rem' }}>
                <h3 style={{ margin: 0, fontSize: '1rem' }}>{label}</h3>
                <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
                    <label style={{ fontSize: '0.75rem', color: '#64748b' }}>
                        Days: <input type="number" min={7} max={90} value={days} onChange={e => setDays(Number(e.target.value) || 30)}
                            style={{ width: 50, padding: '0.2rem 0.3rem', fontSize: '0.75rem', border: '1px solid #d1d5db', borderRadius: 4 }} />
                    </label>
                    <label style={{ fontSize: '0.75rem', color: '#64748b' }}>
                        Sims: <select value={sims} onChange={e => setSims(Number(e.target.value))}
                            style={{ padding: '0.2rem 0.3rem', fontSize: '0.75rem', border: '1px solid #d1d5db', borderRadius: 4 }}>
                            <option value={1000}>1,000</option>
                            <option value={5000}>5,000</option>
                            <option value={10000}>10,000</option>
                            <option value={25000}>25,000</option>
                            <option value={50000}>50,000</option>
                        </select>
                    </label>
                    <button className="btn btn-sm" onClick={() => { void fetchPrice(); }} disabled={priceLoading}>
                        {priceLoading ? 'Fetching...' : 'Get Live Price'}
                    </button>
                    <button className="btn btn-sm btn-primary" onClick={() => { void loadForecast(true); }} disabled={loading}>
                        {loading ? 'Running...' : 'Update Forecast'}
                    </button>
                </div>
            </div>

            {error && <div style={{ margin: '0.75rem', padding: '0.6rem 0.75rem', fontSize: '0.8rem', background: '#fef2f2', color: '#dc2626', border: '1px solid #fecaca', borderRadius: 6 }}>{error}</div>}

            {data && (
                <>
                    <div className="stat-grid dashboard-stat-grid">
                        <div className="stat-card">
                            <div className="stat-label">Current Price</div>
                            <div className="stat-value">{fmt(currentPrice?.rate ?? data.current_price, decimals)}</div>
                            {currentPrice && <div style={{ fontSize: '0.65rem', color: '#94a3b8' }}>as of {currentPrice.last_refreshed}</div>}
                        </div>
                        <div className="stat-card">
                            <div className="stat-label">Ann. Drift</div>
                            <div className="stat-value">{(data.mu_annual * 100).toFixed(2)}%</div>
                        </div>
                        <div className="stat-card">
                            <div className="stat-label">Ann. Volatility</div>
                            <div className="stat-value">{(data.sigma_annual * 100).toFixed(2)}%</div>
                        </div>
                        <div className="stat-card">
                            <div className="stat-label">Prob(Up) {data.forecast_days}d</div>
                            <div className={`stat-value ${data.prob_up >= 50 ? 'positive' : 'negative'}`}>{data.prob_up}%</div>
                        </div>
                    </div>

                    {/* Monte Carlo Forecast Chart */}
                    {(() => {
                        const hist = (data.historical || []).slice(-30);
                        const chartData = [
                            ...hist.map(h => ({ date: h.date, close: h.close, mean: null as number | null, p50: null as number | null, p10_p90: [null, null] as [number | null, number | null], p25_p75: [null, null] as [number | null, number | null], p5_p95: [null, null] as [number | null, number | null] })),
                            // bridge point: last historical day at current price, starts the forecast bands
                            ...(hist.length > 0 ? [{
                                date: hist[hist.length - 1].date,
                                close: null as number | null,
                                mean: data.current_price,
                                p50: data.current_price,
                                p10_p90: [data.current_price, data.current_price] as [number | null, number | null],
                                p25_p75: [data.current_price, data.current_price] as [number | null, number | null],
                                p5_p95: [data.current_price, data.current_price] as [number | null, number | null],
                            }] : []),
                            ...data.daily_summary.map(r => ({
                                date: r.date,
                                close: null as number | null,
                                mean: r.mean,
                                p50: r.p50,
                                p10_p90: [r.p10, r.p90] as [number | null, number | null],
                                p25_p75: [r.p25, r.p75] as [number | null, number | null],
                                p5_p95: [r.p5 ?? r.p10, r.p95 ?? r.p90] as [number | null, number | null],
                            })),
                        ];
                        const allVals = chartData.flatMap(d => [d.close, d.mean, d.p5_p95?.[0], d.p5_p95?.[1]].filter((v): v is number => v != null && isFinite(v)));
                        const yMin = Math.min(...allVals);
                        const yMax = Math.max(...allVals);
                        const pad = (yMax - yMin) * 0.05 || 1;
                        const fmtY = (v: number) => fmt(v, decimals);
                        const fmtDateShort = (d: string) => { const parts = d.split('-'); return parts.length >= 3 ? `${parts[1]}/${parts[2]}` : d; };

                        return (
                            <div style={{ padding: '0.75rem' }}>
                                <div style={{ fontSize: '0.8rem', fontWeight: 600, marginBottom: '0.5rem', color: 'var(--text-primary, #1e293b)' }}>
                                    Monte Carlo Forecast — {data.simulation_count?.toLocaleString()} simulations
                                </div>
                                <ResponsiveContainer width="100%" height={360}>
                                    <ComposedChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                                        <defs>
                                            <linearGradient id={`fcBand95_${pair}`} x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.08} />
                                                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.04} />
                                            </linearGradient>
                                            <linearGradient id={`fcBand90_${pair}`} x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.14} />
                                                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.08} />
                                            </linearGradient>
                                            <linearGradient id={`fcBand50_${pair}`} x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.25} />
                                                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.15} />
                                            </linearGradient>
                                        </defs>
                                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(28,28,28,0.08)" />
                                        <XAxis dataKey="date" tickFormatter={fmtDateShort} tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={{ stroke: 'rgba(28,28,28,0.2)' }} interval="preserveStartEnd" />
                                        <YAxis domain={[yMin - pad, yMax + pad]} tickFormatter={fmtY} tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} width={70} />
                                        <Tooltip
                                            contentStyle={{ fontSize: '0.75rem', borderRadius: 6, border: '1px solid #e2e8f0' }}
                                            formatter={(value: unknown, name: unknown) => {
                                                if (Array.isArray(value)) return [value.map((v: unknown) => fmtY(Number(v))).join(' — '), String(name ?? '')];
                                                return [fmtY(Number(value)), String(name ?? '')];
                                            }}
                                            labelFormatter={(label: unknown) => fmtDateShort(String(label ?? ''))}
                                        />
                                        <Area type="monotone" dataKey="p5_p95" fill={`url(#fcBand95_${pair})`} stroke="none" name="P5–P95" isAnimationActive={false} connectNulls={false} />
                                        <Area type="monotone" dataKey="p10_p90" fill={`url(#fcBand90_${pair})`} stroke="none" name="P10–P90" isAnimationActive={false} connectNulls={false} />
                                        <Area type="monotone" dataKey="p25_p75" fill={`url(#fcBand50_${pair})`} stroke="none" name="P25–P75" isAnimationActive={false} connectNulls={false} />
                                        <Line type="monotone" dataKey="close" stroke="#1e293b" strokeWidth={2} dot={false} name="Historical" isAnimationActive={false} connectNulls={false} />
                                        <Line type="monotone" dataKey="mean" stroke="#3b82f6" strokeWidth={2} dot={false} name="Mean Forecast" isAnimationActive={false} connectNulls={false} />
                                        <Line type="monotone" dataKey="p50" stroke="#8b5cf6" strokeWidth={1.5} strokeDasharray="4 3" dot={false} name="Median (P50)" isAnimationActive={false} connectNulls={false} />
                                        <ReferenceLine y={data.current_price} stroke="#f59e0b" strokeDasharray="3 3" label={{ value: `Current: ${fmtY(data.current_price)}`, position: 'right', fontSize: 10, fill: '#f59e0b' }} />
                                    </ComposedChart>
                                </ResponsiveContainer>
                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 16, height: 2, background: '#1e293b', display: 'inline-block' }} /> Historical</span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 16, height: 2, background: '#3b82f6', display: 'inline-block' }} /> Mean</span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 16, height: 2, background: '#8b5cf6', borderTop: '1px dashed #8b5cf6', display: 'inline-block' }} /> P50</span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 12, height: 12, background: 'rgba(59,130,246,0.25)', borderRadius: 2, display: 'inline-block' }} /> P25–P75</span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 12, height: 12, background: 'rgba(59,130,246,0.14)', borderRadius: 2, display: 'inline-block' }} /> P10–P90</span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}><span style={{ width: 12, height: 12, background: 'rgba(59,130,246,0.06)', borderRadius: 2, display: 'inline-block' }} /> P5–P95</span>
                                </div>
                            </div>
                        );
                    })()}

                    <div style={{ padding: '0.75rem', overflowX: 'auto' }}>
                        <table className="data-table" style={{ fontSize: '0.75rem' }}>
                            <thead>
                                <tr>
                                    <th>Date</th>
                                    <th style={{ textAlign: 'right' }}>Mean</th>
                                    <th style={{ textAlign: 'right' }}>P50</th>
                                    <th style={{ textAlign: 'right' }}>P10</th>
                                    <th style={{ textAlign: 'right' }}>P90</th>
                                    <th style={{ textAlign: 'right' }}>Change %</th>
                                </tr>
                            </thead>
                            <tbody>
                                {Array.isArray(data.daily_summary) && data.daily_summary.length > 0 ? data.daily_summary.map((row) => (
                                    <tr key={`${row.day}-${row.date}`}>
                                        <td>{row.date}</td>
                                        <td style={{ textAlign: 'right' }}>{fmt(row.mean, decimals)}</td>
                                        <td style={{ textAlign: 'right' }}>{fmt(row.p50, decimals)}</td>
                                        <td style={{ textAlign: 'right' }}>{fmt(row.p10, decimals)}</td>
                                        <td style={{ textAlign: 'right' }}>{fmt(row.p90, decimals)}</td>
                                        <td style={{ textAlign: 'right' }} className={pctClass(row.pct_change)}>{row.pct_change > 0 ? '+' : ''}{Number(row.pct_change ?? 0).toFixed(2)}%</td>
                                    </tr>
                                )) : (
                                    <tr><td colSpan={6} style={{ padding: '1rem', color: 'var(--text-muted)' }}>No forecast rows available</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </>
            )}
        </div>
    );
}

const FORECAST_SUB_TABS = [
    { id: 'gold', label: 'Gold (XAU/USD)', pair: 'gold', decimals: 2 },
    { id: 'usdzar', label: 'USD/ZAR', pair: 'usdzar', decimals: 4 },
    { id: 'purchases', label: 'Purchases (XAU/ZAR)', pair: 'purchases', decimals: 2 },
] as const;

function ForecastTab() {
    const [subTab, setSubTab] = usePersistentState<string>('forecast:sub_tab', 'gold');
    const active = FORECAST_SUB_TABS.find(t => t.id === subTab) || FORECAST_SUB_TABS[0];

    return (
        <div>
            <div style={{ display: 'flex', gap: '0', marginBottom: '1rem', borderBottom: '2px solid #e2e8f0' }}>
                {FORECAST_SUB_TABS.map(t => (
                    <button
                        key={t.id}
                        onClick={() => setSubTab(t.id)}
                        style={{
                            padding: '0.55rem 1.2rem',
                            fontSize: '0.82rem',
                            fontWeight: subTab === t.id ? 600 : 400,
                            color: subTab === t.id ? '#1e40af' : '#64748b',
                            background: subTab === t.id ? '#eff6ff' : 'transparent',
                            border: 'none',
                            borderBottom: subTab === t.id ? '2px solid #3b82f6' : '2px solid transparent',
                            marginBottom: '-2px',
                            cursor: 'pointer',
                        }}
                    >
                        {t.label}
                    </button>
                ))}
            </div>
            <ForecastPanel key={active.id} pair={active.pair} label={active.label} decimals={active.decimals} />
        </div>
    );
}

// ===================================================================
// SIDEBAR NAV ITEMS
// ===================================================================
type NavItem = {
    id: string;
    label: string;
    adminOnly?: boolean;
};

type NavSection = {
    label: string;
    items: NavItem[];
};

const NAV_SECTIONS: NavSection[] = [
    {
        label: 'Overview',
        items: [
            { id: 'dashboard', label: 'Dashboard' },
            { id: 'forecast', label: 'Forecasts' },
            { id: 'profit', label: 'Profit' },
        ],
    },
    {
        label: 'Trading',
        items: [
            { id: 'pmx_ledger', label: 'PMX Ledger' },
            { id: 'hedging', label: 'Hedging' },
            { id: 'forward_exposure', label: 'Forward Exposure' },
            { id: 'open_positions_reval', label: 'Open Positions Reval' },
            { id: 'trading_worksheet', label: 'Trading Worksheet' },
        ],
    },
    {
        label: 'TradeMC',
        items: [
            { id: 'trademc', label: 'TradeMC Trades' },
            { id: 'suppliers', label: 'Supplier Balances' },
        ],
    },
    {
        label: 'Tools',
        items: [
            { id: 'export_trades', label: 'Export Trades' },
            { id: 'ticket', label: 'Trading Ticket' },
            { id: 'trade_breakdown', label: 'Trade Breakdown' },
        ],
    },
    {
        label: 'Admin',
        items: [
            { id: 'user_management', label: 'User Management', adminOnly: true },
        ],
    },
];

const PAGE_TITLES: Record<string, string> = {
    dashboard: 'Dashboard',
    pmx_ledger: 'PMX Ledger',
    hedging: 'Hedging',
    profit: 'Profit',
    forward_exposure: 'Forward Exposure',
    open_positions_reval: 'Open Positions Reval',

    trademc: 'TradeMC Trades',
    suppliers: 'Supplier Balances',
    export_trades: 'Export Trades',
    ticket: 'Trading Ticket',
    trading_worksheet: 'Trading Worksheet',
    trade_breakdown: 'Trade Breakdown',
    forecast: 'Forecasts',
    user_management: 'User Management',
};

// ===================================================================
// MAIN APP
// ===================================================================
export default function App() {
    const today = new Date().toISOString().slice(0, 10);
    const reconEndDate = useMemo(() => {
        const dt = new Date();
        return dt.toISOString().slice(0, 10);
    }, []);
    const [tab, setTab] = usePersistentState('ui:active_tab', 'dashboard');
    const [authLoading, setAuthLoading] = useState(true);
    const [authUser, setAuthUser] = useState<AppUser | null>(null);
    const [reconDeltaAlert, setReconDeltaAlert] = useState(false);
    const [tradeMCMissingSageAlert, setTradeMCMissingSageAlert] = useState(false);
    const [priceWarnings, setPriceWarnings] = useState<Array<{ id: string; label: string; message: string; rowId: number }>>([]);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [authError, setAuthError] = useState('');
    const [authBusy, setAuthBusy] = useState(false);
    const pmxAutoSyncInFlightRef = useRef(false);
    const trademcAutoSyncInFlightRef = useRef(false);

    const refreshAuth = useCallback(async () => {
        setAuthLoading(true);
        try {
            const res = await api.authMe();
            setAuthUser(res.user);
            setUsername(res.user.username || '');
            setAuthError('');
        } catch {
            setAuthUser(null);
        } finally {
            setAuthLoading(false);
        }
    }, []);

    useEffect(() => {
        void refreshAuth();
    }, [refreshAuth]);

    useEffect(() => {
        if (!authUser) return;

        const syncPmxInBackground = async () => {
            if (pmxAutoSyncInFlightRef.current) return;
            pmxAutoSyncInFlightRef.current = true;
            try {
                await api.syncPmxLedger({ cmdty: 'All', trd_opt: 'All' });
                window.dispatchEvent(new Event(PMX_AUTO_SYNC_EVENT));
            } catch {
                // Keep background sync silent; manual sync in PMX tab shows detailed errors.
            } finally {
                pmxAutoSyncInFlightRef.current = false;
            }
        };

        void syncPmxInBackground();
        const timer = window.setInterval(() => {
            void syncPmxInBackground();
        }, BACKGROUND_REFRESH_MS);

        return () => window.clearInterval(timer);
    }, [authUser]);

    useEffect(() => {
        if (!authUser) {
            setReconDeltaAlert(false);
            return;
        }

        const computeReconAlert = async () => {
            try {
                            const params: Record<string, string> = {
                    start_date: '2026-03-02',
                    end_date: reconEndDate,
                    baseline_date: '2026-03-01',
                };
                if (typeof window !== 'undefined') {
                    const raw = window.localStorage.getItem('recon:baseline_xau');
                    if (raw !== null) {
                        try {
                            const parsed = JSON.parse(raw);
                            const text = String(parsed ?? '').trim();
                            const n = Number(text);
                            if (text && Number.isFinite(n)) params.baseline_xau = String(n);
                        } catch {
                            // Ignore malformed local storage values.
                        }
                    }
                }
                const res = await api.getPmxReconciliation(params);
                const s = ((res as unknown as Record<string, unknown>).summary || {}) as Record<string, unknown>;
                const reconRows = Array.isArray((res as unknown as Record<string, unknown>).rows)
                    ? (((res as unknown as Record<string, unknown>).rows) as Row[])
                    : [];
                persistCachedReconPayload(s as Row, reconRows);
                const delta = toNullableNumber(s.delta_to_account);
                setReconDeltaAlert(delta !== null && Math.abs(delta) > RECON_DELTA_EPSILON);
            } catch {
                // Keep last known state on background check failure.
            }
        };

        const onReconDelta = (event: Event) => {
            const detail = (event as CustomEvent<{ hasDelta?: unknown }>).detail;
            setReconDeltaAlert(Boolean(detail && detail.hasDelta));
        };
        const onPmxAutoSync = () => { void computeReconAlert(); };

        void computeReconAlert();
        window.addEventListener(RECON_DELTA_EVENT, onReconDelta as EventListener);
        window.addEventListener(PMX_AUTO_SYNC_EVENT, onPmxAutoSync);
        const timer = window.setInterval(() => { void computeReconAlert(); }, BACKGROUND_REFRESH_MS);
        return () => {
            window.removeEventListener(RECON_DELTA_EVENT, onReconDelta as EventListener);
            window.removeEventListener(PMX_AUTO_SYNC_EVENT, onPmxAutoSync);
            window.clearInterval(timer);
        };
    }, [authUser, reconEndDate]);

    useEffect(() => {
        if (!authUser) {
            setTradeMCMissingSageAlert(false);
            return;
        }

        const computeTradeMCMissingSageAlert = async () => {
            try {
                const rows = await api.getTradeMCTrades();
                const hasMissing = (Array.isArray(rows) ? rows : []).some((raw) => {
                    const row = (raw && typeof raw === 'object') ? raw as Row : {};
                    const weight = toNullableNumber(
                        row.weight ?? row.Weight ?? row.qty ?? row.quantity ?? row.Quantity
                    );
                    // TradeMC "Sage Reference" is normally notes, but we tolerate variant keys.
                    const sageRef = asText(
                        row.notes ?? row.Notes ?? row.sage_reference ?? row.sageReference ?? row.sage_ref,
                        ''
                    );
                    const refNumber = asText(row.ref_number ?? row.refNumber ?? row['Ref #'], '');
                    // Positive-only rule; treat blank "-", "--", and "n/a" as missing.
                    const missingSage = !sageRef || ['-', '--', 'n/a', 'na'].includes(sageRef.toLowerCase());
                    const missingRef = !refNumber || ['-', '--', 'n/a', 'na'].includes(refNumber.toLowerCase());
                    return weight !== null && weight > 0 && (missingSage || missingRef);
                });
                setTradeMCMissingSageAlert(hasMissing);
            } catch {
                // Keep last known state on background check failure.
            }
        };

        const onTradeMCAutoSync = () => { void computeTradeMCMissingSageAlert(); };
        const onTradeMCMissingSage = (event: Event) => {
            const detail = (event as CustomEvent<{ hasMissing?: unknown }>).detail;
            if (detail && typeof detail.hasMissing !== 'undefined') {
                setTradeMCMissingSageAlert(Boolean(detail.hasMissing));
            }
        };

        void computeTradeMCMissingSageAlert();
        window.addEventListener(TRADEMC_AUTO_SYNC_EVENT, onTradeMCAutoSync);
        window.addEventListener(TRADEMC_MISSING_SAGE_EVENT, onTradeMCMissingSage as EventListener);
        const timer = window.setInterval(() => { void computeTradeMCMissingSageAlert(); }, BACKGROUND_REFRESH_MS);
        return () => {
            window.removeEventListener(TRADEMC_AUTO_SYNC_EVENT, onTradeMCAutoSync);
            window.removeEventListener(TRADEMC_MISSING_SAGE_EVENT, onTradeMCMissingSage as EventListener);
            window.clearInterval(timer);
        };
    }, [authUser]);

    useEffect(() => {
        const onPriceWarning = (event: Event) => {
            const detail = (event as CustomEvent<{ warnings?: Array<{ id: string; label: string; message: string; rowId: number }> }>).detail;
            setPriceWarnings(Array.isArray(detail?.warnings) ? detail.warnings : []);
        };
        window.addEventListener(PRICE_WARNING_EVENT, onPriceWarning as EventListener);
        return () => window.removeEventListener(PRICE_WARNING_EVENT, onPriceWarning as EventListener);
    }, []);

    useEffect(() => {
        if (!authUser) return;

        const syncTradeMCInBackground = async () => {
            if (trademcAutoSyncInFlightRef.current) return;
            trademcAutoSyncInFlightRef.current = true;
            try {
                const res = await api.syncTradeMC({ wait: false, incremental: true, replace: false });
                const payload = (res as Row) || {};
                if (payload.error) return;
                window.dispatchEvent(new Event(TRADEMC_AUTO_SYNC_EVENT));
            } catch {
                // Keep background sync silent; manual sync in TradeMC tab shows detailed errors.
            } finally {
                trademcAutoSyncInFlightRef.current = false;
            }
        };

        void syncTradeMCInBackground();
        const timer = window.setInterval(() => {
            void syncTradeMCInBackground();
        }, BACKGROUND_REFRESH_MS);

        return () => window.clearInterval(timer);
    }, [authUser]);

    const onLogin = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        if (!username.trim() || !password) {
            setAuthError('Enter your username and password.');
            return;
        }
        setAuthBusy(true);
        setAuthError('');
        try {
            const res = await api.authLogin(username.trim(), password);
            setAuthUser(res.user);
            setPassword('');
        } catch (err: unknown) {
            setAuthError(err instanceof Error ? err.message : String(err || 'Login failed'));
        } finally {
            setAuthBusy(false);
        }
    };

    const onLogout = async () => {
        setAuthBusy(true);
        try {
            await api.authLogout();
        } catch {
            // Always clear local auth state, even if API logout fails.
        } finally {
            setAuthUser(null);
            setPassword('');
            setAuthBusy(false);
        }
    };

    const isAdmin = !!authUser && (
        String(authUser.role || '').toLowerCase() === 'admin'
        || (Array.isArray(authUser.permissions) && authUser.permissions.includes('admin'))
    );

    useEffect(() => {
        if (!isAdmin && tab === 'user_management') {
            setTab('pmx_ledger');
        }
    }, [isAdmin, tab, setTab]);

    const navSections = useMemo(
        () =>
            NAV_SECTIONS
                .map(section => ({
                    ...section,
                    items: section.items.filter(item => !item.adminOnly || isAdmin),
                }))
                .filter(section => section.items.length > 0),
        [isAdmin]
    );

    useEffect(() => {
        const validTabIds = navSections.flatMap(section => section.items.map(item => item.id));
        if (!validTabIds.includes(tab)) {
            setTab('open_positions_reval');
        }
    }, [navSections, tab, setTab]);

    if (authLoading) {
        return (
            <div className="auth-loading-shell">
                <div className="auth-loading-card">Checking authentication...</div>
            </div>
        );
    }

    if (!authUser) {
        return (
            <div className="auth-shell">
                <form className="auth-card" onSubmit={onLogin}>
                    <div className="auth-side">
                        <div className="auth-side-logo">
                            <img src="/logo-light.png" alt="Metal Concentrators" style={{ height: '64px', marginBottom: '16px' }} />
                            <h2>FOUNDATION</h2>
                            <p>FX &amp; Metal Hedging Platform</p>
                        </div>
                        <div className="auth-side-note">Metal Concentrators SA</div>
                    </div>

                    <div className="auth-form">
                        <div className="auth-brand">
                            <h3>Sign In</h3>
                            <p>Use your assigned account credentials.</p>
                        </div>

                        <label className="auth-label">
                            <span className="auth-label-text">Username</span>
                            <div className="auth-input-wrap">
                                <svg className="auth-input-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" />
                                    <circle cx="12" cy="7" r="4" />
                                </svg>
                                <input
                                    type="text"
                                    value={username}
                                    onChange={(e) => setUsername(e.target.value)}
                                    autoComplete="username"
                                    placeholder="Username"
                                    disabled={authBusy}
                                />
                            </div>
                        </label>
                        <label className="auth-label">
                            <span className="auth-label-text">Password</span>
                            <div className="auth-input-wrap">
                                <svg className="auth-input-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                    <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
                                    <path d="M7 11V7a5 5 0 0 1 10 0v4" />
                                </svg>
                                <input
                                    type="password"
                                    value={password}
                                    onChange={(e) => setPassword(e.target.value)}
                                    autoComplete="current-password"
                                    placeholder="Password"
                                    disabled={authBusy}
                                />
                            </div>
                        </label>

                        {authError && <div className="auth-error">{authError}</div>}

                        <button className="auth-submit-btn" type="submit" disabled={authBusy}>
                            {authBusy ? (
                                <>
                                    <span className="auth-submit-spinner" />
                                    Signing in...
                                </>
                            ) : (
                                'Sign In'
                            )}
                        </button>
                        <div className="auth-footer">Read-only and admin access are supported.</div>
                    </div>
                </form>
            </div>
        );
    }

    return (
        <>
            {/* Sidebar */}
            <aside className="sidebar">
                <div className="sidebar-logo">
                    <img src="/logo-dark.png" alt="Metal Concentrators" style={{ height: '32px', marginBottom: '8px' }} />
                    <h1>FOUNDATION</h1>
                    <span>FX &amp; Metal Hedging</span>
                </div>
                <nav className="sidebar-nav">
                    {navSections.map(section => (
                        <div key={section.label}>
                            <div className="sidebar-section-label">{section.label}</div>
                            {section.items.map(item => (
                                <button
                                    key={item.id}
                                    className={`nav-btn ${tab === item.id ? 'active' : ''}`}
                                    onClick={() => setTab(item.id)}
                                >
                                    <span className="nav-icon">
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                            {item.id === 'dashboard' && <><path d="M3 13a9 9 0 1 1 18 0" /><line x1="12" y1="13" x2="17" y2="8" /><line x1="7" y1="13" x2="7.01" y2="13" /><line x1="17" y1="13" x2="17.01" y2="13" /></>}
                                            {item.id === 'pmx_ledger' && <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /></>}
                                            {(item.id === 'positions' || item.id === 'open_positions_reval') && <><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></>}
                                            {item.id === 'hedging' && <><rect x="3" y="11" width="18" height="11" rx="2" ry="2" /><path d="M7 11V7a5 5 0 0 1 10 0v4" /></>}
                                            {item.id === 'profit' && <><line x1="4" y1="20" x2="20" y2="20" /><rect x="6" y="11" width="3" height="7" /><rect x="11" y="8" width="3" height="10" /><rect x="16" y="5" width="3" height="13" /></>}
                                            {item.id === 'forward_exposure' && <><rect x="3" y="4" width="18" height="18" rx="2" ry="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></>}
                                            {item.id === 'trademc' && <><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" /></>}
                                            {item.id === 'suppliers' && <><path d="M6 2L3 6v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6l-3-4z" /><line x1="3" y1="6" x2="21" y2="6" /></>}
                                            {item.id === 'export_trades' && <><path d="M12 3v12" /><polyline points="7 10 12 15 17 10" /><path d="M5 21h14" /></>}
                                            {item.id === 'ticket' && <><polyline points="6 9 6 2 18 2 18 9" /><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2" /><rect x="6" y="14" width="12" height="8" /></>}
                                            {item.id === 'forecast' && <><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></>}
                                            {item.id === 'trading_worksheet' && <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="8" y1="13" x2="16" y2="13" /><line x1="8" y1="17" x2="14" y2="17" /></>}
                                            {item.id === 'trade_breakdown' && <><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="8" y1="13" x2="16" y2="13" /><line x1="8" y1="17" x2="14" y2="17" /></>}
                                            {item.id === 'user_management' && <><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="8.5" cy="7" r="4" /><path d="M20 8v6" /><path d="M23 11h-6" /></>}
                                        </svg>
                                    </span>
                                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.4rem' }}>
                                        <span style={item.id === 'open_positions_reval' && reconDeltaAlert ? { color: '#dc3545' } : {}}>{item.label}</span>
                                        {item.id === 'open_positions_reval' && reconDeltaAlert && (
                                            <span
                                                aria-label="recon-delta-alert"
                                                title="Recon delta detected"
                                                style={{
                                                    width: '8px',
                                                    height: '8px',
                                                    borderRadius: '50%',
                                                    backgroundColor: '#dc3545',
                                                    boxShadow: '0 0 0 2px rgba(220,53,69,0.2)',
                                                    display: 'inline-block',
                                                }}
                                            />
                                        )}
                                        {item.id === 'trademc' && tradeMCMissingSageAlert && (
                                            <span
                                                aria-label="trademc-missing-sage-alert"
                                                title="Positive TradeMC trade(s) missing Sage Reference"
                                                style={{
                                                    width: '8px',
                                                    height: '8px',
                                                    borderRadius: '50%',
                                                    backgroundColor: '#dc3545',
                                                    boxShadow: '0 0 0 2px rgba(220,53,69,0.2)',
                                                    display: 'inline-block',
                                                }}
                                            />
                                        )}
                                        {item.id === 'trademc' && priceWarnings.length > 0 && (
                                            <span
                                                aria-label="price-warning-alert"
                                                title="Price deviation warning"
                                                style={{
                                                    width: '14px',
                                                    height: '14px',
                                                    borderRadius: '50%',
                                                    backgroundColor: '#dc3545',
                                                    color: '#fff',
                                                    display: 'inline-flex',
                                                    alignItems: 'center',
                                                    justifyContent: 'center',
                                                    fontSize: '10px',
                                                    fontWeight: 800,
                                                    lineHeight: 1,
                                                }}
                                            >
                                                !
                                            </span>
                                        )}
                                    </span>
                                </button>
                            ))}
                        </div>
                    ))}
                </nav>
                <div className="sidebar-footer">
                    <div className="sidebar-footer-text">Metal Concentrators SA</div>
                </div>
            </aside>

            {/* Main Content */}
            <div className="main-wrapper">
                <header className="content-header">
                    <div style={{ display: 'flex', alignItems: 'center' }}>
                        <span className="content-header-title">{PAGE_TITLES[tab]}</span>
                    </div>
                    <div className="header-user">
                        <span className="header-user-name">
                            {authUser.display_name || authUser.username} ({authUser.role})
                        </span>
                        <button className="btn btn-sm" onClick={onLogout} disabled={authBusy}>
                            {authBusy ? 'Signing out...' : 'Sign Out'}
                        </button>
                    </div>
                </header>
                <main className="content-body">
                    {priceWarnings.length > 0 && (
                        <div className="corner-warning-toast">
                            <div style={{ fontWeight: 800, marginBottom: 4 }}>Price warning</div>
                            <div style={{ fontSize: 12, lineHeight: 1.4, marginBottom: 8 }}>
                                {priceWarnings[0]?.message}
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                                <button
                                    className="btn btn-sm btn-primary"
                                    onClick={() => {
                                        const ids = priceWarnings.map(w => w.id);
                                        persistAckedPriceWarnings({ ...readAckedPriceWarnings(), ...Object.fromEntries(ids.map(id => [id, true])) });
                                        window.dispatchEvent(new CustomEvent(PRICE_WARNING_ACK_EVENT));
                                        window.dispatchEvent(new CustomEvent(PRICE_WARNING_EVENT, { detail: { warnings: [] } }));
                                        setPriceWarnings([]);
                                    }}
                                >
                                    ✓ Ack
                                </button>
                            </div>
                        </div>
                    )}
                    <RenderGuard title={PAGE_TITLES[String(tab)] || 'Current Section'}>
                        {tab === 'dashboard' && <Dashboard />}
                        {tab === 'forecast' && <ForecastTab />}
                        {tab === 'pmx_ledger' && <PMXLedger />}
                        {tab === 'profit' && <ProfitTab />}
                        {tab === 'forward_exposure' && <ForwardExposure />}
                        {tab === 'open_positions_reval' && <OpenPositionsReval />}

                        {tab === 'trademc' && <TradeMCTrades />}
                        {tab === 'hedging' && <GoldHedging />}
                        {tab === 'suppliers' && <SupplierBalances />}
                        {tab === 'export_trades' && <ExportTrades />}
                        {tab === 'ticket' && <TradingTicket />}
                        {tab === 'trading_worksheet' && <WeightedAverage />}
                        {tab === 'trade_breakdown' && <TradeBreakdownTab />}
                        {tab === 'user_management' && isAdmin && <UserManagement currentUserId={authUser ? Number(authUser.id) : null} />}
                    </RenderGuard>
                </main>
            </div>
        </>
    );
}
const cellLabel = 'Trade #';









