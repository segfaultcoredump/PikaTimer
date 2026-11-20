/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * Talking to PikaTimer: clock sync, the bib list, and the read queue.
 *
 * Every read is saved (IndexedDB) before it is sent, and stays queued until
 * PikaTimer acknowledges it, so reads survive a network drop, a reload, or
 * the browser being closed. Resending a read is harmless: PikaTimer drops
 * a read it already has (same bib, same time).
 */

export const APP_VERSION = '1.2';

const SEND_BATCH = 50;
const FETCH_TIMEOUT_MS = 8000;
const HEARTBEAT_MS = 10000;
const BIB_REFRESH_MS = 60000;
const CLOCK_SAMPLES = 8;
// A sync with a round trip longer than this is too vague to use on its own
const CLOCK_MAX_RTT_MS = 2000;

/** Monotonic-ish epoch milliseconds (same clock as video frame times) */
export function deviceNow() {
    return performance.timeOrigin + performance.now();
}

export class PikaLink extends EventTarget {
    /**
     * @param token the input's token (from the link)
     * @param device {id, name}
     */
    constructor(token, device) {
        super();
        this.token = token;
        this.device = device;
        this.base = `/api/select/${encodeURIComponent(token)}`;

        // Connection state
        this.online = false;
        this.lastContact = 0;
        this.problem = '';          // why we can't send (shown to the user)
        this.linkInvalid = false;
        this.enabled = true;
        this.config = null;

        // Clock: serverTime = deviceTime + offset
        this.samples = [];
        this.offset = loadNumber(`pika-select-offset`, 0);
        this.rtt = null;
        this.clockSynced = false;

        // Bibs
        this.bibs = null;
        this.participants = {};     // bib -> {name, age, sex, race}
        this.bibVersion = null;
        this.bibsUpdated = 0;

        // Queue
        this.store = new ReadStore(`pika-select-${token}`);
        this.queue = [];            // reads waiting to be sent, oldest first
        this.sending = false;
        this.backoffMs = 0;
        this.retryTimer = null;

        this.loadCachedBibs();
    }

    async start() {
        await this.store.open();
        const all = await this.store.all();
        all.sort((a, b) => a.epochMs - b.epochMs);
        this.queue = all.filter(r => r.status === 'queued');
        this.emit('reads', { reads: all });
        this.emit('queue');

        await this.fetchConfig();
        this.heartbeat();
        this.refreshBibs();
        setInterval(() => this.heartbeat(), HEARTBEAT_MS);
        setInterval(() => this.refreshBibs(), BIB_REFRESH_MS);
        window.addEventListener('online', () => { this.backoffMs = 0; this.heartbeat(); this.kick(); });
        this.kick();
    }

    emit(type, detail = {}) {
        this.dispatchEvent(new CustomEvent(type, { detail }));
    }

    // ------------------------------------------------------------------
    // Requests
    // ------------------------------------------------------------------

    params() {
        const p = new URLSearchParams({
            device: this.device.id,
            name: this.device.name,
            pending: String(this.queue.length),
            v: APP_VERSION
        });
        if (this.clockSynced) {
            p.set('offset', String(Math.round(this.offset)));
            if (this.rtt !== null) p.set('rtt', String(Math.round(this.rtt)));
        }
        return p;
    }

    async request(path, init = {}) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
        try {
            const res = await fetch(`${this.base}${path}?${this.params()}`, { cache: 'no-store', ...init, signal: controller.signal });
            this.noteResponse(res);
            return res;
        } catch (ex) {
            this.setOffline(ex.name === 'AbortError' ? 'PikaTimer did not answer' : 'Unable to reach PikaTimer');
            throw ex;
        } finally {
            clearTimeout(timer);
        }
    }

    noteResponse(res) {
        const wasOnline = this.online;
        this.online = true;
        this.lastContact = Date.now();
        if (res.status === 404) {
            this.linkInvalid = true;
            this.problem = 'This link is no longer valid. The input may have been deleted or given a new link in PikaTimer.';
        } else {
            this.linkInvalid = false;
            if (res.status === 503) this.problem = 'PikaTimer is not accepting reads right now (no event open, or the input is stopped).';
            else if (res.ok || res.status === 304) this.problem = '';
        }
        if (!wasOnline) this.emit('status');
        this.emit('status');
    }

    setOffline(reason) {
        this.online = false;
        this.problem = reason;
        this.emit('status');
    }

    async fetchConfig() {
        try {
            const res = await this.request('/config');
            if (!res.ok) return;
            this.applySettings(await res.json());
        } catch (ex) {
            // offline, the heartbeat will keep trying
        }
    }

    applySettings(o) {
        const before = JSON.stringify(this.config);
        this.config = { ...(this.config ?? {}), ...o };
        delete this.config.serverTime;
        if (typeof o.enabled === 'boolean') this.enabled = o.enabled;
        if (JSON.stringify(this.config) !== before) this.emit('config', this.config);
    }

    // Clock sync and heartbeat in one: NTP style, keep the sample with the
    // shortest round trip from the last few
    async heartbeat() {
        const t0 = deviceNow();
        let res;
        try {
            res = await this.request('/time');
        } catch (ex) {
            return;
        }
        const t1 = deviceNow();
        if (!res.ok) return;
        const o = await res.json();
        const rtt = t1 - t0;
        if (rtt <= CLOCK_MAX_RTT_MS || !this.clockSynced) {
            this.samples.push({ offset: o.serverTime - (t0 + t1) / 2, rtt, at: t1 });
            if (this.samples.length > CLOCK_SAMPLES) this.samples.shift();
            const best = this.samples.reduce((a, b) => (b.rtt < a.rtt ? b : a));
            this.offset = best.offset;
            this.rtt = best.rtt;
            this.clockSynced = true;
            saveNumber(`pika-select-offset`, this.offset);
            this.emit('clock');
        }
        this.applySettings(o);
        if (this.enabled) this.kick();
    }

    /** Device time to PikaTimer time */
    toServerTime(deviceMs) {
        return deviceMs + this.offset;
    }

    // ------------------------------------------------------------------
    // Bibs
    // ------------------------------------------------------------------

    loadCachedBibs() {
        try {
            const saved = JSON.parse(localStorage.getItem(`pika-select-bibs-${this.token}`) ?? 'null');
            if (saved && Array.isArray(saved.bibs)) {
                this.bibs = saved.bibs;
                this.participants = saved.participants ?? {};
                this.bibVersion = saved.version;
                this.bibsUpdated = saved.updated ?? 0;
            }
        } catch (ex) {
            // no storage
        }
    }

    async refreshBibs(manual = false) {
        try {
            const headers = {};
            if (this.bibVersion && this.bibs && !manual) headers['If-None-Match'] = `"${this.bibVersion}"`;
            const res = await this.request('/bibs', { headers });
            if (res.status === 304) {
                this.bibsUpdated = Date.now();
                this.emit('bibs', { changed: false });
                return true;
            }
            if (!res.ok) return false;
            const o = await res.json();
            this.bibs = o.bibs;
            this.participants = o.participants ?? {};
            this.bibVersion = o.version;
            this.bibsUpdated = Date.now();
            try {
                localStorage.setItem(`pika-select-bibs-${this.token}`,
                        JSON.stringify({ bibs: this.bibs, participants: this.participants, version: this.bibVersion, updated: this.bibsUpdated }));
            } catch (ex) {
                // no storage, keep it in memory
            }
            this.emit('bibs', { changed: true });
            return true;
        } catch (ex) {
            return false;
        }
    }

    /** The runner with this bib ({name, age, sex, race}), or null */
    participant(bib) {
        return Object.hasOwn(this.participants, bib) ? this.participants[bib] : null;
    }

    /** The length of the longest registered all-digit bib (6 if none are known) */
    maxBibLength() {
        if (this.maxBib?.bibs !== this.bibs) {
            let max = 0;
            for (const b of this.bibs ?? []) if (/^[0-9]+$/.test(b) && b.length > max) max = b.length;
            this.maxBib = { bibs: this.bibs, length: max || 6 };
        }
        return this.maxBib.length;
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    /**
     * Queue a read: saved first, then sent.
     *
     * @param read {bib, deviceEpochMs, score, manual}
     */
    async addRead(read) {
        const r = {
            id: newId(),
            bib: read.bib,
            deviceEpochMs: Math.round(read.deviceEpochMs),
            epochMs: Math.round(this.toServerTime(read.deviceEpochMs)),
            clockSynced: this.clockSynced,
            score: read.score,
            manual: read.manual === true,
            status: 'queued',
            participant: null,
            created: Date.now()
        };
        await this.store.put(r);
        this.queue.push(r);
        this.emit('read', { read: r });
        this.emit('queue');
        this.kick();
        return r;
    }

    // Send now unless we're already sending or waiting out a backoff
    kick() {
        if (this.sending || this.retryTimer || this.queue.length === 0) return;
        this.sendBatch();
    }

    retryLater(ms) {
        this.backoffMs = ms;
        clearTimeout(this.retryTimer);
        this.retryTimer = setTimeout(() => {
            this.retryTimer = null;
            this.kick();
        }, ms);
    }

    async sendBatch() {
        this.sending = true;
        const batch = this.queue.slice(0, SEND_BATCH);
        try {
            const body = JSON.stringify({
                deviceId: this.device.id,
                deviceName: this.device.name,
                reads: batch.map(r => ({ id: r.id, bib: r.bib, epochMs: r.epochMs, deviceEpochMs: r.deviceEpochMs, score: r.score }))
            });
            const res = await this.request('/reads', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
            if (res.status === 503) {
                const o = await res.json().catch(() => ({}));
                if (o.enabled === false) this.enabled = false;
                this.emit('status');
                this.retryLater(5000);
                return;
            }
            if (res.status === 404) {
                this.retryLater(30000);
                return;
            }
            if (!res.ok) {
                this.retryLater(Math.min(15000, Math.max(1000, this.backoffMs * 2)));
                return;
            }
            const o = await res.json();
            if (typeof o.enabled === 'boolean') this.enabled = o.enabled;
            const byId = new Map(batch.map(r => [r.id, r]));
            const done = new Set();
            for (const ack of o.acks ?? []) {
                const r = byId.get(ack.id);
                if (!r) continue;
                r.status = ack.status === 'duplicate' ? 'ok' : ack.status;
                r.participant = ack.participant ?? null;
                r.sent = Date.now();
                done.add(r.id);
                await this.store.put(r);
                this.emit('read', { read: r });
            }
            this.queue = this.queue.filter(r => !done.has(r.id));
            this.backoffMs = 0;
            this.emit('queue');
            if (done.size === 0 && batch.length > 0) {
                // Nothing was taken (the input was stopped part way through)
                this.retryLater(5000);
            }
        } catch (ex) {
            // Network trouble: back off 1, 2, 4 ... 15 seconds
            this.retryLater(Math.min(15000, Math.max(1000, this.backoffMs * 2)));
        } finally {
            this.sending = false;
        }
        if (!this.retryTimer && this.queue.length > 0) this.kick();
    }

    async clearSent() {
        await this.store.removeWhere(r => r.status !== 'queued');
    }
}

// ----------------------------------------------------------------------
// IndexedDB store for the reads (falls back to memory if unavailable)
// ----------------------------------------------------------------------

class ReadStore {
    constructor(name) {
        this.name = name;
        this.db = null;
        this.memory = new Map();
        this.persistent = false;
    }

    open() {
        return new Promise(resolve => {
            let req;
            try {
                req = indexedDB.open(this.name, 1);
            } catch (ex) {
                resolve(false);
                return;
            }
            req.onupgradeneeded = () => {
                const db = req.result;
                if (!db.objectStoreNames.contains('reads')) db.createObjectStore('reads', { keyPath: 'id' });
            };
            req.onsuccess = () => {
                this.db = req.result;
                this.persistent = true;
                resolve(true);
            };
            req.onerror = () => resolve(false);
            req.onblocked = () => resolve(false);
        });
    }

    tx(mode, fn) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction('reads', mode);
            const result = fn(tx.objectStore('reads'));
            tx.oncomplete = () => resolve(result?.result ?? result);
            tx.onerror = () => reject(tx.error);
            tx.onabort = () => reject(tx.error);
        });
    }

    async put(read) {
        this.memory.set(read.id, read);
        if (!this.db) return;
        try {
            await this.tx('readwrite', s => s.put(read));
        } catch (ex) {
            console.warn('Unable to save a read', ex);
        }
    }

    async all() {
        if (!this.db) return [...this.memory.values()];
        try {
            const list = await this.tx('readonly', s => s.getAll());
            for (const r of list) this.memory.set(r.id, r);
            return list;
        } catch (ex) {
            return [...this.memory.values()];
        }
    }

    async removeWhere(test) {
        const list = await this.all();
        const doomed = list.filter(test);
        for (const r of doomed) this.memory.delete(r.id);
        if (!this.db || doomed.length === 0) return;
        await this.tx('readwrite', s => doomed.forEach(r => s.delete(r.id)));
    }
}

// ----------------------------------------------------------------------

export function newId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    const b = crypto.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const h = [...b].map(x => x.toString(16).padStart(2, '0')).join('');
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function loadNumber(key, def) {
    try {
        const v = parseFloat(localStorage.getItem(key));
        return Number.isFinite(v) ? v : def;
    } catch (ex) {
        return def;
    }
}

function saveNumber(key, v) {
    try {
        localStorage.setItem(key, String(v));
    } catch (ex) {
        // no storage
    }
}
