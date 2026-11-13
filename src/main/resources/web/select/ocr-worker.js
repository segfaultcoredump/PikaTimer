/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * Runs the OCR off the page's main thread.
 *
 * In:  {type: 'init'}
 *      {type: 'ocr', id, width, height, buffer (RGBA ArrayBuffer), detLimit}
 * Out: {type: 'progress', what, loaded, total}
 *      {type: 'ready', threads}
 *      {type: 'result', id, items, timing}
 *      {type: 'error', id?, message}
 */
import * as ort from './lib/ort/ort.wasm.bundle.min.mjs';
import { PaddleOCR } from './ocr.js';

const LIB = new URL('./lib/', import.meta.url);
const MODELS = {
    det: new URL('models/ppocrv4_det_mobile.onnx', LIB),
    rec: new URL('models/en_ppocrv4_rec_mobile.onnx', LIB),
    dict: new URL('models/en_dict.txt', LIB)
};

// Size of the runtime + models, for the progress bar
const EXPECTED_BYTES = 14848852 + 4745517 + 7653044;

let ocr = null;

self.onmessage = async (e) => {
    const msg = e.data;
    if (msg.type === 'init') {
        try {
            await init();
        } catch (ex) {
            self.postMessage({ type: 'error', message: `Unable to start the OCR: ${ex?.message ?? ex}` });
        }
    } else if (msg.type === 'ocr') {
        if (!ocr) {
            self.postMessage({ type: 'error', id: msg.id, message: 'OCR not ready' });
            return;
        }
        try {
            const image = { data: new Uint8ClampedArray(msg.buffer), width: msg.width, height: msg.height };
            const result = await ocr.run(image, msg.detLimit || 960);
            self.postMessage({ type: 'result', id: msg.id, items: result.items, timing: result.timing });
        } catch (ex) {
            self.postMessage({ type: 'error', id: msg.id, message: String(ex?.message ?? ex) });
        }
    }
};

async function init() {
    if (ocr) {
        self.postMessage({ type: 'ready', threads: ort.env.wasm.numThreads });
        return;
    }
    // Threads need a cross origin isolated page (SharedArrayBuffer)
    const cores = self.navigator?.hardwareConcurrency || 2;
    ort.env.wasm.numThreads = self.crossOriginIsolated ? Math.max(1, Math.min(4, cores - 1)) : 1;
    ort.env.wasm.wasmPaths = new URL('ort/', LIB).href;

    // The runtime and models are big; fetch them with progress
    const progress = new Progress();
    const [wasm, det, rec, dict] = await Promise.all([
        fetchWithProgress(new URL('ort/ort-wasm-simd-threaded.wasm', LIB), progress),
        fetchWithProgress(MODELS.det, progress),
        fetchWithProgress(MODELS.rec, progress),
        fetch(MODELS.dict).then(r => r.text())
    ]);
    ort.env.wasm.wasmBinary = wasm.buffer;
    self.postMessage({ type: 'progress', what: 'Starting the OCR engine', loaded: 0, total: 0 });
    try {
        ocr = await PaddleOCR.create(ort, det, rec, dict);
    } catch (ex) {
        if (ort.env.wasm.numThreads === 1) throw ex;
        // Some browsers refuse the threaded setup; try again with one thread
        ort.env.wasm.numThreads = 1;
        ocr = await PaddleOCR.create(ort, det, rec, dict);
    }
    self.postMessage({ type: 'ready', threads: ort.env.wasm.numThreads });
}

// Download progress across all of the files (the sizes are only a guide:
// compressed transfers report the compressed size)
class Progress {
    constructor() {
        this.loaded = 0;
        this.lastPost = 0;
    }

    add(bytes) {
        this.loaded += bytes;
        const t = Date.now();
        if (t - this.lastPost > 250) {
            this.lastPost = t;
            self.postMessage({ type: 'progress', what: 'Downloading the OCR engine', loaded: this.loaded, total: EXPECTED_BYTES });
        }
    }
}

async function fetchWithProgress(url, progress) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url.pathname}: HTTP ${res.status}`);
    if (!res.body || !res.body.getReader) {
        const data = new Uint8Array(await res.arrayBuffer());
        progress.add(data.length);
        return data;
    }
    const reader = res.body.getReader();
    const chunks = [];
    let loaded = 0;
    for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        loaded += value.length;
        progress.add(value.length);
    }
    const out = new Uint8Array(loaded);
    let p = 0;
    for (const c of chunks) {
        out.set(c, p);
        p += c.length;
    }
    return out;
}
