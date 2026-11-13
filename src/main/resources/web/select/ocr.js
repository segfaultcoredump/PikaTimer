/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * PaddleOCR (PP-OCRv4 mobile) text detection + recognition on top of
 * onnxruntime-web. Pure JS image handling (no canvas) so it runs the same
 * in a worker on any browser, and in node for testing.
 *
 * Images are { data: Uint8ClampedArray RGBA, width, height }.
 */

// Detection (DBNet) settings, the PaddleOCR defaults
const DET_THRESH = 0.3;       // pixel probability to count as text
const DET_BOX_THRESH = 0.6;   // mean probability for a box to be kept
const DET_UNCLIP = 1.5;       // grow the shrunk text region back out
const DET_MIN_SIZE = 3;       // smallest box side (detection map pixels)
const DET_MAX_BOXES = 24;

// Recognition (SVTR/CTC) settings
const REC_HEIGHT = 48;
const REC_MIN_WIDTH = 320;
const REC_MAX_WIDTH = 960;
const REC_BATCH = 8;

// PaddleOCR normalizes in the order cv2 loads images: B, G, R
const DET_MEAN = [0.406, 0.456, 0.485];
const DET_STD = [0.225, 0.224, 0.229];

export class PaddleOCR {
    /**
     * @param ort the onnxruntime-web module
     * @param det detection model (ArrayBuffer / Uint8Array)
     * @param rec recognition model
     * @param dictText the recognition model's character list, one per line
     * @param options onnxruntime session options
     */
    static async create(ort, det, rec, dictText, options = {}) {
        const sessionOptions = { executionProviders: ['wasm'], graphOptimizationLevel: 'all', ...options };
        const detSession = await ort.InferenceSession.create(det, sessionOptions);
        const recSession = await ort.InferenceSession.create(rec, sessionOptions);
        return new PaddleOCR(ort, detSession, recSession, parseDict(dictText));
    }

    constructor(ort, detSession, recSession, chars) {
        this.ort = ort;
        this.det = detSession;
        this.rec = recSession;
        this.chars = chars;
    }

    /**
     * Find and read the text in an image.
     *
     * @param image RGBA image
     * @param detLimit longest side of the image for detection
     * @returns {items: [{text, score, box: [x0, y0, x1, y1]}], timing}
     */
    async run(image, detLimit = 960) {
        const t0 = now();
        const boxes = await this.detect(image, detLimit);
        const t1 = now();
        const items = boxes.length ? await this.recognize(image, boxes) : [];
        const t2 = now();
        return { items, timing: { det: t1 - t0, rec: t2 - t1, boxes: boxes.length } };
    }

    async detect(image, limit) {
        const { width, height } = image;
        const scale = Math.min(1, limit / Math.max(width, height));
        const dw = Math.max(32, Math.round(width * scale / 32) * 32);
        const dh = Math.max(32, Math.round(height * scale / 32) * 32);

        const input = new Float32Array(3 * dw * dh);
        resizeInto(image, 0, 0, width, height, dw, dh, input, dw, dh, (b, g, r, i, plane) => {
            input[i] = (b / 255 - DET_MEAN[0]) / DET_STD[0];
            input[i + plane] = (g / 255 - DET_MEAN[1]) / DET_STD[1];
            input[i + 2 * plane] = (r / 255 - DET_MEAN[2]) / DET_STD[2];
        });

        const feeds = { [this.det.inputNames[0]]: new this.ort.Tensor('float32', input, [1, 3, dh, dw]) };
        const out = await this.det.run(feeds);
        const map = out[this.det.outputNames[0]];
        const [mh, mw] = map.dims.slice(-2);
        const boxes = dbPostProcess(map.data, mw, mh);

        // Back to image coordinates
        const sx = width / mw, sy = height / mh;
        return boxes.map(b => ({
            x0: clamp(Math.round(b.x0 * sx), 0, width - 1),
            y0: clamp(Math.round(b.y0 * sy), 0, height - 1),
            x1: clamp(Math.round(b.x1 * sx), 0, width - 1),
            y1: clamp(Math.round(b.y1 * sy), 0, height - 1),
            score: b.score
        })).filter(b => b.x1 - b.x0 >= 4 && b.y1 - b.y0 >= 4);
    }

    async recognize(image, boxes) {
        const items = [];
        for (let start = 0; start < boxes.length; start += REC_BATCH) {
            const batch = boxes.slice(start, start + REC_BATCH);
            // One width for the batch, like PaddleOCR (padded with zeros)
            let width = REC_MIN_WIDTH;
            const widths = batch.map(b => {
                const w = b.x1 - b.x0 + 1, h = b.y1 - b.y0 + 1;
                const rw = Math.min(REC_MAX_WIDTH, Math.max(8, Math.ceil(REC_HEIGHT * w / h)));
                width = Math.max(width, rw);
                return rw;
            });
            width = Math.min(REC_MAX_WIDTH, Math.ceil(width / 8) * 8);

            const plane = REC_HEIGHT * width;
            const input = new Float32Array(batch.length * 3 * plane);
            batch.forEach((b, n) => {
                const offset = n * 3 * plane;
                const rw = Math.min(widths[n], width);
                resizeInto(image, b.x0, b.y0, b.x1 - b.x0 + 1, b.y1 - b.y0 + 1, rw, REC_HEIGHT, input, width, REC_HEIGHT,
                    (bl, g, r, i) => {
                        input[offset + i] = bl / 127.5 - 1;
                        input[offset + i + plane] = g / 127.5 - 1;
                        input[offset + i + 2 * plane] = r / 127.5 - 1;
                    });
            });

            const feeds = { [this.rec.inputNames[0]]: new this.ort.Tensor('float32', input, [batch.length, 3, REC_HEIGHT, width]) };
            const out = await this.rec.run(feeds);
            const probs = out[this.rec.outputNames[0]];
            const [, steps, classes] = probs.dims;
            for (let n = 0; n < batch.length; n++) {
                const { text, score } = ctcDecode(probs.data, n * steps * classes, steps, classes, this.chars);
                const b = batch[n];
                items.push({ text, score, detScore: b.score, box: [b.x0, b.y0, b.x1, b.y1] });
            }
        }
        return items;
    }
}

/**
 * The recognition model's characters. Index 0 is the CTC blank, and
 * PaddleOCR adds a space at the end.
 */
export function parseDict(text) {
    const lines = text.replace(/\r/g, '').split('\n');
    if (lines.length && lines[lines.length - 1] === '') lines.pop();
    return ['', ...lines, ' '];
}

/**
 * Greedy CTC decode of one sequence.
 */
export function ctcDecode(data, offset, steps, classes, chars) {
    let text = '';
    let total = 0, count = 0, last = -1;
    for (let t = 0; t < steps; t++) {
        const base = offset + t * classes;
        let best = 0, bestP = data[base];
        for (let c = 1; c < classes; c++) {
            const p = data[base + c];
            if (p > bestP) { bestP = p; best = c; }
        }
        if (best !== 0 && best !== last) {
            text += chars[best] ?? '';
            total += bestP;
            count++;
        }
        last = best;
    }
    return { text, score: count ? total / count : 0 };
}

/**
 * DBNet post processing: threshold the probability map, find the connected
 * regions, score them, and grow each box back out (the model is trained on
 * shrunk text regions). Boxes are axis aligned, which is fine for bibs.
 */
export function dbPostProcess(prob, w, h) {
    const n = w * h;
    const labels = new Int32Array(n);   // 0 = not visited
    const stack = new Int32Array(n);
    const boxes = [];
    let label = 0;

    for (let start = 0; start < n; start++) {
        if (labels[start] !== 0 || prob[start] <= DET_THRESH) continue;
        label++;
        let x0 = w, y0 = h, x1 = -1, y1 = -1, sum = 0, count = 0;
        let sp = 0;
        stack[sp++] = start;
        labels[start] = label;
        while (sp > 0) {
            const i = stack[--sp];
            const x = i % w, y = (i - x) / w;
            sum += prob[i];
            count++;
            if (x < x0) x0 = x;
            if (x > x1) x1 = x;
            if (y < y0) y0 = y;
            if (y > y1) y1 = y;
            // 4 connected neighbours
            if (x > 0 && labels[i - 1] === 0 && prob[i - 1] > DET_THRESH) { labels[i - 1] = label; stack[sp++] = i - 1; }
            if (x < w - 1 && labels[i + 1] === 0 && prob[i + 1] > DET_THRESH) { labels[i + 1] = label; stack[sp++] = i + 1; }
            if (y > 0 && labels[i - w] === 0 && prob[i - w] > DET_THRESH) { labels[i - w] = label; stack[sp++] = i - w; }
            if (y < h - 1 && labels[i + w] === 0 && prob[i + w] > DET_THRESH) { labels[i + w] = label; stack[sp++] = i + w; }
        }
        const bw = x1 - x0 + 1, bh = y1 - y0 + 1;
        if (Math.min(bw, bh) < DET_MIN_SIZE) continue;
        const score = sum / count;
        if (score < DET_BOX_THRESH) continue;

        // Unclip: offset = area * ratio / perimeter
        const d = (bw * bh * DET_UNCLIP) / (2 * (bw + bh));
        boxes.push({
            x0: Math.max(0, x0 - d), y0: Math.max(0, y0 - d),
            x1: Math.min(w - 1, x1 + 1 + d), y1: Math.min(h - 1, y1 + 1 + d),
            score, area: bw * bh
        });
    }
    boxes.sort((a, b) => b.area - a.area);
    return boxes.slice(0, DET_MAX_BOXES);
}

/**
 * Bilinear resize of a region of an RGBA image. For each output pixel
 * set(b, g, r, index, plane) is called, index = y * outStride + x.
 */
function resizeInto(image, sx, sy, sw, sh, dw, dh, _out, outStride, outHeight, set) {
    const { data, width, height } = image;
    const plane = outStride * outHeight;
    const fx = sw / dw, fy = sh / dh;
    for (let y = 0; y < dh; y++) {
        let syf = sy + (y + 0.5) * fy - 0.5;
        if (syf < 0) syf = 0;
        let y0 = Math.floor(syf);
        if (y0 > height - 1) y0 = height - 1;
        const y1 = Math.min(y0 + 1, height - 1);
        const wy = syf - y0;
        for (let x = 0; x < dw; x++) {
            let sxf = sx + (x + 0.5) * fx - 0.5;
            if (sxf < 0) sxf = 0;
            let x0 = Math.floor(sxf);
            if (x0 > width - 1) x0 = width - 1;
            const x1 = Math.min(x0 + 1, width - 1);
            const wx = sxf - x0;
            const a = (y0 * width + x0) * 4, b = (y0 * width + x1) * 4;
            const c = (y1 * width + x0) * 4, d = (y1 * width + x1) * 4;
            const w00 = (1 - wx) * (1 - wy), w01 = wx * (1 - wy), w10 = (1 - wx) * wy, w11 = wx * wy;
            const r = data[a] * w00 + data[b] * w01 + data[c] * w10 + data[d] * w11;
            const g = data[a + 1] * w00 + data[b + 1] * w01 + data[c + 1] * w10 + data[d + 1] * w11;
            const bl = data[a + 2] * w00 + data[b + 2] * w01 + data[c + 2] * w10 + data[d + 2] * w11;
            set(bl, g, r, y * outStride + x, plane);
        }
    }
}

function clamp(v, lo, hi) {
    return v < lo ? lo : v > hi ? hi : v;
}

function now() {
    return (typeof performance !== 'undefined') ? performance.now() : Date.now();
}
