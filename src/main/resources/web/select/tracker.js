/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * Turns the OCR results from a stream of frames into bib reads.
 *
 * A runner is seen in several frames while crossing the band. Text found at
 * about the same place in consecutive frames is grouped into a track, and
 * each frame's reading is a vote. A track is reported once one bib has
 * enough votes, with the time of the track's first frame (when the bib first
 * entered the band). Votes also settle partial reads ("123" vs "1234").
 *
 * Coordinates are 0..1 within the band.
 */

export const DEFAULTS = {
    requiredHits: 2,          // votes needed to report a bib
    quickScore: 0.97,         // a known bib read this well is reported on the first vote
    minScore: 0.6,            // OCR score below this is ignored
    trackTimeoutMs: 1500,     // a track ends when it isn't seen for this long
    lonelyScore: 0.9,         // a track that ends with one vote is reported if its score is this good (known bibs only)
    repeatMs: 60000,          // ignore the same bib for this long after reporting it
    knownBibsOnly: true
};

// What a bib looks like: up to 2 letters, 1 to 6 digits, up to 2 letters
const BIB_SHAPE = /^[A-Z]{0,2}[0-9]{1,6}[A-Z]{0,2}$/;

/**
 * Clean up OCR text into a possible bib, or null.
 */
export function cleanBib(text) {
    if (!text) return null;
    const t = text.toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!t || t.length > 10) return null;
    return BIB_SHAPE.test(t) ? t : null;
}

/**
 * Prominence filter: the bib number is the biggest text on a bib. Numbers
 * that are much smaller than a different number right next to them (the
 * event year, a sponsor's logo, a corral or wave number) are marked small.
 *
 * The big numbers seen in the last moment are remembered too, so a frame
 * where the bib number was missed (blur) doesn't let the year through.
 * The filter runs before the registered bib check, so a bib number that
 * isn't registered yet still hides the small numbers around it.
 *
 * Items: {text, score, cx, cy, h} with the center and the text height in
 * pixels of the (upright) picture. Sets item.small = true.
 */
export class ProminenceFilter {
    constructor({ ratio = 0.6, memoryMs = 700, reachX = 2.0, reachY = 1.8, minScore = 0.6 } = {}) {
        this.ratio = ratio;         // smaller than this times a nearby number: small
        this.memoryMs = memoryMs;
        this.reachX = reachX;       // "nearby", in heights of the bigger number
        this.reachY = reachY;
        this.minScore = minScore;
        this.recent = [];           // {bib, cx, cy, h, time}
    }

    apply(time, items) {
        this.recent = this.recent.filter(r => time - r.time <= this.memoryMs);
        const numbers = [];
        for (const it of items) {
            it.small = false;
            it.bib = cleanBib(it.text);
            if (it.bib && it.score >= this.minScore && it.h > 0) {
                numbers.push({ bib: it.bib, cx: it.cx, cy: it.cy, h: it.h, time });
            }
        }
        const others = [...numbers, ...this.recent];
        for (const it of items) {
            if (!it.bib) continue;
            it.small = others.some(o => o.bib !== it.bib
                    && it.h < this.ratio * o.h
                    && Math.abs(it.cx - o.cx) < this.reachX * o.h
                    && Math.abs(it.cy - o.cy) < this.reachY * o.h);
        }
        this.recent.push(...numbers);
        return items;
    }

    reset() {
        this.recent = [];
    }
}

export class BibTracker {
    constructor(options = {}) {
        this.options = { ...DEFAULTS, ...options };
        this.tracks = [];
        this.lastReported = new Map();   // bib -> time reported
        this.knownBibs = null;           // Set of upper case bibs, or null if unknown
        this.bibCase = new Map();        // upper case -> the bib as registered
        this.nextId = 1;
    }

    setOptions(options) {
        Object.assign(this.options, options);
    }

    /**
     * @param bibs the registered bibs (array), or null
     */
    setKnownBibs(bibs) {
        if (!bibs) {
            this.knownBibs = null;
            this.bibCase.clear();
            return;
        }
        this.knownBibs = new Set();
        this.bibCase.clear();
        for (const b of bibs) {
            const u = String(b).toUpperCase();
            this.knownBibs.add(u);
            this.bibCase.set(u, String(b));
        }
    }

    isKnown(bib) {
        return this.knownBibs !== null && this.knownBibs.has(bib);
    }

    /**
     * Add one frame's OCR results.
     *
     * @param time the frame's capture time (ms)
     * @param items [{text, score, box: [x0, y0, x1, y1]}], box in 0..1 band coordinates
     * @returns the reads to report: [{bib, time, score, hits, known}]
     */
    addFrame(time, items) {
        const o = this.options;
        const filterKnown = o.knownBibsOnly && this.knownBibs !== null;
        const candidates = [];
        for (const item of items) {
            if (item.score < o.minScore) continue;
            const bib = cleanBib(item.text);
            if (!bib) continue;
            if (filterKnown && !this.knownBibs.has(bib)) continue;
            candidates.push({ bib, score: item.score, box: item.box });
        }

        const reads = [];

        // End the tracks that have left the band (before matching, so a
        // new runner isn't added to an old track)
        this.tracks = this.tracks.filter(t => {
            if (time - t.lastTime <= o.trackTimeoutMs) return true;
            if (!t.reported) {
                const [bib, v] = leader(t);
                if (bib && this.isKnown(bib) && v.scoreSum / v.hits >= o.lonelyScore) {
                    this.report(bib, t, v, true, reads);
                }
            }
            return false;
        });

        // Match each candidate to the nearest live track
        const used = new Set();
        for (const c of candidates) {
            let best = null, bestDist = Infinity;
            for (const t of this.tracks) {
                if (used.has(t)) continue;
                const d = boxDistance(t.box, c.box);
                if (d < bestDist) { bestDist = d; best = t; }
            }
            if (best === null || bestDist > 1.5) {
                best = { id: this.nextId++, firstTime: time, lastTime: time, box: c.box, votes: new Map(), reported: false };
                this.tracks.push(best);
            }
            used.add(best);
            best.lastTime = time;
            best.box = c.box;
            const v = best.votes.get(c.bib) ?? { hits: 0, scoreSum: 0, best: 0 };
            v.hits++;
            v.scoreSum += c.score;
            v.best = Math.max(v.best, c.score);
            best.votes.set(c.bib, v);
        }

        // Report the tracks that have enough votes
        for (const t of this.tracks) {
            if (t.reported) continue;
            const [bib, v] = leader(t);
            if (!bib) continue;
            const known = this.isKnown(bib);
            if (v.hits >= o.requiredHits || (known && v.best >= o.quickScore)) {
                t.reported = true;
                this.report(bib, t, v, known, reads);
            }
        }

        return reads;
    }

    report(bib, track, votes, known, reads) {
        const last = this.lastReported.get(bib);
        if (last !== undefined && track.firstTime - last < this.options.repeatMs) return;
        this.lastReported.set(bib, track.firstTime);
        reads.push({
            bib: this.bibCase.get(bib) ?? bib,
            time: track.firstTime,
            score: votes.scoreSum / votes.hits,
            hits: votes.hits,
            known
        });
    }

    /**
     * Note a bib reported earlier (e.g. before the page was reloaded) so a
     * repeat isn't reported again.
     */
    noteReported(bib, time) {
        const key = String(bib).toUpperCase();
        const last = this.lastReported.get(key);
        if (last === undefined || time > last) this.lastReported.set(key, time);
    }

    /** Drop the runners being followed (e.g. the picture was turned), keeping the repeat suppression */
    clearTracks() {
        this.tracks = [];
    }

    /** Forget the repeat suppression (e.g. a new wave) */
    resetRepeats() {
        this.lastReported.clear();
    }
}

// The bib with the most votes (ties go to the better score)
function leader(track) {
    let bestBib = null, best = null;
    for (const [bib, v] of track.votes) {
        if (best === null || v.hits > best.hits || (v.hits === best.hits && v.scoreSum > best.scoreSum)) {
            bestBib = bib;
            best = v;
        }
    }
    return [bestBib, best];
}

// Distance between two box centers, in units of the larger box's size
function boxDistance(a, b) {
    const ax = (a[0] + a[2]) / 2, ay = (a[1] + a[3]) / 2;
    const bx = (b[0] + b[2]) / 2, by = (b[1] + b[3]) / 2;
    const size = Math.max(a[2] - a[0], a[3] - a[1], b[2] - b[0], b[3] - b[1], 0.02);
    return Math.hypot(ax - bx, ay - by) / size;
}
