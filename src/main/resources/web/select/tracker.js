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
    knownBibsOnly: true,
    // When the OCR is slow a runner is only seen once or twice, and moves
    // a long way between frames: follow them further and for longer, and
    // trust a single good read of a registered bib more
    slowFrameMs: 700,         // frames further apart than this are slow
    slowQuickScore: 0.9,
    slowLonelyScore: 0.8
};

const MATCH_DISTANCE = 1.5;       // box sizes a runner can move between frames (at 500 ms)
const MAX_MATCH_DISTANCE = 6;

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
        this.frameMs = 0;                // average time between frames, 0 if unknown
    }

    setOptions(options) {
        Object.assign(this.options, options);
    }

    /** The average time between the frames being read (ms) */
    setFrameInterval(ms) {
        this.frameMs = ms > 0 ? ms : 0;
    }

    // The settings, adjusted for the frame rate
    limits() {
        const o = this.options;
        const slow = this.frameMs > o.slowFrameMs;
        return {
            timeoutMs: Math.max(o.trackTimeoutMs, 2.5 * this.frameMs),
            distance: Math.min(MAX_MATCH_DISTANCE, Math.max(MATCH_DISTANCE, MATCH_DISTANCE * this.frameMs / 500)),
            quickScore: slow ? Math.min(o.quickScore, o.slowQuickScore) : o.quickScore,
            lonelyScore: slow ? Math.min(o.lonelyScore, o.slowLonelyScore) : o.lonelyScore
        };
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
        const lim = this.limits();
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
            if (time - t.lastTime <= lim.timeoutMs) return true;
            if (!t.reported) {
                const [bib, v] = leader(t);
                if (bib && this.isKnown(bib) && v.scoreSum / v.hits >= lim.lonelyScore) {
                    this.report(bib, t, v, true, reads);
                }
            }
            return false;
        });

        // Match the candidates to the live tracks: a track already reading
        // the same bib first, then the nearest. A different number only
        // joins a track close by, and only if it could be a misread of the
        // same bib; otherwise it's the next runner coming through the same
        // spot, who needs their own track (and time).
        const pairs = [];
        candidates.forEach((c, ci) => {
            for (const t of this.tracks) {
                const d = boxDistance(t.box, c.box);
                const same = t.votes.has(c.bib);
                if (same ? d <= MAX_MATCH_DISTANCE : d <= lim.distance && couldBeSame(leader(t)[0], c.bib)) {
                    pairs.push({ ci, t, d, same });
                }
            }
        });
        pairs.sort((p, q) => (q.same - p.same) || (p.d - q.d));
        const used = new Set(), matched = new Map();
        for (const p of pairs) {
            if (used.has(p.t) || matched.has(p.ci)) continue;
            used.add(p.t);
            matched.set(p.ci, p.t);
        }
        candidates.forEach((c, ci) => {
            let t = matched.get(ci);
            if (!t) {
                t = { id: this.nextId++, firstTime: time, lastTime: time, box: c.box, votes: new Map(), reported: false };
                this.tracks.push(t);
            }
            t.lastTime = time;
            t.box = c.box;
            const v = t.votes.get(c.bib) ?? { hits: 0, scoreSum: 0, best: 0 };
            v.hits++;
            v.scoreSum += c.score;
            v.best = Math.max(v.best, c.score);
            t.votes.set(c.bib, v);
        });

        // Report the tracks that have enough votes
        for (const t of this.tracks) {
            if (t.reported) continue;
            const [bib, v] = leader(t);
            if (!bib) continue;
            const known = this.isKnown(bib);
            if (v.hits >= o.requiredHits || (known && v.best >= lim.quickScore)) {
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

// Could two readings be the same bib: one is part of the other (a partial
// read), or they differ in one character (a misread)
function couldBeSame(a, b) {
    if (a === null || b === null) return true;
    if (a.includes(b) || b.includes(a)) return true;
    if (a.length !== b.length) return false;
    let diff = 0;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i] && ++diff > 1) return false;
    return true;
}

// Distance between two box centers, in units of the larger box's size
function boxDistance(a, b) {
    const ax = (a[0] + a[2]) / 2, ay = (a[1] + a[3]) / 2;
    const bx = (b[0] + b[2]) / 2, by = (b[1] + b[3]) / 2;
    const size = Math.max(a[2] - a[0], a[3] - a[1], b[2] - b[0], b[3] - b[1], 0.02);
    return Math.hypot(ax - bx, ay - by) / size;
}
