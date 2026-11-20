/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * Keypad entry: a person at the line types each bib and presses Enter. The
 * time of the read is the moment Enter was pressed.
 *
 * Only digits are accepted, up to the length of the longest registered
 * (all-digit) bib. A bib that isn't registered is still recorded, in case a
 * runner has a bib that was missed at packet pickup.
 */
import { deviceNow } from './net.js';

const $ = (id) => document.getElementById(id);

/** A runner's name for display */
export function runnerName(p) {
    return p ? (p.name || `${p.first ?? ''} ${p.last ?? ''}`.trim() || '—') : '';
}

/** A runner's age, sex and race (when there is more than one) for display */
export function runnerDetail(p) {
    if (!p) return '';
    return [p.age != null && p.age !== 0 ? `Age ${p.age}` : '', p.sex || '', p.race || ''].filter(Boolean).join(' · ');
}

export class ManualEntry {
    /**
     * @param link the PikaLink (registered bibs, runners, the read queue)
     * @param recentRead (bib) => the newest read of this bib on this device, or null
     * @param canType () => true when key presses should go to the keypad
     */
    constructor({ link, recentRead, canType }) {
        this.link = link;
        this.recentRead = recentRead;
        this.canType = canType;
        this.entry = '';
        this.last = null;           // the last read entered, for the confirmation line
        this.active = false;

        const keypad = $('keypad');
        // Act on the press, not the release: quicker, and the time is when
        // the finger hits Enter
        keypad.addEventListener('pointerdown', (e) => {
            const key = e.target.closest('button')?.dataset.key;
            if (!key) return;
            e.preventDefault();     // no focus, so a keyboard Enter can't also "click" it
            this.press(key, e.timeStamp);
        });
        // Clicks from a screen reader or switch control (no pointer press)
        keypad.addEventListener('click', (e) => {
            const key = e.target.closest('button')?.dataset.key;
            if (key && e.detail === 0) this.press(key, e.timeStamp);
        });
        document.addEventListener('keydown', (e) => this.onKey(e));
    }

    activate() {
        this.active = true;
        this.render();
    }

    deactivate() {
        this.active = false;
    }

    onKey(e) {
        if (!this.active || e.ctrlKey || e.metaKey || e.altKey || !this.canType()) return;
        const t = e.target;
        if (t && (t.tagName === 'INPUT' || t.tagName === 'SELECT' || t.tagName === 'TEXTAREA')) return;
        let key = null;
        if (/^[0-9]$/.test(e.key)) key = e.key;
        else if (e.key === 'Backspace') key = 'back';
        else if (e.key === 'Delete' || e.key === 'Escape' || e.key === 'Clear') key = 'clear';
        else if (e.key === 'Enter') key = e.repeat ? null : 'enter';
        else return;
        e.preventDefault();
        if (key) this.press(key, e.timeStamp);
    }

    press(key, timeStamp) {
        if (key === 'enter') {
            this.submit(timeStamp);
        } else if (key === 'back') {
            this.entry = this.entry.slice(0, -1);
        } else if (key === 'clear') {
            this.entry = '';
        } else if (/^[0-9]$/.test(key)) {
            if (this.entry.length >= this.link.maxBibLength()) {
                this.reject();
                return;
            }
            this.entry += key;
        }
        this.render();
    }

    async submit(timeStamp) {
        if (!this.entry) {
            this.reject();
            return;
        }
        // The event's time is on the performance.now() clock; use it unless
        // it looks wrong (some old browsers used the epoch)
        const now = performance.now();
        const at = timeStamp > 0 && timeStamp <= now && now - timeStamp < 5000 ? timeStamp : now;
        const bib = this.entry;
        const deviceEpochMs = performance.timeOrigin + at;
        this.entry = '';
        this.last = { bib, deviceEpochMs };
        this.render();
        const el = $('entry-box');
        el.classList.remove('sent');
        void el.offsetWidth;        // restart the animation
        el.classList.add('sent');
        await this.link.addRead({ bib, deviceEpochMs, score: null, manual: true });
    }

    reject() {
        const el = $('entry');
        el.classList.remove('reject');
        void el.offsetWidth;
        el.classList.add('reject');
        if (navigator.vibrate) navigator.vibrate(40);
    }

    /** Show the entry and who it is (call again when the bib list changes) */
    render() {
        const max = this.link.maxBibLength();
        $('entry-text').textContent = this.entry;
        $('entry').classList.toggle('empty', !this.entry);
        $('entry').classList.toggle('full', this.entry.length >= max);
        const who = $('entry-who'), detail = $('entry-detail');
        who.className = '';

        if (!this.entry) {
            if (this.last) {
                const p = this.link.participant(this.last.bib);
                who.className = 'done';
                who.textContent = `✓ ${this.last.bib} recorded at ${formatTime(this.link.toServerTime(this.last.deviceEpochMs))}`;
                detail.textContent = p ? [runnerName(p), runnerDetail(p)].filter(Boolean).join(' · ') : 'Not registered';
            } else {
                who.textContent = 'Type a bib number, then press Enter';
                detail.textContent = `Up to ${max} digit${max === 1 ? '' : 's'}. The time is taken when Enter is pressed.`;
            }
            return;
        }

        const p = this.link.participant(this.entry);
        const parts = [];
        if (p) {
            who.textContent = runnerName(p);
            parts.push(runnerDetail(p));
        } else {
            who.className = 'warn';
            who.textContent = 'Not registered';
            parts.push('It will still be recorded');
        }
        // A bib entered a moment ago: just a heads up, it can be sent again
        const repeatMs = (this.link.config?.repeatSeconds ?? 60) * 1000;
        const prev = this.recentRead(this.entry);
        if (prev && repeatMs > 0) {
            const ago = deviceNow() - prev.deviceEpochMs;
            if (ago >= 0 && ago < repeatMs) parts.push(`Already entered ${Math.round(ago / 1000)} s ago`);
        }
        detail.textContent = parts.filter(Boolean).join(' · ');
    }
}

const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

export function formatTime(epochMs) {
    const d = new Date(epochMs);
    return `${timeFormat.format(d)}.${Math.floor(d.getMilliseconds() / 100)}`;
}
