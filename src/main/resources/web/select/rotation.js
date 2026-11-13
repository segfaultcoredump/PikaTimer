/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * Picture rotation: turning the camera picture upright when the phone is
 * sideways but the screen isn't (e.g. rotation lock is on).
 *
 * Rotations are clockwise degrees applied to the camera frame: 0, 90, 180, 270.
 * The "view" is the frame after the rotation. Rectangles are normalized
 * (0..1) as [x0, y0, x1, y1].
 */

export function normalizeAngle(a) {
    return ((Math.round(a) % 360) + 360) % 360;
}

/** The size of the view for a frame of w x h */
export function viewSize(w, h, rotation) {
    return (rotation === 90 || rotation === 270) ? { w: h, h: w } : { w, h };
}

/** A rectangle in the view to the same rectangle in the camera frame */
export function viewToRaw(rotation, [x0, y0, x1, y1]) {
    switch (rotation) {
        case 90: return [y0, 1 - x1, y1, 1 - x0];
        case 180: return [1 - x1, 1 - y1, 1 - x0, 1 - y0];
        case 270: return [1 - y1, x0, 1 - y0, x1];
        default: return [x0, y0, x1, y1];
    }
}

/**
 * Which way is up, from a deviceorientation event.
 *
 * @returns {angle, strength}: angle is how far the phone is turned
 * counter-clockwise from upright (0 upright, 90 top to the left, 180 upside
 * down, 270 top to the right). strength is near 0 when the phone lies flat
 * (no useful answer).
 */
export function deviceUp(beta, gamma) {
    const b = beta * Math.PI / 180, g = gamma * Math.PI / 180;
    // "up" in the phone's own coordinates (x to the right, y to the top)
    const ux = -Math.sin(g) * Math.cos(b);
    const uy = Math.sin(b);
    return { angle: normalizeAngle(Math.atan2(ux, uy) * 180 / Math.PI), strength: Math.hypot(ux, uy) };
}

/**
 * The rotation that makes the picture upright. The browser already turns
 * the picture to match the screen, so only the difference between the
 * phone and the screen matters.
 *
 * @param screenAngle the screen's angle (screen.orientation.angle)
 * @param deviceAngle the phone's angle from deviceUp, in steps of 90
 */
export function uprightRotation(screenAngle, deviceAngle) {
    return normalizeAngle(screenAngle - deviceAngle);
}

/**
 * Smooths the phone's angle into steps of 90 degrees, so a tilted tripod or
 * a bump doesn't flip the picture back and forth.
 */
export class OrientationFilter {
    constructor({ margin = 20, holdMs = 1000, minStrength = 0.5 } = {}) {
        this.margin = margin;
        this.holdMs = holdMs;
        this.minStrength = minStrength;
        this.current = null;
        this.candidate = null;
        this.candidateSince = 0;
    }

    /**
     * @returns the angle (0, 90, 180, 270), or null if not known yet
     */
    update(angle, strength, now) {
        if (strength < this.minStrength) return this.current;
        const nearest = normalizeAngle(Math.round(angle / 90) * 90);
        if (this.current === null) {
            this.current = nearest;
            return this.current;
        }
        if (nearest === this.current) {
            this.candidate = null;
            return this.current;
        }
        // Must be well past the halfway point, and stay there
        const off = Math.abs(((angle - this.current + 540) % 360) - 180);
        if (off < 45 + this.margin) {
            this.candidate = null;
            return this.current;
        }
        if (this.candidate !== nearest) {
            this.candidate = nearest;
            this.candidateSince = now;
        } else if (now - this.candidateSince >= this.holdMs) {
            this.current = nearest;
            this.candidate = null;
        }
        return this.current;
    }
}
