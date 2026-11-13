/*
 * Copyright (C) 2026 John Garner
 * GPLv3, see the PikaTimer LICENSE.
 *
 * PikaTimer automatic select timer: watch the finish line with the camera,
 * read the bibs, and send them to PikaTimer.
 */
import { PikaLink, APP_VERSION, newId } from './net.js';
import { BibTracker, ProminenceFilter, cleanBib } from './tracker.js';
import { viewSize, viewToRaw, deviceUp, uprightRotation, normalizeAngle, OrientationFilter } from './rotation.js';

const RECENT_READS = 10;
// Largest side of the image sent to the OCR (the band plus a margin)
const MAX_CROP = 1600;
// Read a little outside the band so bibs on the edge are whole, but only
// count text whose center is inside the band
const BAND_MARGIN = 0.15;
const DEFAULT_BAND = { landscape: { x: 0.05, y: 0.3, w: 0.9, h: 0.4 }, portrait: { x: 0.05, y: 0.35, w: 0.9, h: 0.3 } };

const $ = (id) => document.getElementById(id);

// ----------------------------------------------------------------------
// Settings (per device) and identity
// ----------------------------------------------------------------------

const token = decodeURIComponent(location.pathname.replace(/^\/select\/([^/]+).*$/, '$1'));

function load(key, def) {
    try {
        const v = localStorage.getItem(key);
        return v === null ? def : JSON.parse(v);
    } catch (ex) {
        return def;
    }
}

function save(key, value) {
    try {
        localStorage.setItem(key, JSON.stringify(value));
    } catch (ex) {
        // private mode, no storage
    }
}

function defaultDeviceName(id) {
    const ua = navigator.userAgent;
    const kind = /iPad/.test(ua) ? 'iPad' : /iPhone/.test(ua) ? 'iPhone' : /Android/.test(ua) ? 'Android'
            : /Macintosh/.test(ua) && navigator.maxTouchPoints > 1 ? 'iPad' : 'Device';
    return `${kind} ${id.slice(0, 4).toUpperCase()}`;
}

let deviceId = load('pika-select-device-id', null);
if (!deviceId) {
    deviceId = newId();
    save('pika-select-device-id', deviceId);
}
const settings = {
    name: defaultDeviceName(deviceId),
    cameraId: '',
    requiredHits: 2,
    detLimit: 960,
    showBoxes: true,
    rotation: 'auto',           // 'auto' or 0, 90, 180, 270 (clockwise)
    prominence: true,           // ignore small numbers next to a bigger one
    ...load('pika-select-settings', {})
};
const saveSettings = () => save('pika-select-settings', settings);

// The band is kept separately for a landscape and a portrait picture
let bandShape = 'landscape';
let band = loadBand(bandShape);

function loadBand(shape) {
    const saved = load(`pika-select-band-${token}-${shape}`, null)
            ?? (shape === 'landscape' ? load(`pika-select-band-${token}`, null) : null);
    return { ...DEFAULT_BAND[shape], ...(saved ?? {}) };
}

function saveBand() {
    save(`pika-select-band-${token}-${bandShape}`, band);
}

// ----------------------------------------------------------------------
// State
// ----------------------------------------------------------------------

const device = { id: deviceId, name: settings.name };
const link = new PikaLink(token, device);
const tracker = new BibTracker({ requiredHits: settings.requiredHits });
const prominence = new ProminenceFilter();

const video = $('video');
const overlay = $('overlay');
const overlayCtx = overlay.getContext('2d');
const cropCanvas = document.createElement('canvas');
const cropCtx = cropCanvas.getContext('2d', { willReadFrequently: true });

let worker = null;
let ocrReady = false;
let busy = false;
let paused = false;
let stream = null;
let fileUrl = null;
let frameId = 0;
let lastResult = null;           // {items, crop, at} for the overlay
const frameTimes = [];           // ms per OCR frame, for the stats
let wakeLock = null;
let reads = [];                  // all reads we know about, newest first
const freshIds = new Set();      // reads to flash in the list

// Picture rotation
let autoRotation = 0;            // from the motion sensor
let appliedRotation = null;      // what the display and capture are using
let sensorState = 'waiting';     // waiting, in use, not allowed, not available
let hintDismissed = false;
let sensorRecheck = null;
const orientationFilter = new OrientationFilter();

// ----------------------------------------------------------------------
// OCR worker
// ----------------------------------------------------------------------

function startWorker() {
    try {
        worker = new Worker(new URL('./ocr-worker.js', import.meta.url), { type: 'module' });
    } catch (ex) {
        stageMessage(`This browser can't run the OCR (${ex.message}). Use a recent Safari or Chrome.`);
        return;
    }
    worker.onmessage = (e) => onWorkerMessage(e.data);
    worker.onerror = (e) => stageMessage(`The OCR stopped: ${e.message ?? 'unknown error'}`);
    worker.postMessage({ type: 'init' });
    stageMessage('Loading the OCR engine…', 0);
}

function onWorkerMessage(msg) {
    switch (msg.type) {
        case 'progress':
            if (!ocrReady) {
                const pct = msg.total ? Math.min(99, Math.round(100 * msg.loaded / msg.total)) : null;
                const mb = (msg.loaded / 1048576).toFixed(1);
                stageMessage(`${msg.what}… ${msg.loaded ? `${mb} MB` : ''}`, pct);
            }
            break;
        case 'ready':
            ocrReady = true;
            $('ocr-stats').textContent = msg.threads > 1 ? `OCR ready (${msg.threads} threads)` : 'OCR ready';
            if (stream || fileUrl) hideStageMessage();
            else showStartButton();
            break;
        case 'result':
            busy = false;
            onOcrResult(msg);
            break;
        case 'error':
            busy = false;
            if (!ocrReady) stageMessage(msg.message);
            else console.warn('OCR error', msg.message);
            break;
    }
}

// ----------------------------------------------------------------------
// Camera
// ----------------------------------------------------------------------

function cameraAvailable() {
    return window.isSecureContext && navigator.mediaDevices && navigator.mediaDevices.getUserMedia;
}

async function startCamera() {
    if (!cameraAvailable()) {
        stageMessage(window.isSecureContext
            ? 'This browser does not allow camera access.'
            : 'The camera only works on a secure page. Open the https:// link from PikaTimer.');
        return;
    }
    stopSource();
    const constraints = {
        audio: false,
        video: {
            width: { ideal: 1920 },
            height: { ideal: 1080 },
            frameRate: { ideal: 30 }
        }
    };
    if (settings.cameraId) constraints.video.deviceId = { exact: settings.cameraId };
    else constraints.video.facingMode = { ideal: 'environment' };

    try {
        stream = await navigator.mediaDevices.getUserMedia(constraints);
    } catch (ex) {
        if (settings.cameraId && (ex.name === 'OverconstrainedError' || ex.name === 'NotFoundError')) {
            settings.cameraId = '';
            saveSettings();
            return startCamera();
        }
        stageMessage(ex.name === 'NotAllowedError'
            ? 'Camera access was blocked. Allow the camera for this site in the browser settings, then reload.'
            : `Unable to start the camera: ${ex.message}`);
        showStartButton();
        return;
    }
    video.srcObject = stream;
    await video.play().catch(() => {});
    const track = stream.getVideoTracks()[0];
    setupTrack(track);
    listCameras();
    sourceStarted();
}

async function setupTrack(track) {
    const caps = track.getCapabilities ? track.getCapabilities() : {};
    // Keep the focus and exposure following the scene
    const advanced = [];
    if (caps.focusMode && caps.focusMode.includes('continuous')) advanced.push({ focusMode: 'continuous' });
    if (caps.exposureMode && caps.exposureMode.includes('continuous')) advanced.push({ exposureMode: 'continuous' });
    if (advanced.length) track.applyConstraints({ advanced }).catch(() => {});

    const zoom = $('zoom');
    if (caps.zoom && caps.zoom.max > caps.zoom.min) {
        zoom.min = caps.zoom.min;
        zoom.max = caps.zoom.max;
        zoom.step = caps.zoom.step || 0.1;
        zoom.value = track.getSettings().zoom ?? caps.zoom.min;
        $('zoom-wrap').hidden = false;
        zoom.oninput = () => track.applyConstraints({ advanced: [{ zoom: Number(zoom.value) }] }).catch(() => {});
    } else {
        $('zoom-wrap').hidden = true;
    }
}

async function listCameras() {
    const select = $('camera-select');
    try {
        const devices = (await navigator.mediaDevices.enumerateDevices()).filter(d => d.kind === 'videoinput');
        const current = stream?.getVideoTracks()[0]?.getSettings().deviceId;
        select.innerHTML = '';
        devices.forEach((d, i) => {
            const opt = document.createElement('option');
            opt.value = d.deviceId;
            opt.textContent = d.label || `Camera ${i + 1}`;
            if (d.deviceId === current) opt.selected = true;
            select.appendChild(opt);
        });
    } catch (ex) {
        // leave it empty
    }
}

function startFile(file) {
    stopSource();
    fileUrl = URL.createObjectURL(file);
    video.srcObject = null;
    video.src = fileUrl;
    video.loop = true;
    video.play().catch(() => {});
    $('zoom-wrap').hidden = true;
    sourceStarted();
}

function stopSource() {
    if (stream) {
        stream.getTracks().forEach(t => t.stop());
        stream = null;
    }
    if (fileUrl) {
        URL.revokeObjectURL(fileUrl);
        fileUrl = null;
        video.removeAttribute('src');
        video.load();
    }
}

function sourceStarted() {
    $('start-button').hidden = true;
    $('pause-button').disabled = false;
    if (ocrReady) hideStageMessage();
    else stageMessage('Loading the OCR engine…');
    $('band').hidden = false;
    requestWakeLock();
    const go = () => {
        applyRotation();
        scheduleFrame();
    };
    if (video.readyState >= 1) go();
    else video.addEventListener('loadedmetadata', go, { once: true });
}

async function requestWakeLock() {
    try {
        if ('wakeLock' in navigator && document.visibilityState === 'visible') {
            wakeLock = await navigator.wakeLock.request('screen');
        }
    } catch (ex) {
        // not supported or not allowed
    }
}

// ----------------------------------------------------------------------
// Frames
// ----------------------------------------------------------------------

let frameLoopRunning = false;

function scheduleFrame() {
    if (frameLoopRunning) return;
    frameLoopRunning = true;
    const next = () => {
        if (video.requestVideoFrameCallback) video.requestVideoFrameCallback(onFrame);
        else requestAnimationFrame((t) => onFrame(t, null));
    };
    function onFrame(now, meta) {
        next();
        try {
            processFrame(now, meta);
        } catch (ex) {
            console.warn('Frame error', ex);
            busy = false;
        }
    }
    next();
}

// The band plus a margin: in the view (0..1), and the same area of the
// camera frame (pixels)
function cropRect() {
    const R = currentRotation();
    const mx = band.w * BAND_MARGIN, my = band.h * BAND_MARGIN;
    const view = [Math.max(0, band.x - mx), Math.max(0, band.y - my),
        Math.min(1, band.x + band.w + mx), Math.min(1, band.y + band.h + my)];
    const [u0, v0, u1, v1] = viewToRaw(R, view);
    const vw = video.videoWidth, vh = video.videoHeight;
    const raw = { sx: Math.round(u0 * vw), sy: Math.round(v0 * vh), sw: Math.round((u1 - u0) * vw), sh: Math.round((v1 - v0) * vh) };
    const size = viewSize(vw, vh, R);
    return { R, view, raw, viewW: (view[2] - view[0]) * size.w, viewH: (view[3] - view[1]) * size.h };
}

function processFrame(now, meta) {
    if (busy || paused || !ocrReady || !video.videoWidth || video.paused && !stream) return;
    // When the frame was captured, on the same clock as performance.now()
    const captured = meta?.captureTime ?? meta?.presentationTime ?? now;
    const deviceEpochMs = performance.timeOrigin + captured;

    const c = cropRect();
    if (c.raw.sw < 16 || c.raw.sh < 16) return;
    // The crop is drawn upright (rotated), the way the timer sees it
    const scale = Math.min(1, MAX_CROP / Math.max(c.viewW, c.viewH));
    const cw = Math.round(c.viewW * scale), ch = Math.round(c.viewH * scale);
    if (cropCanvas.width !== cw || cropCanvas.height !== ch) {
        cropCanvas.width = cw;
        cropCanvas.height = ch;
    }
    const sideways = c.R === 90 || c.R === 270;
    const dw = sideways ? ch : cw, dh = sideways ? cw : ch;
    cropCtx.setTransform(1, 0, 0, 1, 0, 0);
    cropCtx.translate(cw / 2, ch / 2);
    cropCtx.rotate(c.R * Math.PI / 180);
    cropCtx.drawImage(video, c.raw.sx, c.raw.sy, c.raw.sw, c.raw.sh, -dw / 2, -dh / 2, dw, dh);
    cropCtx.setTransform(1, 0, 0, 1, 0, 0);
    const image = cropCtx.getImageData(0, 0, cw, ch);

    busy = true;
    const id = ++frameId;
    pending.set(id, { deviceEpochMs, crop: c, cw, ch, rotation: c.R, started: performance.now() });
    worker.postMessage({ type: 'ocr', id, width: cw, height: ch, buffer: image.data.buffer, detLimit: settings.detLimit },
            [image.data.buffer]);
}

const pending = new Map();

function onOcrResult(msg) {
    const frame = pending.get(msg.id);
    pending.delete(msg.id);
    if (!frame) return;
    frameTimes.push(performance.now() - frame.started);
    if (frameTimes.length > 20) frameTimes.shift();
    updateStats();
    // The picture was turned while this frame was being read
    if (frame.rotation !== currentRotation()) return;

    // Crop pixels -> 0..1 in the view; keep text centered in the band
    const [vx0, vy0, vx1, vy1] = frame.crop.view;
    const toView = (x, y) => [vx0 + x / frame.cw * (vx1 - vx0), vy0 + y / frame.ch * (vy1 - vy0)];
    const vd = viewDims();
    const found = msg.items.map(it => {
        const [x0, y0] = toView(it.box[0], it.box[1]);
        const [x1, y1] = toView(it.box[2], it.box[3]);
        // center and height in pixels, for the prominence filter
        return { text: it.text, score: it.score, box: [x0, y0, x1, y1],
            cx: (x0 + x1) / 2 * vd.w, cy: (y0 + y1) / 2 * vd.h, h: (y1 - y0) * vd.h };
    });
    // Drop the year, sponsor logos and such: numbers much smaller than a
    // bib number right next to them
    if (settings.prominence) prominence.apply(frame.deviceEpochMs, found);

    const items = [];
    const shown = [];
    for (const it of found) {
        const [x0, y0, x1, y1] = it.box;
        const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
        const inBand = cx >= band.x && cx <= band.x + band.w && cy >= band.y && cy <= band.y + band.h;
        const small = settings.prominence && it.small;
        const bib = cleanBib(it.text);
        const known = !tracker.options.knownBibsOnly || tracker.knownBibs === null || (bib !== null && tracker.knownBibs.has(bib));
        const accepted = inBand && bib !== null && known && !small;
        shown.push({ text: it.text, score: it.score, box: it.box, accepted, small: small && inBand });
        if (inBand && !small) {
            items.push({
                text: it.text,
                score: it.score,
                box: [(x0 - band.x) / band.w, (y0 - band.y) / band.h, (x1 - band.x) / band.w, (y1 - band.y) / band.h]
            });
        }
    }
    lastResult = { items: shown, at: performance.now() };
    drawOverlay();

    for (const read of tracker.addFrame(frame.deviceEpochMs, items)) {
        link.addRead({ bib: read.bib, deviceEpochMs: read.time, score: read.score });
    }
}

function updateStats() {
    if (!frameTimes.length) return;
    const avg = frameTimes.reduce((a, b) => a + b, 0) / frameTimes.length;
    $('ocr-stats').textContent = `${(1000 / avg).toFixed(1)} fps · ${Math.round(avg)} ms`;
}

// ----------------------------------------------------------------------
// Picture rotation
// ----------------------------------------------------------------------

function currentRotation() {
    return settings.rotation === 'auto' ? autoRotation : Number(settings.rotation);
}

// The size of the picture as shown (after the rotation)
function viewDims() {
    return viewSize(video.videoWidth || 16, video.videoHeight || 9, currentRotation());
}

// Apply a change of rotation (manual or from the sensor) to the display,
// the band and the tracker
function applyRotation() {
    const R = currentRotation();
    if (R !== appliedRotation) {
        appliedRotation = R;
        tracker.clearTracks();
        prominence.reset();
        lastResult = null;
    }
    const v = viewDims();
    const shape = v.w >= v.h ? 'landscape' : 'portrait';
    if (shape !== bandShape) {
        bandShape = shape;
        band = loadBand(shape);
    }
    layoutVideo();
    layoutBand();
    renderRotation();
}

function screenAngle() {
    const a = screen.orientation?.angle ?? window.orientation ?? 0;
    return normalizeAngle(a);
}

function onDeviceOrientation(e) {
    if (e.beta === null || e.gamma === null || e.beta === undefined || e.gamma === undefined) return;
    if (sensorState !== 'in use') {
        sensorState = 'in use';
        renderRotation();
    }
    const up = deviceUp(e.beta, e.gamma);
    const deviceAngle = orientationFilter.update(up.angle, up.strength, performance.now());
    // A turn has to hold for a moment; check again then in case the sensor
    // only reports changes
    if (orientationFilter.candidate !== null) {
        clearTimeout(sensorRecheck);
        sensorRecheck = setTimeout(() => onDeviceOrientation({ beta: e.beta, gamma: e.gamma }), orientationFilter.holdMs + 50);
    }
    if (deviceAngle === null) return;
    const r = uprightRotation(screenAngle(), deviceAngle);
    if (r !== autoRotation) {
        autoRotation = r;
        if (settings.rotation === 'auto') applyRotation();
    }
}

function onScreenOrientation() {
    if (orientationFilter.current === null) return;
    autoRotation = uprightRotation(screenAngle(), orientationFilter.current);
    applyRotation();
}

// iOS only sends motion events after the user allows it (needs a tap)
function requestSensor() {
    const DOE = window.DeviceOrientationEvent;
    if (!DOE) {
        sensorState = 'not available';
        renderRotation();
        return;
    }
    if (typeof DOE.requestPermission === 'function') {
        DOE.requestPermission().then(answer => {
            if (answer !== 'granted') {
                sensorState = 'not allowed';
                renderRotation();
            }
        }).catch(() => {
            sensorState = 'not allowed';
            renderRotation();
        });
    }
    // No readings after a few seconds: no sensor (e.g. a laptop)
    setTimeout(() => {
        if (sensorState === 'waiting') {
            sensorState = 'not available';
            renderRotation();
        }
    }, 4000);
}

function setupRotation() {
    window.addEventListener('deviceorientation', onDeviceOrientation);
    if (screen.orientation?.addEventListener) screen.orientation.addEventListener('change', onScreenOrientation);
    else window.addEventListener('orientationchange', onScreenOrientation);

    // Each tap turns the picture another quarter turn and stops the auto rotation
    $('rotate-button').onclick = () => {
        settings.rotation = (currentRotation() + 90) % 360;
        saveSettings();
        applyRotation();
    };
    $('rotate-auto').onclick = () => {
        settings.rotation = 'auto';
        saveSettings();
        requestSensor();
        applyRotation();
    };
    $('hint-close').onclick = () => {
        hintDismissed = true;
        renderRotation();
    };
}

function renderRotation() {
    const R = currentRotation();
    const auto = settings.rotation === 'auto';
    $('rotation-label').textContent = auto ? `Auto · ${R}°` : `Manual · ${R}°`;
    $('rotate-auto').hidden = auto;
    const sensor = {
        'waiting': 'waiting for the motion sensor',
        'in use': 'using the motion sensor',
        'not allowed': 'the motion sensor was not allowed, use the Rotate button',
        'not available': 'no motion sensor, use the Rotate button'
    }[sensorState];
    $('rotation-info').textContent = auto
        ? `Auto: turns the picture upright when the phone is sideways (${sensor}).`
        : 'Manual: the picture stays turned the way it was set.';
    if ($('rotation-select')) $('rotation-select').value = String(settings.rotation);

    // A tall picture can't see much of the finish line
    const v = viewDims();
    $('orient-hint').hidden = hintDismissed || !(stream || fileUrl) || v.w >= v.h;
}

// ----------------------------------------------------------------------
// Video layout, the band and the overlay
// ----------------------------------------------------------------------

// Turn the video element (CSS only, no cost per frame)
function layoutVideo() {
    const stage = $('stage');
    const v = viewDims();
    // In the narrow (portrait) layout the stage follows the picture's shape
    stage.style.setProperty('--view-aspect', String(clamp(v.w / v.h, 0.75, 21 / 9)));
    const sw = stage.clientWidth, sh = stage.clientHeight;
    const R = currentRotation();
    const sideways = R === 90 || R === 270;
    const w = sideways ? sh : sw, h = sideways ? sw : sh;
    video.style.width = `${w}px`;
    video.style.height = `${h}px`;
    video.style.left = `${(sw - w) / 2}px`;
    video.style.top = `${(sh - h) / 2}px`;
    video.style.transform = R ? `rotate(${R}deg)` : '';
}

// Where the picture is inside the stage (object-fit: contain)
function videoRect() {
    const stage = $('stage').getBoundingClientRect();
    const v = viewDims();
    const scale = Math.min(stage.width / v.w, stage.height / v.h);
    const w = v.w * scale, h = v.h * scale;
    return { x: (stage.width - w) / 2, y: (stage.height - h) / 2, w, h, stageW: stage.width, stageH: stage.height };
}

function layoutBand() {
    const r = videoRect();
    const el = $('band');
    el.style.left = `${r.x + band.x * r.w}px`;
    el.style.top = `${r.y + band.y * r.h}px`;
    el.style.width = `${band.w * r.w}px`;
    el.style.height = `${band.h * r.h}px`;
    drawOverlay();
}

function drawOverlay() {
    const r = videoRect();
    const dpr = window.devicePixelRatio || 1;
    const w = Math.round(r.stageW * dpr), h = Math.round(r.stageH * dpr);
    if (overlay.width !== w || overlay.height !== h) {
        overlay.width = w;
        overlay.height = h;
    }
    overlayCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
    overlayCtx.clearRect(0, 0, r.stageW, r.stageH);
    if (!settings.showBoxes || !lastResult || performance.now() - lastResult.at > 3000) return;
    overlayCtx.font = '600 14px -apple-system, sans-serif';
    overlayCtx.lineWidth = 2;
    for (const it of lastResult.items) {
        const x = r.x + it.box[0] * r.w, y = r.y + it.box[1] * r.h;
        const bw = (it.box[2] - it.box[0]) * r.w, bh = (it.box[3] - it.box[1]) * r.h;
        const color = it.accepted ? '#2ed3a0' : it.small ? '#ffb020' : 'rgba(255,255,255,0.55)';
        overlayCtx.strokeStyle = color;
        overlayCtx.strokeRect(x, y, bw, bh);
        const label = it.small ? `${it.text} (small)` : `${it.text} ${Math.round(it.score * 100)}%`;
        const tw = overlayCtx.measureText(label).width + 8;
        overlayCtx.fillStyle = 'rgba(0,0,0,0.7)';
        overlayCtx.fillRect(x, y - 20, tw, 20);
        overlayCtx.fillStyle = color;
        overlayCtx.fillText(label, x + 4, y - 5);
    }
}

function setupBandEditing() {
    const el = $('band');
    let drag = null;
    el.addEventListener('pointerdown', (e) => {
        e.preventDefault();
        el.setPointerCapture(e.pointerId);
        drag = { mode: e.target.dataset.handle || 'move', x: e.clientX, y: e.clientY, start: { ...band } };
    });
    el.addEventListener('pointermove', (e) => {
        if (!drag) return;
        const r = videoRect();
        const dx = (e.clientX - drag.x) / r.w, dy = (e.clientY - drag.y) / r.h;
        const s = drag.start, MIN = 0.06;
        if (drag.mode === 'move') {
            band.x = clamp(s.x + dx, 0, 1 - s.w);
            band.y = clamp(s.y + dy, 0, 1 - s.h);
        } else if (drag.mode === 'tl') {
            const x = clamp(s.x + dx, 0, s.x + s.w - MIN), y = clamp(s.y + dy, 0, s.y + s.h - MIN);
            band.w = s.w + (s.x - x);
            band.h = s.h + (s.y - y);
            band.x = x;
            band.y = y;
        } else if (drag.mode === 'br') {
            band.w = clamp(s.w + dx, MIN, 1 - s.x);
            band.h = clamp(s.h + dy, MIN, 1 - s.y);
        }
        layoutBand();
    });
    const end = () => {
        if (!drag) return;
        drag = null;
        saveBand();
    };
    el.addEventListener('pointerup', end);
    el.addEventListener('pointercancel', end);
    const relayout = () => {
        layoutVideo();
        layoutBand();
        renderRotation();
    };
    new ResizeObserver(relayout).observe($('stage'));
    // The camera's picture size changes (e.g. the screen turned)
    video.addEventListener('resize', () => applyRotation());
}

function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
}

// ----------------------------------------------------------------------
// Stage messages
// ----------------------------------------------------------------------

function stageMessage(text, progress = null) {
    $('stage-message').hidden = false;
    $('stage-text').textContent = text;
    $('progress').hidden = progress === null;
    if (progress !== null) $('progress-bar').style.width = `${progress}%`;
}

function hideStageMessage() {
    $('stage-message').hidden = true;
}

function showStartButton() {
    $('stage-message').hidden = false;
    $('progress').hidden = true;
    $('stage-text').textContent = cameraAvailable()
        ? 'Point the camera at the finish line, then set the band where the bibs pass.'
        : 'The camera only works on a secure (https) page. Open the https:// link from PikaTimer, or pick a video file in Settings.';
    $('start-button').hidden = !cameraAvailable();
}

// ----------------------------------------------------------------------
// Reads list
// ----------------------------------------------------------------------

const timeFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

function formatTime(epochMs) {
    const d = new Date(epochMs);
    return `${timeFormat.format(d)}.${Math.floor(d.getMilliseconds() / 100)}`;
}

function readState(r) {
    if (r.status === 'queued') return { cls: 'queued', text: 'Queued' };
    if (r.status === 'ok') return r.participant ? { cls: 'ok', text: 'Sent' } : { cls: 'unknown', text: 'Not registered' };
    if (r.status === 'before_event') return { cls: 'before_event', text: 'Before event date' };
    return { cls: 'invalid', text: 'Rejected' };
}

function renderReads() {
    const list = $('reads');
    list.innerHTML = '';
    const recent = reads.slice(0, RECENT_READS);
    $('no-reads').hidden = recent.length > 0;
    for (const r of recent) {
        const li = document.createElement('li');
        const state = readState(r);
        li.className = state.cls + (freshIds.has(r.id) ? ' new' : '');
        const p = r.participant;
        const who = p ? (p.name || `${p.first ?? ''} ${p.last ?? ''}`.trim() || '—')
                : (r.status === 'queued' ? 'Waiting for PikaTimer…' : '—');
        const detail = p ? [p.age != null && p.age !== 0 ? `Age ${p.age}` : '', p.sex || '', p.race || ''].filter(Boolean).join(' · ') : '';
        li.innerHTML = `<span class="bib"></span><span class="who"></span><span class="time"></span>
                        <span class="detail"></span><span class="status"></span>`;
        li.querySelector('.bib').textContent = r.bib;
        if (r.bib.length > 4) li.querySelector('.bib').classList.add('long');
        li.querySelector('.who').textContent = who;
        li.querySelector('.time').textContent = formatTime(r.epochMs);
        li.querySelector('.detail').textContent = detail;
        li.querySelector('.status').textContent = state.text;
        list.appendChild(li);
    }
    freshIds.clear();
}

function upsertRead(r) {
    const i = reads.findIndex(x => x.id === r.id);
    if (i >= 0) reads[i] = r;
    else {
        reads.push(r);
        freshIds.add(r.id);
    }
    reads.sort((a, b) => b.epochMs - a.epochMs);
    if (reads.length > 500) reads.length = 500;
    renderReads();
}

// ----------------------------------------------------------------------
// Status
// ----------------------------------------------------------------------

function renderStatus() {
    const net = $('net-pill');
    if (link.linkInvalid) {
        net.textContent = 'Invalid link';
        net.className = 'pill bad';
    } else if (!link.online) {
        net.textContent = 'Offline';
        net.className = 'pill bad';
    } else if (link.problem || !link.enabled) {
        net.textContent = 'Not accepting';
        net.className = 'pill warn';
    } else {
        net.textContent = 'Connected';
        net.className = 'pill ok';
    }

    const queued = link.queue.length;
    const q = $('queue-pill');
    q.textContent = queued ? `${queued} queued` : 'All sent';
    q.className = queued ? 'pill warn' : 'pill ok';

    const banner = $('banner');
    let text = link.problem;
    if (!text && !link.enabled) text = 'PikaTimer is not accepting reads from this input right now. Reads are kept on this device until it does.';
    if (!link.online && queued) text = `${link.problem}. ${queued} read${queued === 1 ? ' is' : 's are'} saved on this device and will be sent when the connection is back.`;
    banner.hidden = !text;
    banner.textContent = text;
    banner.className = link.linkInvalid || !link.online ? '' : 'warn';

    renderDeviceInfo();
}

function renderConfig() {
    const c = link.config ?? {};
    if (c.location) $('location-name').textContent = c.location;
    $('input-name').textContent = c.input ? `Input: ${c.input}` : '';
    document.title = c.location ? `Select Timer · ${c.location}` : 'PikaTimer Select Timer';
    tracker.setOptions({
        knownBibsOnly: c.knownBibsOnly !== false,
        repeatMs: (c.repeatSeconds ?? 60) * 1000
    });
    $('input-settings').textContent = `Set in PikaTimer: ${c.knownBibsOnly !== false ? 'only registered bibs are read' : 'any number is read'}; `
            + `a bib is ignored for ${c.repeatSeconds ?? 60} s after it is read.`;
    renderBibInfo();
}

function renderBibInfo() {
    const count = link.bibs ? link.bibs.length : 0;
    const when = link.bibsUpdated ? new Date(link.bibsUpdated).toLocaleTimeString() : 'never';
    $('bib-count').textContent = link.bibs ? `${count.toLocaleString()} bibs` : '';
    $('bib-info').textContent = link.bibs
        ? `${count.toLocaleString()} registered bibs, last checked ${when}. The list is checked every minute.`
        : 'The list of registered bibs has not been loaded yet.';
}

function renderDeviceInfo() {
    const clock = link.clockSynced
        ? `Clock: ${link.offset >= 0 ? '+' : ''}${(link.offset / 1000).toFixed(2)} s to match PikaTimer (±${Math.max(1, Math.round((link.rtt ?? 0) / 2))} ms).`
        : 'Clock: not synced with PikaTimer yet.';
    const storage = link.store.persistent ? 'Reads are saved on this device.' : 'Reads can NOT be saved on this device (private browsing?), keep this page open.';
    $('device-info').textContent = `${clock} ${storage} App ${APP_VERSION}.`;
}

// ----------------------------------------------------------------------
// Settings dialog
// ----------------------------------------------------------------------

function setupSettings() {
    const dialog = $('settings');
    $('settings-button').onclick = () => {
        $('device-name').value = settings.name;
        $('required-hits').value = String(settings.requiredHits);
        $('det-limit').value = String(settings.detLimit);
        $('show-boxes').checked = settings.showBoxes;
        $('prominence').checked = settings.prominence;
        renderBibInfo();
        renderDeviceInfo();
        if (stream) listCameras();
        dialog.showModal();
    };
    $('device-name').onchange = () => {
        settings.name = $('device-name').value.trim() || defaultDeviceName(deviceId);
        device.name = settings.name;
        saveSettings();
    };
    $('camera-select').onchange = () => {
        settings.cameraId = $('camera-select').value;
        saveSettings();
        startCamera();
    };
    $('video-file').onchange = () => {
        const f = $('video-file').files[0];
        if (f) {
            startFile(f);
            dialog.close();
        }
    };
    $('required-hits').onchange = () => {
        settings.requiredHits = Number($('required-hits').value);
        tracker.setOptions({ requiredHits: settings.requiredHits });
        saveSettings();
    };
    $('det-limit').onchange = () => {
        settings.detLimit = Number($('det-limit').value);
        saveSettings();
    };
    $('prominence').onchange = () => {
        settings.prominence = $('prominence').checked;
        prominence.reset();
        saveSettings();
    };
    $('show-boxes').onchange = () => {
        settings.showBoxes = $('show-boxes').checked;
        saveSettings();
        drawOverlay();
    };
    $('refresh-bibs').onclick = async () => {
        $('refresh-bibs').disabled = true;
        $('bib-info').textContent = 'Checking…';
        const ok = await link.refreshBibs(true);
        $('refresh-bibs').disabled = false;
        renderBibInfo();
        if (!ok) $('bib-info').textContent += ' (Unable to reach PikaTimer.)';
    };
    $('reset-band').onclick = () => {
        band = { ...DEFAULT_BAND[bandShape] };
        saveBand();
        layoutBand();
    };
    $('rotation-select').onchange = () => {
        const v = $('rotation-select').value;
        settings.rotation = v === 'auto' ? 'auto' : Number(v);
        saveSettings();
        if (v === 'auto') requestSensor();
        applyRotation();
    };
    $('reset-repeats').onclick = () => tracker.resetRepeats();
    $('clear-sent').onclick = async () => {
        await link.clearSent();
        reads = reads.filter(r => r.status === 'queued');
        renderReads();
    };
}

// ----------------------------------------------------------------------
// Start up
// ----------------------------------------------------------------------

function init() {
    setupBandEditing();
    setupSettings();
    setupRotation();
    renderRotation();

    $('start-button').onclick = () => {
        // Ask for the motion sensor in the same tap (iOS needs that)
        if (settings.rotation === 'auto') requestSensor();
        startCamera();
    };
    $('pause-button').onclick = () => {
        paused = !paused;
        $('pause-button').textContent = paused ? 'Resume' : 'Pause';
        if (paused) {
            lastResult = null;
            drawOverlay();
            $('ocr-stats').textContent = 'Paused';
        }
    };

    link.addEventListener('status', renderStatus);
    link.addEventListener('queue', renderStatus);
    link.addEventListener('clock', renderDeviceInfo);
    link.addEventListener('config', renderConfig);
    link.addEventListener('bibs', () => {
        tracker.setKnownBibs(link.bibs);
        renderBibInfo();
    });
    link.addEventListener('reads', (e) => {
        reads = e.detail.reads.slice().sort((a, b) => b.epochMs - a.epochMs);
        // After a reload, don't report the bibs this device just reported again
        for (const r of reads) tracker.noteReported(r.bib, r.deviceEpochMs);
        renderReads();
    });
    link.addEventListener('read', (e) => upsertRead(e.detail.read));

    if (link.bibs) tracker.setKnownBibs(link.bibs);
    renderConfig();
    renderStatus();

    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible' && (stream || fileUrl)) requestWakeLock();
    });
    window.addEventListener('beforeunload', (e) => {
        if (link.queue.length > 0) {
            e.preventDefault();
            e.returnValue = '';
        }
    });

    if (!token || !/^[a-z0-9]{4,32}$/.test(token)) {
        stageMessage('This link is not complete. Open the link (or scan the QR code) shown in PikaTimer.');
        return;
    }
    link.start().catch(ex => console.warn('Unable to start', ex));
    startWorker();
}

init();
