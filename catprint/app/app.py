"""
CatPrint HA — Mini Thermal Printer Service for Home Assistant
Supports GOTOOGO C15 and similar iPrint-compatible BLE thermal printers.
Provides REST API + Web UI for printing text, images, and QR codes.

v1.0.11: Single persistent BLE event loop, asyncio.Lock, retry-with-limit,
         MTU negotiation, font fallback, graceful shutdown, diag auth.
"""

import asyncio
import logging
import os
import signal
import sqlite3
import sys
import textwrap
import threading
import time
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Flask, jsonify, request, render_template, send_from_directory

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PRINTER_WIDTH_PX = 384  # 48 bytes * 8 bits — standard for 58mm thermal
BYTES_PER_ROW = PRINTER_WIDTH_PX // 8  # 48
SCAN_TIMEOUT = 10.0
CHUNK_DELAY = 0.01  # seconds between BLE chunks
JOB_MAX_RETRIES = 3  # max retry attempts per queued print job

# MXW01 BLE UUIDs (FunPrint protocol, reverse-engineered via PacketLogger)
PRINTER_CMD_UUID    = "0000ae01-0000-1000-8000-00805f9b34fb"  # init/end commands
PRINTER_DATA_UUID   = "0000ae03-0000-1000-8000-00805f9b34fb"  # raw bitmap stream
PRINTER_NOTIFY_UUID = "0000ae02-0000-1000-8000-00805f9b34fb"  # printer responses

# MXW01 protocol commands
def _mxw_init_cmd(rows: int) -> bytes:
    return bytes([0x22, 0x21, 0xA9, 0x00, 0x04, 0x00,
                  rows & 0xFF, (rows >> 8) & 0xFF,
                  0x30, 0x00, 0x00, 0x00])

MXW_END_CMD = bytes([0x22, 0x21, 0xAD, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00])

# Diagnostics endpoint auth token (set DIAG_TOKEN env var to enable protection)
DIAG_TOKEN = os.environ.get("DIAG_TOKEN", "")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("catprint")

# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------


def build_raw_bitmap(bitmap_rows: list[list[int]]) -> bytes:
    """Flatten bitmap rows into a raw byte stream for MXW01."""
    return bytes(b for row in bitmap_rows for b in row)


# ---------------------------------------------------------------------------
# Image processing (PIL-based)
# ---------------------------------------------------------------------------

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

try:
    import qrcode
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False


def text_to_image(text: str, font_size: int = 24, width: int = PRINTER_WIDTH_PX) -> "Image.Image":
    """Render text to a 1-bit PIL image suitable for the printer."""
    if not HAS_PIL:
        raise RuntimeError("Pillow is required for text rendering")

    font = None
    font_paths = [
        # macOS
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
        # Alpine Linux (apk add ttf-dejavu)
        "/usr/share/fonts/ttf-dejavu/DejaVuSans.ttf",
        # Debian/Ubuntu
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            font = ImageFont.truetype(fp, font_size)
            break
    if font is None:
        # load_default(size=) requires Pillow >= 10; fall back for older versions
        try:
            font = ImageFont.load_default(size=font_size)
        except TypeError:
            font = ImageFont.load_default()

    dummy_img = Image.new("RGB", (width, 10), "white")
    dummy_draw = ImageDraw.Draw(dummy_img)

    avg_char_w = font_size * 0.6
    chars_per_line = max(int(width / avg_char_w), 10)
    wrapped = "\n".join(textwrap.fill(line, width=chars_per_line) for line in text.split("\n"))

    bbox = dummy_draw.textbbox((0, 0), wrapped, font=font)
    text_h = bbox[3] - bbox[1] + font_size

    img = Image.new("RGB", (width, text_h + 20), "white")
    draw = ImageDraw.Draw(img)
    draw.text((10, 10), wrapped, fill="black", font=font)

    return img


def qr_to_image(data: str, width: int = PRINTER_WIDTH_PX) -> "Image.Image":
    """Generate a QR code image."""
    if not HAS_QRCODE:
        raise RuntimeError("qrcode library is required")
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    ratio = width / img.width
    img = img.resize((width, int(img.height * ratio)), Image.NEAREST)
    return img


def image_to_bitmap(img: "Image.Image") -> list[list[int]]:
    """
    Convert a PIL image to a list of rows, each row being a list of bytes.
    Each bit represents one pixel (1 = black, 0 = white).
    Width is padded/cropped to PRINTER_WIDTH_PX.
    """
    if img.width != PRINTER_WIDTH_PX:
        ratio = PRINTER_WIDTH_PX / img.width
        img = img.resize(
            (PRINTER_WIDTH_PX, max(1, int(img.height * ratio))),
            Image.LANCZOS,
        )

    img = img.convert("L")
    img = img.convert("1")  # Floyd-Steinberg dithering by default

    pixels = img.load()
    rows = []
    bytes_per_row = PRINTER_WIDTH_PX // 8

    for y in range(img.height):
        row_bytes = []
        for byte_idx in range(bytes_per_row):
            byte_val = 0
            for bit in range(8):
                x = byte_idx * 8 + bit
                if x < img.width:
                    px = pixels[x, y]
                    if px == 0:  # black pixel
                        byte_val |= 1 << bit
            row_bytes.append(byte_val)
        rows.append(row_bytes)

    return rows


# ---------------------------------------------------------------------------
# BLE Communication (using bleak)
# ---------------------------------------------------------------------------

try:
    from bleak import BleakClient, BleakScanner
    HAS_BLEAK = True
except ImportError:
    HAS_BLEAK = False

IS_LINUX = os.uname().sysname == "Linux"
BLE_CONNECT_RETRIES = 3

KNOWN_PRINTER_NAMES = ["GT01", "GB01", "GB02", "GB03", "GB04", "GT02", "C15", "MXW01"]

# _last_ble_device is set inside the BLE event loop — always accessed from there
_last_ble_device = None

printer_state = {
    "address": os.environ.get("PRINTER_ADDRESS", ""),
    "name": os.environ.get("PRINTER_NAME", ""),
    "status": "disconnected",
    "last_print": None,
    "print_count": 0,
    "error": None,
}


async def scan_for_printer(timeout: float = SCAN_TIMEOUT) -> dict | None:
    """Scan BLE for a known printer. Must be called from the BLE event loop."""
    global _last_ble_device
    if not HAS_BLEAK:
        return None

    log.info("Scanning for BLE printers...")
    devices = await BleakScanner.discover(timeout=timeout, return_adv=False)

    for device in devices:
        name = device.name or ""
        if any(known in name for known in KNOWN_PRINTER_NAMES):
            log.info(f"Found printer: {name} @ {device.address}")
            _last_ble_device = device
            return {"name": name, "address": device.address}

    log.warning("No printer found during scan")
    return None


def _reset_adapter() -> None:
    """Power-cycle the BT adapter via bluetoothctl (Linux only, best-effort)."""
    if not IS_LINUX:
        return
    import subprocess
    try:
        log.info("Resetting BT adapter (power off/on)...")
        subprocess.run(["bluetoothctl", "power", "off"], capture_output=True, timeout=5)
        time.sleep(1)
        subprocess.run(["bluetoothctl", "power", "on"], capture_output=True, timeout=5)
        time.sleep(1)
        log.info("BT adapter reset done")
    except Exception as e:
        log.debug(f"Adapter reset failed (non-critical): {e}")


def _clear_bluez_cache(addr: str) -> None:
    """Remove stale BlueZ device cache (Linux only, best-effort)."""
    if not IS_LINUX:
        return
    try:
        import subprocess
        result = subprocess.run(
            ["bluetoothctl", "remove", addr],
            capture_output=True, timeout=5, text=True,
        )
        log.debug(f"bluetoothctl remove {addr}: rc={result.returncode}")
    except Exception as e:
        log.debug(f"BlueZ cache clear failed (non-critical): {e}")


def _trust_device(addr: str) -> None:
    """Mark BLE device as trusted in BlueZ."""
    if not IS_LINUX:
        return
    import subprocess
    try:
        subprocess.run(["bluetoothctl", "trust", addr], capture_output=True, timeout=5)
        log.debug(f"bluetoothctl trust {addr}: OK")
    except Exception as e:
        log.debug(f"Trust failed (non-critical): {e}")


async def send_to_printer(bitmap_data: bytes, address: str = None,
                          max_retries: int = BLE_CONNECT_RETRIES,
                          connect_timeout: float = 60.0) -> bool:
    """Send raw bitmap data to MXW01 printer over BLE. Must run in BLE event loop."""
    global _last_ble_device
    if not HAS_BLEAK:
        raise RuntimeError("bleak library is required for BLE communication")

    addr = address or printer_state["address"]
    if not addr:
        found = await scan_for_printer()
        if not found:
            raise RuntimeError("No printer found. Please scan first or set PRINTER_ADDRESS.")
        addr = found["address"]
        printer_state["address"] = addr
        printer_state["name"] = found["name"]

    rows = len(bitmap_data) // BYTES_PER_ROW
    printer_state["status"] = "connecting"

    last_err = None
    for attempt in range(1, max_retries + 1):
        log.info(f"Connecting to printer at {addr}... ({rows} rows) [attempt {attempt}/{max_retries}]")

        if IS_LINUX and attempt > 1:
            _reset_adapter()
            _clear_bluez_cache(addr)
            await asyncio.sleep(1.0)

        if IS_LINUX:
            # Always scan on the current event loop — BLEDevice from a different
            # loop has stale D-Bus paths that cause "device not found".
            log.info("Fresh BLE scan on current event loop...")
            _last_ble_device = None
            await scan_for_printer(timeout=10.0)
            _trust_device(addr)
            await asyncio.sleep(2.0)

        connect_target = addr
        if IS_LINUX and _last_ble_device and _last_ble_device.address == addr:
            connect_target = _last_ble_device
            log.info("Using BLEDevice object for connection (BlueZ optimized)")

        try:
            async with BleakClient(connect_target, timeout=connect_timeout) as client:
                # Request larger MTU for faster transfers (BCM43455 supports up to 512)
                try:
                    await client.request_mtu(512)
                except Exception:
                    pass  # non-critical — will use negotiated default

                printer_state["status"] = "printing"
                chunk_size = max(client.mtu_size - 3, 20)
                log.info(f"Connected. MTU: {client.mtu_size}, chunk: {chunk_size}, rows: {rows}")

                ack = asyncio.Event()
                done = asyncio.Event()

                def notify_handler(sender, data):
                    log.debug(f"Printer notify: {data.hex()}")
                    if len(data) >= 3 and data[2] == 0xA9:
                        ack.set()
                    elif len(data) >= 3 and data[2] == 0xAA:
                        done.set()

                await client.start_notify(PRINTER_NOTIFY_UUID, notify_handler)
                await asyncio.sleep(0.3)

                # 1. Init — declares image dimensions
                init_cmd = _mxw_init_cmd(rows)
                log.info(f"Init: {init_cmd.hex()}")
                await client.write_gatt_char(PRINTER_CMD_UUID, init_cmd, response=False)
                await asyncio.wait_for(ack.wait(), timeout=5.0)
                log.info("Init ACK received")

                # 2. Stream raw bitmap
                for i in range(0, len(bitmap_data), chunk_size):
                    await client.write_gatt_char(PRINTER_DATA_UUID, bitmap_data[i:i+chunk_size], response=False)
                    await asyncio.sleep(CHUNK_DELAY)

                # 3. End command
                await client.write_gatt_char(PRINTER_CMD_UUID, MXW_END_CMD, response=False)
                log.info("End command sent, waiting for completion...")

                # 4. Wait for completion notification
                try:
                    await asyncio.wait_for(done.wait(), timeout=15.0)
                    log.info("Print completed (printer confirmed)")
                except asyncio.TimeoutError:
                    log.warning("No completion notification — print may still succeed")

                printer_state["status"] = "idle"
                printer_state["last_print"] = datetime.now().isoformat()
                printer_state["print_count"] += 1
                printer_state["error"] = None
                log.info("Print job completed successfully")
                return True

        except Exception as e:
            last_err = e
            log.warning(f"Attempt {attempt}/{max_retries} failed [{type(e).__name__}]: {e!r}")
            _last_ble_device = None
            if attempt < max_retries:
                await asyncio.sleep(2.0)
            continue

    printer_state["status"] = "error"
    printer_state["error"] = str(last_err) or repr(last_err)
    log.error(f"Print failed after {max_retries} attempts: {last_err!r}")
    raise last_err


# ---------------------------------------------------------------------------
# Database (SQLite — templates, history, queue)
# ---------------------------------------------------------------------------

DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "catprint.db")))


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL,
                text       TEXT    NOT NULL,
                font_size  INTEGER NOT NULL DEFAULT 24,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS print_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                type       TEXT    NOT NULL,
                summary    TEXT    NOT NULL,
                rows       INTEGER,
                font_size  INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS print_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                type        TEXT    NOT NULL,
                summary     TEXT    NOT NULL,
                bitmap_data BLOB    NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                printed_at  TIMESTAMP,
                error       TEXT
            )
        """)
        # Migration: add retry_count to existing deployments
        try:
            conn.execute("ALTER TABLE print_queue ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # column already exists
    log.info(f"Database ready at {DB_PATH}")


def log_print(type_: str, summary: str, rows: int = None, font_size: int = None) -> None:
    """Insert a record into print_history (best-effort, never raises)."""
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO print_history (type, summary, rows, font_size) VALUES (?, ?, ?, ?)",
                (type_, summary[:200], rows, font_size),
            )
    except Exception as exc:
        log.warning(f"Could not log print: {exc}")


# ---------------------------------------------------------------------------
# Print queue
# ---------------------------------------------------------------------------


def enqueue_job(type_: str, summary: str, bitmap_data: bytes) -> int:
    """Add a print job to the queue with status='pending'. Returns job id."""
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO print_queue (type, summary, bitmap_data) VALUES (?, ?, ?)",
            (type_, summary[:200], bitmap_data),
        )
        return cursor.lastrowid


def get_pending_jobs() -> list[dict]:
    """Return all pending print jobs ordered by creation time."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, type, summary, bitmap_data FROM print_queue "
            "WHERE status = 'pending' ORDER BY id ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def mark_job_printed(job_id: int) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE print_queue SET status = 'printed', printed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )


def mark_job_failed(job_id: int, error: str) -> None:
    """Increment retry_count. Move to 'failed' only after JOB_MAX_RETRIES attempts;
    otherwise keep as 'pending' so the background scanner retries automatically."""
    with get_db() as conn:
        conn.execute(
            """UPDATE print_queue
               SET retry_count = retry_count + 1,
                   status = CASE WHEN retry_count + 1 >= ? THEN 'failed' ELSE 'pending' END,
                   error = ?
               WHERE id = ?""",
            (JOB_MAX_RETRIES, error[:500], job_id),
        )


# ---------------------------------------------------------------------------
# BLE worker — single persistent event loop
# ---------------------------------------------------------------------------
#
# All BLE I/O runs in one dedicated asyncio event loop (_ble_loop) hosted in
# a background thread.  Flask routes submit coroutines via run_ble() /
# run_async() and block until results come back.  This eliminates the
# "BLEDevice from a different event loop" D-Bus errors caused by creating a
# new_event_loop() per request.
# ---------------------------------------------------------------------------

_ble_loop: asyncio.AbstractEventLoop | None = None
_ble_lock: asyncio.Lock | None = None  # asyncio.Lock — created inside _ble_loop


def run_ble(coro) -> any:
    """Submit a BLE coroutine to the dedicated loop, acquire the BLE lock, block."""
    if _ble_loop is None:
        raise RuntimeError("BLE worker not started")

    async def _locked():
        async with _ble_lock:
            return await coro

    future = asyncio.run_coroutine_threadsafe(_locked(), _ble_loop)
    return future.result(timeout=180.0)


def run_async(coro) -> any:
    """Submit any async coroutine to the dedicated loop without acquiring the lock."""
    if _ble_loop is None:
        raise RuntimeError("BLE worker not started")
    future = asyncio.run_coroutine_threadsafe(coro, _ble_loop)
    return future.result(timeout=60.0)


async def _drain_queue(address: str) -> int:
    """
    Print all pending jobs using the given BLE address.
    Must be called from within the BLE event loop (already under _ble_lock).
    Returns the number of successfully printed jobs.
    """
    jobs = get_pending_jobs()
    if not jobs:
        return 0

    log.info(f"Draining queue: {len(jobs)} pending job(s)...")
    printed = 0
    for job in jobs:
        try:
            await send_to_printer(bytes(job["bitmap_data"]), address=address,
                                  max_retries=1, connect_timeout=20.0)
            mark_job_printed(job["id"])
            log_print(job["type"], job["summary"])
            printed += 1
            log.info(f"Queue job {job['id']} ({job['type']}) printed OK")
        except Exception as e:
            mark_job_failed(job["id"], str(e) or repr(e))
            log.error(f"Queue job {job['id']} failed [{type(e).__name__}]: {e!r}")
            break  # printer likely disconnected — stop draining
    return printed


async def _scanner_loop_async() -> None:
    """Async task in BLE worker loop: scan for printer every 10 s, drain queue when found."""
    log.info("Background BLE scanner started (interval: 10 s)")
    while True:
        await asyncio.sleep(10)

        if not get_pending_jobs():
            continue

        # Skip if another BLE operation is already running
        if _ble_lock.locked():
            log.debug("Scanner: BLE busy, skipping cycle")
            continue

        async with _ble_lock:
            log.info("Scanner: pending jobs found, scanning for printer...")
            try:
                found = await scan_for_printer(timeout=8.0)
                if found:
                    printer_state["address"] = found["address"]
                    printer_state["name"] = found["name"]
                    printer_state["status"] = "idle"
                    log.info(f"Scanner: printer found at {found['address']}, draining queue...")
                    await _drain_queue(found["address"])
                    if printer_state["status"] not in ("printing", "error"):
                        printer_state["status"] = "idle"
                else:
                    log.debug("Scanner: no printer found this cycle")
            except Exception as e:
                log.warning(f"Scanner cycle error [{type(e).__name__}]: {e!r}")


def _ble_worker_thread() -> None:
    """Dedicated thread hosting the single persistent BLE event loop."""
    global _ble_loop, _ble_lock

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _ble_loop = loop

    async def _main() -> None:
        global _ble_lock
        _ble_lock = asyncio.Lock()
        if HAS_BLEAK:
            asyncio.create_task(_scanner_loop_async())
        # Keep loop alive until process exits
        await asyncio.Event().wait()

    try:
        loop.run_until_complete(_main())
    except Exception as e:
        log.error(f"BLE worker crashed: {e!r}")
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload


def require_diag_auth(f):
    """Decorator: require X-Auth-Token header (or ?token=) when DIAG_TOKEN is set."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if DIAG_TOKEN:
            token = request.headers.get("X-Auth-Token") or request.args.get("token", "")
            if token != DIAG_TOKEN:
                return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ---- Web UI ----

@app.route("/")
def index():
    return render_template("index.html")


# ---- REST API ----

@app.route("/api/status", methods=["GET"])
def api_status():
    """Get printer status."""
    return jsonify({
        "status": "ok",
        "printer": printer_state,
        "capabilities": {
            "has_pil": HAS_PIL,
            "has_qrcode": HAS_QRCODE,
            "has_bleak": HAS_BLEAK,
            "printer_width": PRINTER_WIDTH_PX,
        },
    })


@app.route("/api/scan", methods=["POST"])
def api_scan():
    """Scan for BLE printers."""
    try:
        result = run_ble(scan_for_printer())
        if result:
            printer_state["address"] = result["address"]
            printer_state["name"] = result["name"]
            printer_state["status"] = "idle"
            pending = get_pending_jobs()
            if pending:
                run_ble(_drain_queue(result["address"]))
            return jsonify({"status": "found", "printer": result, "pending_jobs": len(pending)})
        return jsonify({"status": "not_found", "message": "No printer found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/print/text", methods=["POST"])
def api_print_text():
    """Print text. Body: { "text": "...", "font_size": 24 }"""
    data = request.get_json(force=True)
    text = data.get("text", "")
    font_size = data.get("font_size", 24)

    if not text.strip():
        return jsonify({"status": "error", "message": "No text provided"}), 400

    try:
        img = text_to_image(text, font_size=font_size)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    job_id = enqueue_job("text", text[:80], bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("text", text[:80], rows=len(bitmap), font_size=font_size)
        return jsonify({"status": "ok", "rows_printed": len(bitmap), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": str(e), "job_id": job_id}), 202


@app.route("/api/print/image", methods=["POST"])
def api_print_image():
    """Print an image. Send as multipart/form-data with field 'image'."""
    if "image" not in request.files:
        return jsonify({"status": "error", "message": "No image file provided"}), 400

    file = request.files["image"]

    try:
        img = Image.open(file.stream).convert("RGB")
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    summary = file.filename or "obraz"
    job_id = enqueue_job("image", summary, bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("image", summary, rows=len(bitmap))
        return jsonify({"status": "ok", "rows_printed": len(bitmap), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": str(e), "job_id": job_id}), 202


@app.route("/api/print/qr", methods=["POST"])
def api_print_qr():
    """Print a QR code. Body: { "data": "https://..." }"""
    data = request.get_json(force=True)
    qr_data = data.get("data", "")

    if not qr_data.strip():
        return jsonify({"status": "error", "message": "No QR data provided"}), 400

    try:
        img = qr_to_image(qr_data)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    job_id = enqueue_job("qr", qr_data[:80], bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("qr", qr_data[:80], rows=len(bitmap))
        return jsonify({"status": "ok", "rows_printed": len(bitmap), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": str(e), "job_id": job_id}), 202


@app.route("/api/feed", methods=["POST"])
def api_feed():
    """Feed paper. Body: { "lines": 3 }"""
    data = request.get_json(force=True) if request.data else {}
    lines = data.get("lines", 3)
    feed_rows = max(lines * 8, 8)
    bitmap_data = bytes(BYTES_PER_ROW * feed_rows)

    try:
        run_ble(send_to_printer(bitmap_data))
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---- Templates ----

@app.route("/api/templates", methods=["GET"])
def api_get_templates():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, text, font_size, created_at FROM templates ORDER BY created_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/templates", methods=["POST"])
def api_save_template():
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    text = data.get("text", "").strip()
    font_size = int(data.get("font_size", 24))

    if not name or not text:
        return jsonify({"status": "error", "message": "Name and text are required"}), 400

    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO templates (name, text, font_size) VALUES (?, ?, ?)",
            (name, text, font_size),
        )
    return jsonify({"status": "ok", "id": cursor.lastrowid})


@app.route("/api/templates/<int:template_id>", methods=["DELETE"])
def api_delete_template(template_id):
    with get_db() as conn:
        conn.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    return jsonify({"status": "ok"})


# ---- History ----

@app.route("/api/history", methods=["GET"])
def api_get_history():
    limit = min(int(request.args.get("limit", 30)), 100)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, type, summary, rows, font_size, created_at "
            "FROM print_history ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/history", methods=["DELETE"])
def api_clear_history():
    with get_db() as conn:
        conn.execute("DELETE FROM print_history")
    return jsonify({"status": "ok"})


# ---- HA Integration helpers ----

@app.route("/api/print/ha-shopping-list", methods=["POST"])
def api_print_shopping_list():
    """
    Print a formatted shopping list.
    Body: { "items": ["Mleko", "Chleb", "Masło"], "title": "Lista zakupów" }
    """
    data = request.get_json(force=True)
    items = data.get("items", [])
    title = data.get("title", "🛒 Lista zakupów")

    if not items:
        return jsonify({"status": "error", "message": "No items provided"}), 400

    lines = [title, "─" * 30, ""]
    for item in items:
        lines.append(f"  □  {item}")
    lines += ["", "─" * 30, f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]

    text = "\n".join(lines)

    try:
        img = text_to_image(text, font_size=22)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    preview = ", ".join(items[:3]) + (f" +{len(items)-3} więcej" if len(items) > 3 else "")
    summary = f"{len(items)} pozycji: {preview}"
    job_id = enqueue_job("shopping_list", summary, bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("shopping_list", summary, rows=len(bitmap))
        return jsonify({"status": "ok", "items_count": len(items), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": str(e), "job_id": job_id}), 202


@app.route("/api/print/ha-notification", methods=["POST"])
def api_print_notification():
    """
    Print a HA-style notification.
    Body: { "title": "Alert!", "message": "...", "source": "Home Assistant" }
    """
    data = request.get_json(force=True)
    title = data.get("title", "Notification")
    message = data.get("message", "")
    source = data.get("source", "Home Assistant")

    lines = [
        "━" * 32,
        f"  ⚡ {title}",
        "━" * 32,
        "",
        message,
        "",
        "─" * 32,
        f"  {source}  •  {datetime.now().strftime('%H:%M')}",
        "",
    ]

    text = "\n".join(lines)

    try:
        img = text_to_image(text, font_size=20)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    summary = f"{title}: {message[:60]}"
    job_id = enqueue_job("notification", summary, bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("notification", summary, rows=len(bitmap))
        return jsonify({"status": "ok", "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": str(e), "job_id": job_id}), 202


# ---- Diagnostics ----

@app.route("/api/diag/ble", methods=["POST"])
@require_diag_auth
def api_diag_ble():
    """Low-level BLE diagnostics — tests connection at different layers.
    Protected by X-Auth-Token header when DIAG_TOKEN env var is set."""
    import subprocess
    addr = printer_state.get("address") or (request.json.get("address", "") if request.data else "")
    results = {}

    try:
        r = subprocess.run(["hciconfig", "hci0"], capture_output=True, text=True, timeout=5)
        results["hciconfig"] = r.stdout.strip()
    except Exception as e:
        results["hciconfig"] = f"error: {e}"

    try:
        r = subprocess.run(["dmesg"], capture_output=True, text=True, timeout=5)
        bt_lines = [l for l in r.stdout.splitlines()
                    if "blue" in l.lower() or "bt" in l.lower() or "hci" in l.lower()]
        results["dmesg_bt"] = bt_lines[-15:] if bt_lines else ["no bluetooth kernel messages"]
    except Exception as e:
        results["dmesg_bt"] = [f"error: {e}"]

    if addr:
        try:
            scan_proc = subprocess.run(
                ["bluetoothctl", "--timeout", "8", "scan", "on"],
                capture_output=True, text=True, timeout=12,
            )
            results["bluetoothctl_scan"] = scan_proc.stdout.strip()[-500:]

            conn_proc = subprocess.run(
                ["bluetoothctl", "connect", addr],
                capture_output=True, text=True, timeout=15,
            )
            results["bluetoothctl_connect"] = {
                "stdout": conn_proc.stdout.strip()[-500:],
                "stderr": conn_proc.stderr.strip()[-500:],
                "rc": conn_proc.returncode,
            }
            subprocess.run(["bluetoothctl", "disconnect", addr], capture_output=True, timeout=5)
        except subprocess.TimeoutExpired:
            results["bluetoothctl_connect"] = "TIMEOUT after 15s"
        except Exception as e:
            results["bluetoothctl_connect"] = f"error: {e}"

        try:
            r = subprocess.run(["hcitool", "lecc", addr], capture_output=True, text=True, timeout=10)
            results["hcitool_lecc"] = {
                "stdout": r.stdout.strip(),
                "stderr": r.stderr.strip(),
                "rc": r.returncode,
            }
        except subprocess.TimeoutExpired:
            results["hcitool_lecc"] = "TIMEOUT"
        except FileNotFoundError:
            results["hcitool_lecc"] = "hcitool not found"
        except Exception as e:
            results["hcitool_lecc"] = f"error: {e}"

    return jsonify(results)


# ---- Queue ----

@app.route("/api/queue", methods=["GET"])
def api_get_queue():
    """List print queue entries (without bitmap data). ?status=pending|printed|failed|all"""
    status_filter = request.args.get("status", "pending")
    with get_db() as conn:
        if status_filter == "all":
            rows = conn.execute(
                "SELECT id, type, summary, status, retry_count, created_at, printed_at, error "
                "FROM print_queue ORDER BY id DESC LIMIT 100"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, type, summary, status, retry_count, created_at, printed_at, error "
                "FROM print_queue WHERE status = ? ORDER BY id DESC LIMIT 100",
                (status_filter,),
            ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/queue/<int:job_id>", methods=["DELETE"])
def api_delete_queue_job(job_id):
    with get_db() as conn:
        conn.execute("DELETE FROM print_queue WHERE id = ?", (job_id,))
    return jsonify({"status": "ok"})


@app.route("/api/queue/flush", methods=["POST"])
def api_flush_queue():
    """Try to immediately print all pending jobs (triggers a BLE scan if needed)."""
    addr = printer_state.get("address")
    if not addr:
        try:
            found = run_ble(scan_for_printer())
            if not found:
                return jsonify({"status": "error", "message": "No printer found"}), 404
            addr = found["address"]
            printer_state["address"] = addr
            printer_state["name"] = found["name"]
            printer_state["status"] = "idle"
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    pending = get_pending_jobs()
    if not pending:
        return jsonify({"status": "ok", "printed": 0, "message": "Queue empty"})

    try:
        printed = run_ble(_drain_queue(addr))
        return jsonify({"status": "ok", "printed": printed, "total": len(pending)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _log_ble_diagnostics() -> None:
    """Log BLE/BlueZ environment info for debugging connection issues."""
    import subprocess
    log.info(f"Platform: {os.uname().sysname} / {os.uname().machine}")
    log.info(f"IS_LINUX: {IS_LINUX}")

    if not IS_LINUX:
        return

    dbus_ok = os.path.exists("/var/run/dbus/system_bus_socket")
    log.info(f"D-Bus system socket: {'present' if dbus_ok else 'MISSING — BLE will fail!'}")

    try:
        result = subprocess.run(["bluetoothctl", "--version"], capture_output=True, text=True, timeout=5)
        log.info(f"BlueZ: {result.stdout.strip()}")
    except FileNotFoundError:
        log.warning("bluetoothctl not found")
    except Exception as e:
        log.warning(f"Could not check BlueZ version: {e}")

    try:
        result = subprocess.run(["bluetoothctl", "show"], capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            line = line.strip()
            if any(k in line for k in ["Controller", "Powered", "Name"]):
                log.info(f"  {line}")
    except Exception:
        pass


def _handle_shutdown(signum, frame) -> None:
    log.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5123))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "false").lower() == "true"

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    init_db()
    log.info(f"CatPrint HA starting on {host}:{port}")
    log.info(f"   PIL: {'OK' if HAS_PIL else 'NO'}  |  QR: {'OK' if HAS_QRCODE else 'NO'}  |  BLE: {'OK' if HAS_BLEAK else 'NO'}")

    if HAS_BLEAK:
        _log_ble_diagnostics()

    # Start single persistent BLE worker thread
    threading.Thread(target=_ble_worker_thread, daemon=True, name="ble-worker").start()
    # Give the worker time to initialize _ble_lock before Flask starts serving
    time.sleep(0.5)

    app.run(host=host, port=port, debug=debug)
