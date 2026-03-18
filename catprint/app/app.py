"""
CatPrint HA — Mini Thermal Printer Service for Home Assistant
Supports GOTOOGO C15 and similar iPrint-compatible BLE thermal printers.
Provides REST API + Web UI for printing text, images, and shopping lists.
"""

import asyncio
import io
import logging
import os
import sqlite3
import struct
import textwrap
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, render_template, send_from_directory

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PRINTER_WIDTH_PX = 384  # 48 bytes * 8 bits — standard for 58mm thermal
BYTES_PER_ROW = PRINTER_WIDTH_PX // 8  # 48
SCAN_TIMEOUT = 10.0
CHUNK_DELAY = 0.01  # seconds between BLE chunks

# MXW01 BLE UUIDs (FunPrint protocol, reverse-engineered via PacketLogger)
PRINTER_CMD_UUID    = "0000ae01-0000-1000-8000-00805f9b34fb"  # init/end commands
PRINTER_DATA_UUID   = "0000ae03-0000-1000-8000-00805f9b34fb"  # raw bitmap stream
PRINTER_NOTIFY_UUID = "0000ae02-0000-1000-8000-00805f9b34fb"  # printer responses

# MXW01 protocol commands
# Init: declares image height (rows) and width (48 bytes/row) before bitmap transfer
def _mxw_init_cmd(rows: int) -> bytes:
    return bytes([0x22, 0x21, 0xA9, 0x00, 0x04, 0x00,
                  rows & 0xFF, (rows >> 8) & 0xFF,
                  0x30, 0x00, 0x00, 0x00])

# End: signals that all bitmap data has been sent
MXW_END_CMD = bytes([0x22, 0x21, 0xAD, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00])

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


def text_to_image(text: str, font_size: int = 24, width: int = PRINTER_WIDTH_PX) -> "Image.Image":
    """Render text to a 1-bit PIL image suitable for the printer."""
    if not HAS_PIL:
        raise RuntimeError("Pillow is required for text rendering")

    # Try to use a nice font, fall back to default
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
        font = ImageFont.load_default(size=font_size)

    # Wrap text to fit the width
    dummy_img = Image.new("RGB", (width, 10), "white")
    dummy_draw = ImageDraw.Draw(dummy_img)

    # Estimate characters per line
    avg_char_w = font_size * 0.6
    chars_per_line = max(int(width / avg_char_w), 10)
    wrapped = "\n".join(textwrap.fill(line, width=chars_per_line) for line in text.split("\n"))

    # Measure final text
    bbox = dummy_draw.textbbox((0, 0), wrapped, font=font)
    text_h = bbox[3] - bbox[1] + font_size  # add padding

    img = Image.new("RGB", (width, text_h + 20), "white")
    draw = ImageDraw.Draw(img)
    draw.text((10, 10), wrapped, fill="black", font=font)

    return img




def image_to_bitmap(img: "Image.Image") -> list[list[int]]:
    """
    Convert a PIL image to a list of rows, each row being a list of bytes.
    Each bit represents one pixel (1 = black, 0 = white).
    Width is padded/cropped to PRINTER_WIDTH_PX.
    """
    # Resize width to PRINTER_WIDTH_PX
    if img.width != PRINTER_WIDTH_PX:
        ratio = PRINTER_WIDTH_PX / img.width
        img = img.resize(
            (PRINTER_WIDTH_PX, max(1, int(img.height * ratio))),
            Image.LANCZOS,
        )

    # Convert to grayscale and apply Floyd-Steinberg dithering → 1-bit
    img = img.convert("L")
    img = img.convert("1")  # applies dithering by default

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
                    # In mode "1": 0 = black, 255 = white
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

# Input validation limits
MAX_TEXT_LENGTH = 2000
MIN_FONT_SIZE = 8
MAX_FONT_SIZE = 72

# Known printer BLE advertisement names
KNOWN_PRINTER_NAMES = ["GT01", "GB01", "GB02", "GB03", "GB04", "GT02", "C15", "MXW01"]

# ---------------------------------------------------------------------------
# Thread-safe global state
# ---------------------------------------------------------------------------

# Lock protects _last_ble_device and printer_state from concurrent access
# by Flask request threads and the background scanner thread.
_state_lock = threading.Lock()

# _last_ble_device holds the BLEDevice object from the most recent scan
# so we can hand it directly to BleakClient on Linux/BlueZ
# (avoids D-Bus re-resolution timeouts on RPi).
_last_ble_device = None

printer_state = {
    "address": os.environ.get("PRINTER_ADDRESS", ""),
    "name": os.environ.get("PRINTER_NAME", ""),
    "status": "disconnected",
    "last_print": None,
    "print_count": 0,
    "error": None,
}


def _update_state(**kwargs) -> None:
    """Thread-safe update of printer_state."""
    with _state_lock:
        printer_state.update(kwargs)


def _get_state(*keys) -> tuple:
    """Thread-safe read of printer_state values."""
    with _state_lock:
        return tuple(printer_state[k] for k in keys)


def _get_state_copy() -> dict:
    """Thread-safe snapshot of printer_state."""
    with _state_lock:
        return dict(printer_state)


def _set_ble_device(device) -> None:
    """Thread-safe setter for _last_ble_device."""
    global _last_ble_device
    with _state_lock:
        _last_ble_device = device


def _get_ble_device():
    """Thread-safe getter for _last_ble_device."""
    with _state_lock:
        return _last_ble_device


async def scan_for_printer(timeout: float = SCAN_TIMEOUT) -> dict | None:
    """Scan BLE for a known printer. Stores BLEDevice object for Linux."""
    if not HAS_BLEAK:
        return None

    log.info("Scanning for BLE printers...")
    devices = await BleakScanner.discover(timeout=timeout, return_adv=False)

    for device in devices:
        name = device.name or ""
        if any(known in name for known in KNOWN_PRINTER_NAMES):
            log.info(f"Found printer: {name} @ {device.address}")
            _set_ble_device(device)
            return {"name": name, "address": device.address}

    log.warning("No printer found during scan")
    return None


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
        log.debug(f"bluetoothctl remove {addr}: {result.stdout.strip()} {result.stderr.strip()}")
    except FileNotFoundError:
        # Try D-Bus directly if bluetoothctl is missing (Alpine minimal)
        try:
            import subprocess
            result = subprocess.run(
                ["dbus-send", "--system", "--dest=org.bluez",
                 f"/org/bluez/hci0/dev_{addr.replace(':', '_')}",
                 "org.bluez.Adapter1.RemoveDevice",
                 f"objpath:/org/bluez/hci0/dev_{addr.replace(':', '_')}"],
                capture_output=True, timeout=5, text=True,
            )
            log.debug(f"dbus-send remove: {result.returncode}")
        except Exception:
            pass
    except Exception as e:
        log.debug(f"BlueZ cache clear failed (non-critical): {e}")


def _trust_device(addr: str) -> None:
    """Mark BLE device as trusted in BlueZ (avoids pairing prompts)."""
    if not IS_LINUX:
        return
    import subprocess
    try:
        subprocess.run(["bluetoothctl", "trust", addr], capture_output=True, timeout=5)
        log.debug(f"bluetoothctl trust {addr}: OK")
    except Exception as e:
        log.debug(f"Trust failed (non-critical): {e}")


async def send_to_printer(bitmap_data: bytes, address: str = None) -> bool:
    """Send raw bitmap data to MXW01 printer over BLE."""
    if not HAS_BLEAK:
        raise RuntimeError("bleak library is required for BLE communication")

    addr = address or _get_state("address")[0]
    if not addr:
        found = await scan_for_printer()
        if not found:
            raise RuntimeError("No printer found. Please scan first or set PRINTER_ADDRESS.")
        addr = found["address"]
        _update_state(address=addr, name=found["name"])

    rows = len(bitmap_data) // BYTES_PER_ROW
    _update_state(status="connecting")

    last_err = None
    for attempt in range(1, BLE_CONNECT_RETRIES + 1):
        log.info(f"Connecting to printer at {addr}... ({rows} rows) [attempt {attempt}/{BLE_CONNECT_RETRIES}]")

        if IS_LINUX:
            cached_dev = _get_ble_device()
            need_scan = (attempt > 1
                         or cached_dev is None
                         or cached_dev.address != addr)
            if attempt > 1:
                _clear_bluez_cache(addr)
                await asyncio.sleep(1.0)
            if need_scan:
                log.info("Fresh BLE scan on current event loop...")
                _set_ble_device(None)
                await scan_for_printer(timeout=10.0)
            # Trust the device in BlueZ to avoid pairing prompts
            _trust_device(addr)
            await asyncio.sleep(2.0)

        # Prefer BLEDevice object on Linux (avoids D-Bus address resolution timeout)
        cached_dev = _get_ble_device()
        connect_target = addr
        if IS_LINUX and cached_dev and cached_dev.address == addr:
            connect_target = cached_dev
            log.info("Using BLEDevice object for connection (BlueZ optimized)")

        try:
            async with BleakClient(connect_target, timeout=60.0) as client:
                _update_state(status="printing")
                chunk_size = max(client.mtu_size - 3, 20)
                log.info(f"Connected. MTU: {client.mtu_size}, rows: {rows}")

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

                # 1. Send init — declares image dimensions
                init_cmd = _mxw_init_cmd(rows)
                log.info(f"Init: {init_cmd.hex()}")
                await client.write_gatt_char(PRINTER_CMD_UUID, init_cmd, response=False)
                try:
                    await asyncio.wait_for(ack.wait(), timeout=5.0)
                    log.info("Init ACK received")
                except asyncio.TimeoutError:
                    log.warning("Init ACK timeout — continuing anyway (printer may still work)")

                # 2. Stream raw bitmap
                for i in range(0, len(bitmap_data), chunk_size):
                    await client.write_gatt_char(PRINTER_DATA_UUID, bitmap_data[i:i+chunk_size], response=False)
                    await asyncio.sleep(CHUNK_DELAY)

                # 3. Send end command
                await client.write_gatt_char(PRINTER_CMD_UUID, MXW_END_CMD, response=False)
                log.info("End command sent, waiting for completion...")

                # 4. Wait for completion notification
                try:
                    await asyncio.wait_for(done.wait(), timeout=15.0)
                    log.info("Print completed (printer confirmed)")
                except asyncio.TimeoutError:
                    log.warning("No completion notification — print may still succeed")

                with _state_lock:
                    printer_state["status"] = "idle"
                    printer_state["last_print"] = datetime.now().isoformat()
                    printer_state["print_count"] += 1
                    printer_state["error"] = None
                log.info("Print job completed successfully")
                return True

        except Exception as e:
            last_err = e
            log.warning(f"Attempt {attempt}/{BLE_CONNECT_RETRIES} failed [{type(e).__name__}]: {e!r}")
            _set_ble_device(None)  # force fresh scan on next attempt
            if attempt < BLE_CONNECT_RETRIES:
                await asyncio.sleep(2.0)
            continue

    # All retries exhausted
    _update_state(status="error", error=str(last_err) or repr(last_err))
    log.error(f"Print failed after {BLE_CONNECT_RETRIES} attempts: {last_err!r}")
    raise last_err


# ---------------------------------------------------------------------------
# Database (SQLite — templates)
# ---------------------------------------------------------------------------

DB_PATH = Path(os.environ.get("DB_PATH", str(Path(__file__).parent / "catprint.db")))


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        # Enable WAL mode for better concurrent read/write performance
        conn.execute("PRAGMA journal_mode=WAL")
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
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                printed_at  TIMESTAMP,
                error       TEXT
            )
        """)
        # Indexes for frequent queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON print_queue(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_history_created ON print_history(created_at)")
    log.info(f"Database ready at {DB_PATH} (WAL mode)")


def log_print(type_: str, summary: str, rows: int = None, font_size: int = None) -> None:
    """Insert a record into print_history (best-effort, never raises)."""
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO print_history (type, summary, rows, font_size) VALUES (?, ?, ?, ?)",
                (type_, summary[:200], rows, font_size),
            )
    except sqlite3.DatabaseError as exc:
        log.error(f"Database error logging print (possible corruption): {exc}")
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
    with get_db() as conn:
        conn.execute(
            "UPDATE print_queue SET status = 'failed', error = ? WHERE id = ?",
            (error[:500], job_id),
        )


# ---------------------------------------------------------------------------
# Background BLE scanner
# ---------------------------------------------------------------------------


async def _drain_queue(address: str) -> int:
    """
    Print all pending jobs using the given BLE address.
    Returns the number of successfully printed jobs.
    """
    jobs = get_pending_jobs()
    if not jobs:
        return 0

    log.info(f"Draining queue: {len(jobs)} pending job(s)...")
    printed = 0
    for job in jobs:
        try:
            await send_to_printer(bytes(job["bitmap_data"]), address=address)
            mark_job_printed(job["id"])
            log_print(job["type"], job["summary"])
            printed += 1
            log.info(f"Queue job {job['id']} ({job['type']}) printed OK")
        except Exception as e:
            mark_job_failed(job["id"], str(e) or repr(e))
            log.error(f"Queue job {job['id']} failed [{type(e).__name__}]: {e!r}")
            break  # printer likely disconnected, stop
    return printed


def _scanner_loop() -> None:
    """Background thread: scan for BLE printer every 10 s, drain queue when found."""
    log.info("Background BLE scanner started (interval: 10 s)")
    while True:
        time.sleep(10)

        # Skip if nothing to print
        try:
            if not get_pending_jobs():
                continue
        except Exception as e:
            log.warning(f"Scanner: DB error checking queue: {e!r}")
            continue

        # Try to acquire BLE lock — skip cycle if another operation is running
        if not _ble_lock.acquire(blocking=False):
            log.debug("Scanner: BLE busy, skipping cycle")
            continue

        log.info("Scanner: pending jobs found, scanning for printer...")
        try:
            found = run_async(scan_for_printer(timeout=8.0))
            if found:
                _update_state(address=found["address"], name=found["name"], status="idle")
                log.info(f"Scanner: printer found at {found['address']}, draining queue...")
                run_async(_drain_queue(found["address"]))
                status = _get_state("status")[0]
                if status not in ("printing", "error"):
                    _update_state(status="idle")
            else:
                log.debug("Scanner: no printer found this cycle")
        except Exception as e:
            log.warning(f"Scanner cycle error [{type(e).__name__}]: {e!r}")
        finally:
            _ble_lock.release()


# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload

# Global lock: only one BLE operation at a time (scan OR connect)
_ble_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Dedicated asyncio event loop (single loop for all async/BLE work)
# ---------------------------------------------------------------------------

_loop: asyncio.AbstractEventLoop | None = None
_loop_thread: threading.Thread | None = None


def _start_event_loop() -> None:
    """Start a persistent asyncio event loop in a background thread."""
    global _loop, _loop_thread
    _loop = asyncio.new_event_loop()

    def _run():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    _loop_thread = threading.Thread(target=_run, daemon=True, name="asyncio-loop")
    _loop_thread.start()
    log.info("Dedicated asyncio event loop started")


def run_async(coro):
    """Submit a coroutine to the dedicated event loop and wait for the result."""
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=60.0)


def run_ble(coro):
    """Run a BLE coroutine exclusively — blocks until the lock is free."""
    acquired = _ble_lock.acquire(timeout=35.0)
    if not acquired:
        raise RuntimeError("BLE busy — another operation is already in progress")
    try:
        return run_async(coro)
    finally:
        _ble_lock.release()


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
        "printer": _get_state_copy(),
        "capabilities": {
            "has_pil": HAS_PIL,
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
            _update_state(address=result["address"], name=result["name"], status="idle")
            pending = get_pending_jobs()
            if pending:
                run_ble(_drain_queue(result["address"]))
            return jsonify({"status": "found", "printer": result, "pending_jobs": len(pending)})
        return jsonify({"status": "not_found", "message": "No printer found"}), 404
    except Exception as e:
        log.error(f"Scan failed: {e!r}")
        return jsonify({"status": "error", "message": "BLE scan failed"}), 500


@app.route("/api/print/text", methods=["POST"])
def api_print_text():
    """Print text. Body: { "text": "...", "font_size": 24 }"""
    data = request.get_json(force=True)
    text = data.get("text", "")
    font_size = data.get("font_size", 24)

    if not text.strip():
        return jsonify({"status": "error", "message": "No text provided"}), 400
    if len(text) > MAX_TEXT_LENGTH:
        return jsonify({"status": "error", "message": f"Text too long (max {MAX_TEXT_LENGTH} chars)"}), 400
    font_size = max(MIN_FONT_SIZE, min(MAX_FONT_SIZE, int(font_size)))

    try:
        img = text_to_image(text, font_size=font_size)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        log.error(f"Image rendering failed: {e!r}")
        return jsonify({"status": "error", "message": "Failed to render text"}), 500

    job_id = enqueue_job("text", text[:80], bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("text", text[:80], rows=len(bitmap), font_size=font_size)
        return jsonify({"status": "ok", "rows_printed": len(bitmap), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": "Printer unavailable, job queued", "job_id": job_id}), 202


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
        log.error(f"Image processing failed: {e!r}")
        return jsonify({"status": "error", "message": "Failed to process image"}), 500

    summary = file.filename or "obraz"
    job_id = enqueue_job("image", summary, bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("image", summary, rows=len(bitmap))
        return jsonify({"status": "ok", "rows_printed": len(bitmap), "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": "Printer unavailable, job queued", "job_id": job_id}), 202




@app.route("/api/feed", methods=["POST"])
def api_feed():
    """Feed paper. Body: { "lines": 3 }"""
    data = request.get_json(force=True) if request.data else {}
    lines = data.get("lines", 3)
    feed_rows = max(lines * 8, 8)  # ~8 bitmap rows per line
    bitmap_data = bytes(BYTES_PER_ROW * feed_rows)

    try:
        run_ble(send_to_printer(bitmap_data))
        return jsonify({"status": "ok"})
    except Exception as e:
        log.error(f"Feed failed: {e!r}")
        return jsonify({"status": "error", "message": "Feed operation failed"}), 500


# ---- Templates ----

@app.route("/api/templates", methods=["GET"])
def api_get_templates():
    """List all saved templates."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, text, font_size, created_at FROM templates ORDER BY created_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/templates", methods=["POST"])
def api_save_template():
    """Save a new template. Body: { "name": "...", "text": "...", "font_size": 24 }"""
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
    """Delete a template by id."""
    with get_db() as conn:
        conn.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    return jsonify({"status": "ok"})


# ---- History ----

@app.route("/api/history", methods=["GET"])
def api_get_history():
    """Return last N print history entries."""
    limit = min(int(request.args.get("limit", 30)), 100)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, type, summary, rows, font_size, created_at "
            "FROM print_history ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/history/<int:history_id>", methods=["DELETE"])
def api_delete_history_entry(history_id):
    """Delete a single print history entry by id."""
    with get_db() as conn:
        conn.execute("DELETE FROM print_history WHERE id = ?", (history_id,))
    return jsonify({"status": "ok"})


@app.route("/api/history", methods=["DELETE"])
def api_clear_history():
    """Clear all print history."""
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

    # Build a nicely formatted list
    lines = [title, "─" * 30, ""]
    for i, item in enumerate(items, 1):
        lines.append(f"  □  {item}")
    lines.append("")
    lines.append("─" * 30)
    lines.append(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    text = "\n".join(lines)

    try:
        img = text_to_image(text, font_size=22)
        bitmap = image_to_bitmap(img)
        bitmap_bytes = build_raw_bitmap(bitmap)
    except Exception as e:
        log.error(f"Shopping list rendering failed: {e!r}")
        return jsonify({"status": "error", "message": "Failed to render shopping list"}), 500

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
        return jsonify({"status": "queued", "message": "Printer unavailable, job queued", "job_id": job_id}), 202


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
        log.error(f"Notification rendering failed: {e!r}")
        return jsonify({"status": "error", "message": "Failed to render notification"}), 500

    summary = f"{title}: {message[:60]}"
    job_id = enqueue_job("notification", summary, bitmap_bytes)
    try:
        run_ble(send_to_printer(bitmap_bytes))
        mark_job_printed(job_id)
        log_print("notification", summary, rows=len(bitmap))
        return jsonify({"status": "ok", "job_id": job_id})
    except Exception as e:
        log.warning(f"Print failed, job {job_id} queued for retry: {e}")
        return jsonify({"status": "queued", "message": "Printer unavailable, job queued", "job_id": job_id}), 202


# ---- Queue ----

@app.route("/api/queue", methods=["GET"])
def api_get_queue():
    """List print queue entries (without bitmap data). ?status=pending|printed|failed|all"""
    status_filter = request.args.get("status", "pending")
    with get_db() as conn:
        if status_filter == "all":
            rows = conn.execute(
                "SELECT id, type, summary, status, created_at, printed_at, error "
                "FROM print_queue ORDER BY id DESC LIMIT 100"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, type, summary, status, created_at, printed_at, error "
                "FROM print_queue WHERE status = ? ORDER BY id DESC LIMIT 100",
                (status_filter,),
            ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/queue/<int:job_id>", methods=["DELETE"])
def api_delete_queue_job(job_id):
    """Delete a queued job by id."""
    with get_db() as conn:
        conn.execute("DELETE FROM print_queue WHERE id = ?", (job_id,))
    return jsonify({"status": "ok"})


@app.route("/api/queue/clear-done", methods=["POST"])
def api_clear_done_queue():
    """Remove all printed and failed jobs from the queue (keeps pending)."""
    with get_db() as conn:
        conn.execute("DELETE FROM print_queue WHERE status != 'pending'")
    return jsonify({"status": "ok"})


@app.route("/api/queue/flush", methods=["POST"])
def api_flush_queue():
    """Try to immediately print all pending jobs (triggers a BLE scan if needed)."""
    addr = _get_state("address")[0]
    if not addr:
        try:
            found = run_ble(scan_for_printer())
            if not found:
                return jsonify({"status": "error", "message": "No printer found"}), 404
            addr = found["address"]
            _update_state(address=addr, name=found["name"], status="idle")
        except Exception as e:
            log.error(f"Flush scan failed: {e!r}")
            return jsonify({"status": "error", "message": "Could not find printer"}), 500

    pending = get_pending_jobs()
    if not pending:
        return jsonify({"status": "ok", "printed": 0, "message": "Queue empty"})

    try:
        printed = run_ble(_drain_queue(addr))
        return jsonify({"status": "ok", "printed": printed, "total": len(pending)})
    except Exception as e:
        log.error(f"Queue flush failed: {e!r}")
        return jsonify({"status": "error", "message": "Failed to flush queue"}), 500


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

    # Check D-Bus socket
    dbus_ok = os.path.exists("/var/run/dbus/system_bus_socket")
    log.info(f"D-Bus system socket: {'present' if dbus_ok else 'MISSING — BLE will fail!'}")

    # BlueZ version
    try:
        result = subprocess.run(["bluetoothctl", "--version"],
                                capture_output=True, text=True, timeout=5)
        log.info(f"BlueZ: {result.stdout.strip()}")
    except FileNotFoundError:
        log.warning("bluetoothctl not found — using dbus-send fallback for cache clearing")
    except Exception as e:
        log.warning(f"Could not check BlueZ version: {e}")

    # Check if adapter is available
    try:
        result = subprocess.run(["bluetoothctl", "show"],
                                capture_output=True, text=True, timeout=5)
        for line in result.stdout.splitlines():
            line = line.strip()
            if any(k in line for k in ["Controller", "Powered", "Name"]):
                log.info(f"  {line}")
    except Exception:
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5123))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "false").lower() == "true"

    init_db()
    log.info(f"CatPrint HA starting on {host}:{port}")
    log.info(f"   PIL: {'OK' if HAS_PIL else 'NO'}  |  BLE: {'OK' if HAS_BLEAK else 'NO'}")

    # Start dedicated asyncio event loop (shared by all async operations)
    _start_event_loop()

    if HAS_BLEAK:
        _log_ble_diagnostics()

    # Start background BLE scanner (skips if bleak not available)
    if HAS_BLEAK:
        threading.Thread(target=_scanner_loop, daemon=True, name="bt-scanner").start()
    else:
        log.warning("BLE not available — background scanner disabled")

    app.run(host=host, port=port, debug=debug)
