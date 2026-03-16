"""
CatPrint HA — Mini Thermal Printer Service for Home Assistant
Supports GOTOOGO C15 and similar iPrint-compatible BLE thermal printers.
Provides REST API + Web UI for printing text, images, and QR codes.
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

try:
    import qrcode
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False


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
        # Linux
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


def qr_to_image(data: str, width: int = PRINTER_WIDTH_PX) -> "Image.Image":
    """Generate a QR code image."""
    if not HAS_QRCODE:
        raise RuntimeError("qrcode library is required")
    qr = qrcode.QRCode(box_size=8, border=2)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    # Resize to printer width keeping aspect ratio
    ratio = width / img.width
    img = img.resize((width, int(img.height * ratio)), Image.NEAREST)
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

# Known printer BLE advertisement names
KNOWN_PRINTER_NAMES = ["GT01", "GB01", "GB02", "GB03", "GB04", "GT02", "C15", "MXW01"]

# Global state
printer_state = {
    "address": os.environ.get("PRINTER_ADDRESS", ""),
    "name": os.environ.get("PRINTER_NAME", ""),
    "status": "disconnected",
    "last_print": None,
    "print_count": 0,
    "error": None,
}


async def scan_for_printer(timeout: float = SCAN_TIMEOUT) -> dict | None:
    """Scan BLE for a known printer."""
    if not HAS_BLEAK:
        return None

    log.info("Scanning for BLE printers...")
    devices = await BleakScanner.discover(timeout=timeout)

    for device in devices:
        name = device.name or ""
        if any(known in name for known in KNOWN_PRINTER_NAMES):
            log.info(f"Found printer: {name} @ {device.address}")
            return {"name": name, "address": device.address}

    log.warning("No printer found during scan")
    return None


async def send_to_printer(bitmap_data: bytes, address: str = None) -> bool:
    """Send raw bitmap data to MXW01 printer over BLE."""
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
    log.info(f"Connecting to printer at {addr}... ({rows} rows)")
    printer_state["status"] = "connecting"

    try:
        async with BleakClient(addr, timeout=20.0) as client:
            printer_state["status"] = "printing"
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
            await asyncio.wait_for(ack.wait(), timeout=5.0)
            log.info("Init ACK received")

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

            printer_state["status"] = "idle"
            printer_state["last_print"] = datetime.now().isoformat()
            printer_state["print_count"] += 1
            printer_state["error"] = None
            log.info("Print job completed successfully")
            return True

    except Exception as e:
        printer_state["status"] = "error"
        printer_state["error"] = str(e) or repr(e)
        log.error(f"Print failed [{type(e).__name__}]: {e!r}")
        raise


# ---------------------------------------------------------------------------
# Database (SQLite — templates)
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
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                printed_at  TIMESTAMP,
                error       TEXT
            )
        """)
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
        if not get_pending_jobs():
            continue

        # Try to acquire BLE lock — skip cycle if another operation is running
        if not _ble_lock.acquire(blocking=False):
            log.debug("Scanner: BLE busy, skipping cycle")
            continue

        log.info("Scanner: pending jobs found, scanning for printer...")
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            found = loop.run_until_complete(scan_for_printer(timeout=8.0))
            if found:
                printer_state["address"] = found["address"]
                printer_state["name"] = found["name"]
                printer_state["status"] = "idle"
                log.info(f"Scanner: printer found at {found['address']}, draining queue...")
                loop.run_until_complete(_drain_queue(found["address"]))
                if printer_state["status"] not in ("printing", "error"):
                    printer_state["status"] = "idle"
            else:
                log.debug("Scanner: no printer found this cycle")
            loop.close()
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


def run_async(coro):
    """Run an async coroutine from synchronous Flask context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def run_ble(coro):
    """Run a BLE coroutine exclusively — blocks until the lock is free."""
    acquired = _ble_lock.acquire(timeout=35.0)
    if not acquired:
        raise RuntimeError("BLE busy — another operation is already in progress")
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
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
    feed_rows = max(lines * 8, 8)  # ~8 bitmap rows per line
    bitmap_data = bytes(BYTES_PER_ROW * feed_rows)

    try:
        run_ble(send_to_printer(bitmap_data))
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5123))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "false").lower() == "true"

    init_db()
    log.info(f"🖨️  CatPrint HA starting on {host}:{port}")
    log.info(f"   PIL: {'✅' if HAS_PIL else '❌'}  |  QR: {'✅' if HAS_QRCODE else '❌'}  |  BLE: {'✅' if HAS_BLEAK else '❌'}")

    # Start background BLE scanner (skips if bleak not available)
    if HAS_BLEAK:
        threading.Thread(target=_scanner_loop, daemon=True, name="bt-scanner").start()
    else:
        log.warning("BLE not available — background scanner disabled")

    app.run(host=host, port=port, debug=debug)
