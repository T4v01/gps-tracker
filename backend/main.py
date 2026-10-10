"""
GPS Tracker - Backend (v2)
FastAPI + WebSocket + SQLite (WAL). Soporta muchos celulares a la vez,
envío por lotes (cola offline), tiempo real y claves opcionales.

Variables de entorno (todas opcionales):
  GPS_TOKEN      clave que deben enviar los celulares/app (header X-Token)
  PANEL_TOKEN    clave para ver el panel (header X-Key / ?key= en la URL)
  RETENTION_DAYS días de historial a conservar (90 por defecto)
"""
import asyncio
import json
import os
import sqlite3
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Optional, Union

from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "gps.db"
FRONTEND_DIR = BASE_DIR.parent / "frontend"
GPS_TOKEN = os.getenv("GPS_TOKEN", "")
PANEL_TOKEN = os.getenv("PANEL_TOKEN", "")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90"))

# ------------------------------ Base de datos ------------------------------
_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row
_lock = threading.Lock()


def init_db():
    with _lock:
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("PRAGMA synchronous=NORMAL")
        _db.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                uuid TEXT PRIMARY KEY, nombre TEXT, primera_conexion TEXT,
                ultima_conexion TEXT, ultima_lat REAL, ultima_lng REAL, ultima_ip TEXT);
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, device_uuid TEXT NOT NULL,
                lat REAL NOT NULL, lng REAL NOT NULL, accuracy REAL, speed REAL,
                battery REAL, ip TEXT, timestamp TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_loc_device_time ON locations(device_uuid, timestamp);
            CREATE TABLE IF NOT EXISTS alertas (
                id INTEGER PRIMARY KEY AUTOINCREMENT, device_uuid TEXT NOT NULL, tipo TEXT NOT NULL,
                lat REAL, lng REAL, mensaje TEXT, bateria REAL, creado TEXT NOT NULL, resuelto_en TEXT);
            CREATE TABLE IF NOT EXISTS lugares (
                id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL, lat REAL NOT NULL, lng REAL NOT NULL, radio REAL NOT NULL);
            """
        )
        nuevas = {
            "devices": ["ultima_accuracy REAL", "ultima_speed REAL", "ultima_battery REAL", "color TEXT", "fuente TEXT", "ultima_extra TEXT"],
            "locations": ["heading REAL", "altitude REAL", "source TEXT"],
        }
        for tabla, cols in nuevas.items():
            existentes = {r["name"] for r in _db.execute(f"PRAGMA table_info({tabla})")}
            for c in cols:
                if c.split()[0] not in existentes:
                    _db.execute(f"ALTER TABLE {tabla} ADD COLUMN {c}")
        _db.commit()


init_db()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def a_iso(ts, ahora: datetime) -> str:
    """Convierte ms-epoch / segundos / ISO a ISO UTC uniforme. Futuro o inválido => ahora."""
    try:
        if ts is None:
            return iso(ahora)
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000 if ts > 1e11 else ts, timezone.utc)
        else:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return iso(ahora)
    if dt > ahora + timedelta(minutes=2):
        dt = ahora
    return iso(dt)


def check(esperado: str, recibido: str):
    if esperado and recibido != esperado:
        raise HTTPException(status_code=401, detail="Clave inválida")


def ip_cliente(request: Request) -> Optional[str]:
    xff = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    return xff or (request.client.host if request.client else None)


# --------------------------------- Modelos ---------------------------------
class Punto(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    accuracy: Optional[float] = None
    speed: Optional[float] = None
    heading: Optional[float] = None
    altitude: Optional[float] = None
    battery: Optional[float] = Field(default=None, ge=0, le=100)
    timestamp: Union[float, str, None] = None
    extra: Optional[dict[str, Union[float, int, str, bool]]] = None   # telemetría opcional del celular (sensores)


class UbicacionIn(Punto):
    uuid: str = Field(min_length=8, max_length=64)
    nombre: Optional[str] = Field(default=None, max_length=40)
    source: Optional[str] = Field(default=None, max_length=10)


class LoteIn(BaseModel):
    uuid: str = Field(min_length=8, max_length=64)
    nombre: Optional[str] = Field(default=None, max_length=40)
    source: Optional[str] = Field(default=None, max_length=10)
    puntos: list[Punto] = Field(max_length=500)


class EditarIn(BaseModel):
    nombre: Optional[str] = Field(default=None, min_length=1, max_length=40)
    color: Optional[str] = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")


# --------------------------------- WebSocket --------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def _send(self, ws: WebSocket, texto: str):
        try:
            await asyncio.wait_for(ws.send_text(texto), 5)
        except Exception:
            self.disconnect(ws)

    async def broadcast(self, data: dict):
        texto = json.dumps(data)
        await asyncio.gather(*(self._send(ws, texto) for ws in list(self.active)))


manager = ConnectionManager()


async def limpieza():
    while True:
        corte = iso(utcnow() - timedelta(days=RETENTION_DAYS))
        with _lock:
            _db.execute("DELETE FROM locations WHERE timestamp < ?", (corte,))
            _db.commit()
        await asyncio.sleep(86400)


@asynccontextmanager
async def lifespan(_: FastAPI):
    tarea = asyncio.create_task(limpieza())
    yield
    tarea.cancel()


app = FastAPI(title="GPS Tracker", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------- Ingesta ---------------------------------
_dentro: dict = {}      # (uuid, lugar_id) -> True si el dispositivo está dentro de ese lugar
_comandos: dict = {}    # uuid -> comandos pendientes para el celular ('sonar', 'ubicar')


def _dist_m(lat1, lng1, lat2, lng2) -> float:
    a = sin(radians(lat2 - lat1) / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(radians(lng2 - lng1) / 2) ** 2
    return 12742000 * asin(sqrt(a))


CRITICAS = ('sos', 'caida')   # alertas que requieren ser atendidas


def _crear_alerta(uuid, tipo, lat, lng, mensaje, bateria, nombre) -> dict:
    """Inserta una alerta (llamar con _lock tomado) y devuelve el mensaje para el panel."""
    creado = iso(utcnow())
    cur = _db.execute(
        "INSERT INTO alertas (device_uuid,tipo,lat,lng,mensaje,bateria,creado) VALUES (?,?,?,?,?,?,?)",
        (uuid, tipo, lat, lng, mensaje, bateria, creado))
    _db.commit()
    return {"tipo": "alerta", "id": cur.lastrowid, "uuid": uuid, "nombre": nombre, "alerta_tipo": tipo,
            "lat": lat, "lng": lng, "mensaje": mensaje, "battery": bateria, "creado": creado,
            "activa": tipo in CRITICAS}


def guardar(uuid, nombre, source, puntos, ip) -> dict:
    ahora = utcnow()
    filas = sorted(((a_iso(p.timestamp, ahora), p) for p in puntos), key=lambda x: x[0])
    ts, u = filas[-1]
    recibido = iso(ahora)
    with _lock:
        prev = _db.execute("SELECT ultima_battery FROM devices WHERE uuid=?", (uuid,)).fetchone()
        extra_json = json.dumps(u.extra) if u.extra and len(json.dumps(u.extra)) <= 500 else None
        _db.executemany(
            "INSERT INTO locations (device_uuid,lat,lng,accuracy,speed,battery,ip,timestamp,heading,altitude,source)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(uuid, p.lat, p.lng, p.accuracy, p.speed, p.battery, ip, t, p.heading, p.altitude, source) for t, p in filas],
        )
        _db.execute(
            """INSERT INTO devices (uuid,nombre,primera_conexion,ultima_conexion,ultima_lat,ultima_lng,ultima_ip,
                                    ultima_accuracy,ultima_speed,ultima_battery,fuente,ultima_extra)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(uuid) DO UPDATE SET
                 nombre=COALESCE(?,nombre), ultima_conexion=excluded.ultima_conexion,
                 ultima_lat=excluded.ultima_lat, ultima_lng=excluded.ultima_lng, ultima_ip=excluded.ultima_ip,
                 ultima_accuracy=excluded.ultima_accuracy, ultima_speed=excluded.ultima_speed,
                 ultima_battery=COALESCE(excluded.ultima_battery, ultima_battery),
                 fuente=COALESCE(excluded.fuente, fuente), ultima_extra=COALESCE(excluded.ultima_extra, ultima_extra)""",
            (uuid, nombre or f"Dispositivo {uuid[:6]}", recibido, recibido, u.lat, u.lng, ip,
             u.accuracy, u.speed, u.battery, source, extra_json, nombre),
        )
        _db.commit()
        d = _db.execute("SELECT nombre, ultima_battery FROM devices WHERE uuid=?", (uuid,)).fetchone()
        extra = []   # alerta automática: batería baja (<= 15 %) al cruzar el umbral
        if u.battery is not None and u.battery <= 15 and (prev is None or prev["ultima_battery"] is None or prev["ultima_battery"] > 15):
            extra.append(_crear_alerta(uuid, "bateria", u.lat, u.lng, f"Batería al {int(u.battery)} %", u.battery, d["nombre"]))
        for lg in _db.execute("SELECT id,nombre,lat,lng,radio FROM lugares").fetchall():   # geocercas
            dentro = _dist_m(u.lat, u.lng, lg["lat"], lg["lng"]) <= lg["radio"]
            antes = _dentro.get((uuid, lg["id"]))
            _dentro[(uuid, lg["id"])] = dentro
            if antes is not None and antes != dentro:
                extra.append(_crear_alerta(uuid, "lugar", u.lat, u.lng,
                                           f"{'Entró a' if dentro else 'Salió de'} {lg['nombre']}", u.battery, d["nombre"]))
    return {
        "tipo": "ubicacion", "uuid": uuid, "nombre": d["nombre"], "lat": u.lat, "lng": u.lng,
        "accuracy": u.accuracy, "speed": u.speed, "heading": u.heading,
        "battery": d["ultima_battery"], "ip": ip, "timestamp": ts, "recibido": recibido, "_alertas": extra, "fuente": source, "extra": u.extra,
    }


async def procesar(uuid, nombre, source, puntos, request: Request):
    nombre = (nombre or "").strip()[:40] or None
    msg = await asyncio.to_thread(guardar, uuid, nombre, source, puntos, ip_cliente(request))
    extras = msg.pop("_alertas", [])
    await manager.broadcast(msg)
    for a in extras:
        await manager.broadcast(a)
    return {"ok": True, "guardados": len(puntos)}


@app.post("/api/ubicacion")
async def recibir_ubicacion(p: UbicacionIn, request: Request, x_token: str = Header(default="")):
    check(GPS_TOKEN, x_token)
    return await procesar(p.uuid, p.nombre, p.source, [p], request)


@app.post("/api/ubicaciones")
async def recibir_lote(lote: LoteIn, request: Request, x_token: str = Header(default="")):
    check(GPS_TOKEN, x_token)
    if not lote.puntos:
        return {"ok": True, "guardados": 0}
    return await procesar(lote.uuid, lote.nombre, lote.source, lote.puntos, request)


# ------------------------------ Alertas / SOS -------------------------------
class AlertaIn(BaseModel):
    uuid: str = Field(min_length=8, max_length=64)
    nombre: Optional[str] = Field(default=None, max_length=40)
    tipo: str = Field(default="sos", pattern="^(sos|caida|impacto)$")
    lat: Optional[float] = Field(default=None, ge=-90, le=90)
    lng: Optional[float] = Field(default=None, ge=-180, le=180)
    accuracy: Optional[float] = None
    mensaje: Optional[str] = Field(default=None, max_length=120)
    battery: Optional[float] = Field(default=None, ge=0, le=100)


class CancelarIn(BaseModel):
    uuid: str = Field(min_length=8, max_length=64)


def _resolver(donde: str, params: list) -> list:
    with _lock:
        ids = [r["id"] for r in _db.execute(
            "SELECT id FROM alertas WHERE tipo IN ('sos','caida') AND resuelto_en IS NULL AND " + donde, params)]
        _db.execute("UPDATE alertas SET resuelto_en=? WHERE tipo IN ('sos','caida') AND resuelto_en IS NULL AND " + donde,
                    [iso(utcnow()), *params])
        _db.commit()
    return ids


@app.post("/api/alerta")
async def crear_alerta(a: AlertaIn, request: Request, x_token: str = Header(default="")):
    check(GPS_TOKEN, x_token)
    if a.lat is not None and a.lng is not None:   # guarda también la posición del SOS
        await procesar(a.uuid, a.nombre, None, [Punto(lat=a.lat, lng=a.lng, accuracy=a.accuracy, battery=a.battery)], request)

    def _crear():
        with _lock:
            fila = _db.execute("SELECT nombre FROM devices WHERE uuid=?", (a.uuid,)).fetchone()
            nombre = fila["nombre"] if fila else (a.nombre or f"Dispositivo {a.uuid[:6]}")
            if not fila:
                ahora = iso(utcnow())
                _db.execute("INSERT INTO devices (uuid,nombre,primera_conexion,ultima_conexion) VALUES (?,?,?,?)",
                            (a.uuid, nombre, ahora, ahora))
            return _crear_alerta(a.uuid, a.tipo, a.lat, a.lng, a.mensaje, a.battery, nombre)

    msg = await asyncio.to_thread(_crear)
    await manager.broadcast(msg)
    return {"ok": True, "id": msg["id"]}


@app.post("/api/alerta/cancelar")
async def cancelar_alerta(c: CancelarIn, x_token: str = Header(default="")):
    """El propio celular avisa 'estoy bien'."""
    check(GPS_TOKEN, x_token)
    ids = await asyncio.to_thread(_resolver, "device_uuid=?", [c.uuid])
    for i in ids:
        await manager.broadcast({"tipo": "alerta_resuelta", "id": i, "uuid": c.uuid, "por": "dispositivo"})
    return {"ok": True, "resueltas": len(ids)}


@app.post("/api/alerta/{alerta_id}/resolver")
async def resolver_alerta(alerta_id: int, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        fila = _db.execute("SELECT device_uuid FROM alertas WHERE id=?", (alerta_id,)).fetchone()
    if not fila:
        raise HTTPException(status_code=404, detail="Alerta no encontrada")
    await asyncio.to_thread(_resolver, "id=?", [alerta_id])
    await manager.broadcast({"tipo": "alerta_resuelta", "id": alerta_id, "uuid": fila["device_uuid"], "por": "panel"})
    return {"ok": True}


@app.get("/api/alertas")
def listar_alertas(limit: int = Query(30, ge=1, le=200), x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        filas = _db.execute("SELECT a.*, d.nombre FROM alertas a LEFT JOIN devices d ON d.uuid=a.device_uuid "
                            "ORDER BY a.id DESC LIMIT ?", (limit,)).fetchall()
    return [{"id": f["id"], "uuid": f["device_uuid"], "nombre": f["nombre"] or f["device_uuid"][:6],
             "alerta_tipo": f["tipo"], "lat": f["lat"], "lng": f["lng"], "mensaje": f["mensaje"],
             "creado": f["creado"], "activa": f["tipo"] in CRITICAS and f["resuelto_en"] is None} for f in filas]


# --------------------- Lugares (geocercas) y comandos al celular ---------------------
class LugarIn(BaseModel):
    nombre: str = Field(min_length=1, max_length=40)
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    radio: float = Field(default=100, ge=20, le=5000)


class ComandoIn(BaseModel):
    tipo: str = Field(pattern="^(sonar|ubicar)$")


@app.get("/api/lugares")
def listar_lugares(x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        return [dict(r) for r in _db.execute("SELECT * FROM lugares ORDER BY id").fetchall()]


@app.post("/api/lugares")
def crear_lugar(lg: LugarIn, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        cur = _db.execute("INSERT INTO lugares (nombre,lat,lng,radio) VALUES (?,?,?,?)",
                          (lg.nombre.strip(), lg.lat, lg.lng, lg.radio))
        _db.commit()
    _dentro.clear()   # vuelve a evaluar dónde está cada dispositivo
    return {"id": cur.lastrowid, "nombre": lg.nombre.strip(), "lat": lg.lat, "lng": lg.lng, "radio": lg.radio}


@app.delete("/api/lugares/{lugar_id}")
def borrar_lugar(lugar_id: int, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        _db.execute("DELETE FROM lugares WHERE id=?", (lugar_id,))
        _db.commit()
    for k in [k for k in _dentro if k[1] == lugar_id]:
        _dentro.pop(k, None)
    return {"ok": True}


@app.post("/api/dispositivos/{uuid}/comando")
def enviar_comando(uuid: str, c: ComandoIn, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    cola = _comandos.setdefault(uuid, [])
    if c.tipo not in cola:
        cola.append(c.tipo)
    return {"ok": True}


@app.get("/api/comandos/{uuid}")
def pedir_comandos(uuid: str, x_token: str = Header(default="")):
    check(GPS_TOKEN, x_token)
    return {"comandos": _comandos.pop(uuid, [])}


# ----------------------------------- Panel ----------------------------------
@app.get("/api/health")
def health():
    return {"ok": True, "dispositivos_conectados_panel": len(manager.active)}


@app.get("/api/dispositivos")
def listar_dispositivos(x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        filas = _db.execute("SELECT * FROM devices ORDER BY ultima_conexion DESC").fetchall()
    ahora = iso(utcnow())
    return [{**dict(f), "ahora": ahora} for f in filas]


@app.put("/api/dispositivos/{uuid}")
def editar_dispositivo(uuid: str, payload: EditarIn, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    nombre = payload.nombre.strip() if payload.nombre else None
    with _lock:
        cur = _db.execute("UPDATE devices SET nombre=COALESCE(?,nombre), color=COALESCE(?,color) WHERE uuid=?",
                          (nombre, payload.color, uuid))
        _db.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    return {"ok": True}


@app.get("/api/historial/{uuid}")
def historial(uuid: str, desde: Optional[str] = None, hasta: Optional[str] = None,
              limit: int = Query(20000, ge=1, le=100000), x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    ahora = utcnow()
    sql = "SELECT lat,lng,accuracy,speed,heading,altitude,battery,ip,timestamp FROM locations WHERE device_uuid=?"
    params: list = [uuid]
    if desde:
        sql += " AND timestamp >= ?"
        params.append(a_iso(desde, ahora))
    if hasta:
        sql += " AND timestamp <= ?"
        params.append(a_iso(hasta, ahora))
    sql += " ORDER BY timestamp ASC LIMIT ?"
    params.append(limit)
    with _lock:
        filas = _db.execute(sql, params).fetchall()
    return [dict(f) for f in filas]


@app.delete("/api/historial/{uuid}")
def borrar_historial(uuid: str, desde: Optional[str] = None, hasta: Optional[str] = None,
                     x_key: str = Header(default="")):
    """Borra los puntos de un dispositivo (en un rango, o todos si no se indica rango)."""
    check(PANEL_TOKEN, x_key)
    ahora = utcnow()
    sql, params = "DELETE FROM locations WHERE device_uuid=?", [uuid]
    if desde:
        sql += " AND timestamp >= ?"
        params.append(a_iso(desde, ahora))
    if hasta:
        sql += " AND timestamp <= ?"
        params.append(a_iso(hasta, ahora))
    with _lock:
        cur = _db.execute(sql, params)
        _db.commit()
    return {"ok": True, "borrados": cur.rowcount}


@app.delete("/api/dispositivos/{uuid}")
def borrar_dispositivo(uuid: str, x_key: str = Header(default="")):
    check(PANEL_TOKEN, x_key)
    with _lock:
        _db.execute("DELETE FROM locations WHERE device_uuid=?", (uuid,))
        _db.execute("DELETE FROM alertas WHERE device_uuid=?", (uuid,))
        _db.execute("DELETE FROM devices WHERE uuid=?", (uuid,))
        _db.commit()
    return {"ok": True}


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket, key: str = ""):
    if PANEL_TOKEN and key != PANEL_TOKEN:
        await websocket.close(code=4401)
        return
    await manager.connect(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), 25)
            except asyncio.TimeoutError:
                await websocket.send_text('{"tipo":"ping"}')  # mantiene viva la conexión tras proxies
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        manager.disconnect(websocket)


# ------------------------------- Frontend -----------------------------------
@app.get("/")
@app.get("/panel")
def panel():
    return FileResponse(FRONTEND_DIR / "viewer.html")


@app.get("/telefono")
def telefono():
    return FileResponse(FRONTEND_DIR / "phone.html")


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
