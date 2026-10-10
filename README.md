# GPS Tracker

Visor de GPS casero: los celulares reportan su ubicación desde una página web
(sin instalar nada), y un panel muestra su posición en vivo, historial de rutas
y estadísticas básicas.

**No pensado para producción.** Sin autenticación robusta, CORS abierto, todo
simplificado a propósito. Solo debe funcionar para uso personal/pruebas.

---

## 1. Estructura del proyecto

```
gps-tracker/
├── backend/
│   ├── main.py            # API + WebSocket + servidor de las páginas
│   └── requirements.txt
├── frontend/
│   ├── viewer.html        # Panel: mapa en vivo + historial   -> /panel
│   └── phone.html         # Página que abre cada celular      -> /telefono
└── README.md
```

## 2. Instalación en el servidor (Oracle Cloud, Ubuntu)

Conéctate por SSH (o usa la extensión Remote-SSH de VS Code) y ejecuta:

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip

cd ~
# sube o clona la carpeta gps-tracker aquí, luego:
cd gps-tracker/backend

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Prueba rápida (Ctrl+C para detener)
uvicorn main:app --host 0.0.0.0 --port 8000
```

Si ves `Uvicorn running on http://0.0.0.0:8000`, el backend funciona. Abre
`http://IP_DE_TU_SERVIDOR:8000/panel` desde tu PC para comprobarlo (el mapa
se verá vacío porque todavía no hay dispositivos).

## 3. Abrir el puerto en Oracle Cloud

Oracle Cloud bloquea todo por defecto. Necesitas dos pasos:

**a) Security List / Network Security Group (en la consola web de Oracle Cloud):**
Ve a tu instancia → Subred → Security List → "Add Ingress Rule":
- Source CIDR: `0.0.0.0/0`
- Puerto destino: `8000` (o `80`/`443` si usas Caddy, ver paso 4)

**b) Firewall del propio Ubuntu:**
```bash
sudo ufw allow 8000/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable   # si no estaba activo
```

## 4. HTTPS obligatorio para que el GPS funcione en el celular

Los navegadores móviles **bloquean la Geolocation API en sitios sin HTTPS**
(excepto en `localhost`). Sin esto, `phone.html` nunca va a pedir permiso de
ubicación. La forma más simple sin comprar dominio:

```bash
# 1. Instalar Caddy (genera HTTPS automático con Let's Encrypt)
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install -y caddy

# 2. Averigua la IP pública de tu instancia (la que ya usas para SSH), ej: 140.10.20.30
```

Edita `/etc/caddy/Caddyfile` y deja solo esto (cambia la IP por la tuya,
usando guiones en vez de puntos):

```
140-10-20-30.sslip.io {
    reverse_proxy localhost:8000
}
```

`sslip.io` es un servicio gratuito que resuelve automáticamente
`X-X-X-X.sslip.io` hacia la IP `X.X.X.X` — no necesitas comprar dominio ni
configurar DNS.

```bash
sudo systemctl restart caddy
```

Caddy sacará el certificado HTTPS solo. A partir de ahora:
- Panel: `https://140-10-20-30.sslip.io/panel`
- Celular: `https://140-10-20-30.sslip.io/telefono`

## 5. Dejar el backend corriendo siempre (systemd)

```bash
sudo tee /etc/systemd/system/gps-tracker.service > /dev/null <<EOF
[Unit]
Description=GPS Tracker backend
After=network.target

[Service]
User=$USER
WorkingDirectory=/home/$USER/gps-tracker/backend
ExecStart=/home/$USER/gps-tracker/backend/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now gps-tracker
sudo systemctl status gps-tracker
```

Para ver logs en vivo: `journalctl -u gps-tracker -f`

## 6. Uso

1. Desde cada celular, abre `https://TU-DOMINIO-SSLIP/telefono`, acepta el
   permiso de ubicación, y opcionalmente ponle un nombre al dispositivo.
   Deja la pestaña abierta (funciona en segundo plano mientras el navegador
   no se cierre del todo; si el sistema mata la pestaña, vuelve a abrirla).
2. Desde tu PC, abre `https://TU-DOMINIO-SSLIP/panel` — ahí verás el
   dispositivo aparecer en el mapa y en la lista lateral en tiempo real.
3. Selecciona un dispositivo en la lista, elige un rango de fechas y pulsa
   "Ver ruta" para trazar su historial con distancia recorrida.

## 7. Novedades de la v2

- **Más celulares a la vez**: SQLite en modo WAL, envío por lotes (`POST /api/ubicaciones`) y difusión en paralelo por WebSocket.
- **Cola offline** en `/telefono`: sin internet los puntos se guardan y se suben al volver la conexión, con su hora real.
- **Pantalla activa** (Wake Lock) y ahorro de datos: solo envía si te mueves 8 m o pasan ~15 s.
- **Panel**: batería, velocidad y precisión, estela en vivo, "Seguir en vivo", exportar GPX, "en línea" que se refresca solo, diseño para móvil y nombres escapados (sin XSS).
- **IP real** detrás de Caddy (`X-Forwarded-For`), validación de coordenadas y borrado automático del historial (`RETENTION_DAYS`).
- **Claves opcionales**: `GPS_TOKEN` (celulares/app) y `PANEL_TOKEN` (panel). Con claves:
  `/telefono?token=CLAVE` y `/panel?key=CLAVE` (se recuerdan en el navegador). En systemd añade
  `Environment=GPS_TOKEN=... PANEL_TOKEN=...` en la sección `[Service]`.
- Si ya tenías `gps.db`, se actualiza sola (migración automática).

## 8. Pendiente / ideas

- Geocercas con alertas y reproducción animada de la ruta.
- Tracking 100 % confiable en segundo plano: usar la app Flutter (`geo_app`) con servicio en primer plano.
