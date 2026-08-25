# SmartHome Dashboard

Ein Smart-Home-Dashboard als **einzelne Python-Datei** — PV-Anlage (Fronius),
Bitcoin-Miner (Braiins OS+ / Bitmain), Kameras mit optionaler
KI-Personenerkennung (YOLO), Strompreise (aWATTar), Bitcoin-Kurs, Wetter
und eine kleine Familien-Organisation (Einkaufsliste, Kalender, Notizen) —
alles inklusive Web-Oberfläche in `smarthome_all_in_one.py`.

Trotz der einen Datei ist der Code intern sauber in Module getrennt
(Datenbank, Fronius, Miner, externe APIs, Automatisierung, Heimüberwachung
usw.) — jedes Modul läuft in seinem eigenen, isolierten Namensraum, wie ein
echter Python-Import. Das macht die Datei zwar lang, aber leicht in einem
Rutsch zu kopieren/deployen (z. B. auf einen Raspberry Pi).

## ✨ Features

- **PV-Überwachung** live vom Fronius-Wechselrichter (Solar API)
- **Miner-Automatisierung**: Bitcoin-Miner automatisch bei PV-Überschuss
  starten/stoppen (mehrere Trigger-Quellen: Überschuss, Netzbezug,
  Erzeugung, Akku-Ladestand)
- **Heimüberwachung**: Kamera-Integration (MJPEG/RTSP) mit optionaler
  YOLO-Personenerkennung, Aufnahmen und Benachrichtigungen
- **Strompreise** (aWATTar) und **Bitcoin-Kurs** in Echtzeit
- **Wetter & Solarprognose** inkl. optionaler "Morgennachricht"
- **Familien-Organisation**: Einkaufsliste, Kalender, Notizen
- **Benachrichtigungen** über E-Mail, ntfy, Telegram oder Browser-Push
- Personalisierbare Oberfläche (Akzentfarbe, Hintergrund, Widgets),
  Befehlspalette (<kbd>Strg</kbd>+<kbd>K</kbd>), Tastenkürzel
- **🆕 Demo-Modus**: Dashboard mit erfundenen Beispieldaten starten, ganz
  ohne eigene Hardware — perfekt zum Ausprobieren oder für Screenshots
- **🆕 CSV-Export**: Energie-Verlauf, Tages-Zusammenfassungen und
  Miner-Statistiken als CSV herunterladen (Einstellungen → Daten)
- **🆕 Health-Check-Endpoint** (`/api/health`) für Docker/Uptime-Kuma/
  Watchtower

## 📋 Voraussetzungen

- Python 3.10+
- Optional für Personenerkennung: eine halbwegs aktuelle CPU (oder GPU)
  sowie ~2 GB freier Speicher für das YOLO-Modell

## 🚀 Installation

```bash
git clone https://github.com/<dein-nutzername>/<dein-repo>.git
cd <dein-repo>
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
python3 smarthome_all_in_one.py
```

Das Dashboard läuft danach auf `http://<ip-dieses-rechners>:5000`.

Die SQLite-Datenbank wird beim ersten Start automatisch unter
`instance/smarthome.db` angelegt — es ist kein separater Datenbankserver
nötig.

### Ohne eigene Hardware ausprobieren (Demo-Modus)

```bash
DEMO_MODE=1 python3 smarthome_all_in_one.py
```

Füllt eine leere Datenbank einmalig mit plausiblen Beispieldaten
(Energieverlauf, Bitcoin-Kurs, Einkaufsliste, Kalender). Bereits
vorhandene echte Daten werden dabei **nie** überschrieben.

### Mit Docker

```bash
docker build -t smarthome-dashboard .
docker run -d --name smarthome \
  -p 5000:5000 \
  -v smarthome_data:/app/instance \
  -e SMARTHOME_SECRET="<zufälliger-string>" \
  smarthome-dashboard
```

## ⚙️ Einrichtung nach dem ersten Start

1. **Fronius Solar API aktivieren**: Am Wechselrichter unter
   *Menü → Netzwerk → Solar API → Aktiviert*. Danach die IP-Adresse
   des Wechselrichters unter *Einstellungen → PV-Anlage* eintragen.
2. **Miner hinzufügen**: Über *Miner → Hinzufügen* — IP-Adresse,
   Firmware (Braiins OS+ oder Bitmain) und die Automatisierungs-Regeln
   (z. B. "ab 500 W PV-Überschuss einschalten") festlegen.
3. **Kameras hinzufügen**: Über *Heimüberwachung → Kameras* — MJPEG-
   oder RTSP-URL eintragen (Format: `rtsp://benutzer:passwort@ip:554/stream1`).
   Personenerkennung optional in *Einstellungen → Überwachung* aktivieren
   (Neustart des Dashboards nötig, lädt dabei das Erkennungsmodell).
4. **Benachrichtigungen konfigurieren**: Unter *Heimüberwachung →
   Einstellungen → Benachrichtigungen* zwischen E-Mail, ntfy oder
   Telegram wählen. Für Gmail wird ein **App-Passwort** benötigt
   (nicht das normale Konto-Passwort) — siehe
   Google-Kontoeinstellungen → Sicherheit → App-Passwörter.
5. **Browser-Push** (optional): zusätzlich `pip install pywebpush`
   installieren, danach im selben Menü aktivierbar.
6. **API-Zugriff von außen** (z. B. für Home Assistant): Unter
   *Heimüberwachung → Einstellungen* findest du den API-Schlüssel für
   `/api/dashboard?api_key=...` bzw. den Header `X-API-Key`.

## 🔐 Sicherheitshinweise

- Setze unbedingt die Umgebungsvariable `SMARTHOME_SECRET` auf einen
  eigenen, zufälligen Wert (Standard ist nur für lokale Tests gedacht).
  Erzeugen z. B. mit: `python3 -c "import secrets; print(secrets.token_hex(32))"`
- Die Datei `instance/smarthome.db` enthält deine echten Zugangsdaten
  (Miner-Passwörter, Kamera-URLs, ggf. E-Mail-Zugangsdaten) — sie ist in
  `.gitignore` bereits ausgeschlossen und darf **nie** committed werden.
- Standardmäßig lauscht der Server auf `0.0.0.0:5000` ohne eigenes
  Login — für den Einsatz außerhalb des Heimnetzes wird ein
  Reverse-Proxy mit Zugriffsschutz (z. B. nginx mit Basic-Auth, oder ein
  VPN wie WireGuard/Tailscale) dringend empfohlen.

## 📤 Datenexport

Unter *Einstellungen → Daten* lassen sich herunterladen:
- Energie-Verlauf der letzten 24h/7 Tage als CSV
- Tages-Zusammenfassungen (PV-Ertrag, Eigenverbrauch, Einspeisung, …)
- Miner-Statistiken pro Gerät über `/api/export/miner-stats/<id>.csv`

## 🩺 Health-Check

`GET /api/health` liefert `{"status": "ok", ...}` — geeignet für
Docker-`HEALTHCHECK`, [Uptime Kuma](https://github.com/louislam/uptime-kuma)
oder Watchtower.

## 🗂️ Projektstruktur

```
smarthome_all_in_one.py   # Backend + Web-Oberfläche, alles in einer Datei
requirements.txt          # Python-Abhängigkeiten
Dockerfile                # Optionales Container-Setup
instance/                 # wird automatisch angelegt (SQLite-DB, .gitignore't)
```

## 📄 Lizenz

Noch keine Lizenz vergeben — füge z. B. eine `LICENSE`-Datei (MIT/Apache-2.0)
hinzu, bevor du das Repository veröffentlichst, falls andere den Code
verwenden dürfen sollen.
