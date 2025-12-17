# PAW Patrol – Charaktere (Cloud Run)

Ein kleiner Flask-Service für data-tales.dev, der ein lokales Crawler-Datenset (`characters.json` + Bilder) als Galerie darstellt und zusätzlich JSON-Endpunkte bereitstellt.

## Lokal starten

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
python main.py
