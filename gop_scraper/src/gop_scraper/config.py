
import os

BASE = "https://posadas.gestiondeobrasprivadas.com.ar"
LOGIN_URL = f"{BASE}/frontend/web/site/login"
MY_TRAYS_URL = f"{BASE}/frontend/web/formality/index-all"

USER = os.getenv("USER_MUNI", "")
PASS = os.getenv("PASS_MUNI", "")

HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
DOWNLOAD_PDFS = os.getenv("DOWNLOAD_PDFS", "false").lower() == "true"

OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
DOWNLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "downloads"))
