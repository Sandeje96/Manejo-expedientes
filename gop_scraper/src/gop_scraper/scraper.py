
import os, time
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from .config import LOGIN_URL, MY_TRAYS_URL, HEADLESS, DOWNLOAD_PDFS, OUTPUT_DIR, DOWNLOAD_DIR
from . import selectors as S
from .utils import timestamp, ensure_dir

def _mask(s, show=2):
    if not s: return "(vacío)"
    return s[:show] + "•"*(max(0,len(s)-show))

def _load_env():
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")
    if not user or not pw:
        root_env = Path(__file__).resolve().parents[2] / ".env"
        if root_env.exists():
            load_dotenv(dotenv_path=root_env, override=True)
            user = os.getenv("USER_MUNI", "")
            pw = os.getenv("PASS_MUNI", "")
    print(f"[CFG] USER_MUNI={_mask(user)}  PASS_MUNI={_mask(pw)}")
    if not user or not pw:
        raise RuntimeError("No se pudieron cargar USER_MUNI / PASS_MUNI desde .env. Verificá que estén en la raíz del proyecto y sin comillas.")
    return user, pw

def _login(page, user, pw):
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(400)
    filled_user = False
    filled_pass = False
    for sel in S.LOGIN_USER.split(","):
        sel = sel.strip()
        try:
            page.wait_for_selector(sel, timeout=3000)
            page.fill(sel, user, timeout=3000)
            filled_user = True
            break
        except Exception:
            continue
    if not filled_user:
        try:
            page.get_by_label(S.USER_LABEL).fill(user, timeout=2000)
            filled_user = True
        except Exception:
            pass
    if not filled_user:
        try:
            page.get_by_placeholder(S.USER_PLACEHOLDER).fill(user, timeout=2000)
            filled_user = True
        except Exception:
            pass

    for sel in S.LOGIN_PASS.split(","):
        sel = sel.strip()
        try:
            page.wait_for_selector(sel, timeout=3000)
            page.fill(sel, pw, timeout=3000)
            filled_pass = True
            break
        except Exception:
            continue
    if not filled_pass:
        try:
            page.get_by_label(S.PASS_LABEL).fill(pw, timeout=2000)
            filled_pass = True
        except Exception:
            pass
    if not filled_pass:
        try:
            page.get_by_placeholder(S.PASS_PLACEHOLDER).fill(pw, timeout=2000)
            filled_pass = True
        except Exception:
            pass

    if not (filled_user and filled_pass):
        page.screenshot(path=str(Path(OUTPUT_DIR)/"login_screen.png"))
        raise RuntimeError("No pude ubicar los campos de login. Revisá selectors.py o enviame una captura de la pantalla de login.")

    try:
        page.click(S.LOGIN_SUBMIT, timeout=3000)
    except Exception:
        page.get_by_role("button", name=lambda s: "Ingresar" in s or "Login" in s).click(timeout=3000)

    page.wait_for_load_state("networkidle")

def _collect_page_rows(page):
    try:
        page.wait_for_selector(S.TABLE_ROWS, timeout=7000)
    except Exception:
        pass
    rows = page.locator(S.TABLE_ROWS)
    n = rows.count()
    collected = []
    for i in range(n):
        r = rows.nth(i)
        tds = r.locator("td")
        cols = tds.count()
        get = lambda j: (tds.nth(j).inner_text().strip() if j < cols else "")
        url_detalle = ""
        try:
            link = tds.nth(8).locator("a").first
            if link:
                href = link.get_attribute("href")
                url_detalle = href or ""
        except Exception:
            pass
        collected.append({
            "nro_sistema": get(0),
            "expediente": get(1),
            "estado": get(2),
            "profesional": get(3),
            "nomenclatura": get(4),
            "bandeja_actual": get(5),
            "fecha_entrada": get(6),
            "usuario_asignado": get(7),
            "url_detalle": url_detalle,
        })
    return collected

def _try_download_pdfs(page, row):
    if not row.get("url_detalle"):
        return 0
    count = 0
    try:
        page.goto(row["url_detalle"], wait_until="domcontentloaded")
        links = page.locator("a")
        total = links.count()
        for i in range(total):
            a = links.nth(i)
            txt = (a.inner_text() or "").strip().lower()
            href = a.get_attribute("href") or ""
            if any(k in txt for k in ["pdf", "descargar"]) or href.lower().endswith(".pdf"):
                target_dir = ensure_dir(os.path.join(DOWNLOAD_DIR, row.get("nro_sistema") or "sin_numero"))
                with page.expect_download(timeout=5000) as dl:
                    try:
                        a.click()
                    except Exception:
                        continue
                download = dl.value
                filename = download.suggested_filename or f"archivo_{int(time.time())}.pdf"
                download.save_as(os.path.join(target_dir, filename))
                count += 1
    except Exception:
        pass
    return count

def run_scraper():
    user, pw = _load_env()
    ensure_dir(OUTPUT_DIR); ensure_dir(DOWNLOAD_DIR)
    out_csv = os.path.join(OUTPUT_DIR, f"expedientes_{timestamp()}.csv")
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        _login(page, user, pw)

        page.goto(MY_TRAYS_URL, wait_until="networkidle")

        page_idx = 1
        while True:
            current = _collect_page_rows(page)
            all_rows += current

            from .selectors import PAGINATION_NEXT
            next_btn = None
            try:
                next_btn_loc = page.locator(PAGINATION_NEXT)
                if next_btn_loc.count() > 0:
                    next_btn = next_btn_loc.first
            except Exception:
                next_btn = None

            if next_btn:
                try:
                    next_btn.click()
                    page.wait_for_load_state("networkidle")
                    page_idx += 1
                    continue
                except PWTimeout:
                    pass
            break

        df = pd.DataFrame(all_rows)
        df = df[["nro_sistema","expediente","profesional","bandeja_actual","fecha_entrada","usuario_asignado"]]
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        print(f"[OK] Filas: {len(df)} -> {out_csv}")
        if len(all_rows)==0:
            from pathlib import Path
            page.screenshot(path=str(Path(OUTPUT_DIR)/"my_trays_screen.png"))
            print(f"[INFO] Guardé captura: {Path(OUTPUT_DIR)/'my_trays_screen.png'} para diagnóstico.")
        browser.close()

    return out_csv
