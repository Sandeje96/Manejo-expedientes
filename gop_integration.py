import os
import sys
import time
import pandas as pd
from datetime import datetime
from flask import current_app
from pathlib import Path

def _ensure_gop_imports():
    """Configura los imports del módulo GOP."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    gop_dir = os.path.join(current_dir, 'gop_scraper')
    
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    if gop_dir not in sys.path:
        sys.path.insert(0, gop_dir)

def _run_gop_scraper():
    """
    Ejecuta el scraper GOP directamente sin imports complejos.
    Retorna la ruta del CSV generado.
    """
    _ensure_gop_imports()
    
    try:
        # Import directo de las funciones del scraper
        from scraper import run_scraper
        return run_scraper()
    except ImportError:
        # Si falla, ejecutamos el código directamente
        return _run_scraper_direct()

def _run_scraper_direct():
    """Versión directa del scraper sin imports de módulos."""
    from dotenv import load_dotenv
    from playwright.sync_api import sync_playwright
    
    # Cargar variables de entorno
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")
    
    if not user or not pw:
        raise RuntimeError("No se encontraron credenciales USER_MUNI/PASS_MUNI en .env")
    
    # Configuración
    BASE = "https://posadas.gestiondeobrasprivadas.com.ar"
    LOGIN_URL = f"{BASE}/frontend/web/site/login"
    MY_TRAYS_URL = f"{BASE}/frontend/web/formality/index-all"
    HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"  # Cambiar a false para debug
    
    # Directorio de salida
    output_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    out_csv = os.path.join(output_dir, f"expedientes_{timestamp}.csv")
    
    all_rows = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()
        
        # Login
        current_app.logger.info("Navegando a página de login...")
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        
        # Verificar que estamos en la página correcta
        if "login" not in page.url.lower():
            raise RuntimeError(f"No se pudo acceder a la página de login. URL actual: {page.url}")
        
        # Llenar usuario - probar múltiples selectores
        filled_user = False
        user_selectors = [
            'input[name="LoginForm[username]"]',
            'input#loginform-username',
            'input[name="username"]',
            'input[type="text"]'
        ]
        
        for selector in user_selectors:
            try:
                if page.locator(selector).count() > 0:
                    page.fill(selector, user)
                    filled_user = True
                    current_app.logger.info(f"Usuario llenado con selector: {selector}")
                    break
            except:
                continue
        
        if not filled_user:
            # Tomar screenshot para debug
            page.screenshot(path="login_debug.png")
            raise RuntimeError("No se pudo llenar el campo de usuario")
        
        # Llenar contraseña - probar múltiples selectores
        filled_pass = False
        pass_selectors = [
            'input[name="LoginForm[password]"]',
            'input#loginform-password',
            'input[name="password"]',
            'input[type="password"]'
        ]
        
        for selector in pass_selectors:
            try:
                if page.locator(selector).count() > 0:
                    page.fill(selector, pw)
                    filled_pass = True
                    current_app.logger.info(f"Contraseña llenada con selector: {selector}")
                    break
            except:
                continue
        
        if not filled_pass:
            page.screenshot(path="login_debug.png")
            raise RuntimeError("No se pudo llenar el campo de contraseña")
        
        # Hacer click en submit - probar múltiples selectores
        submitted = False
        submit_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("Ingresar")',
            'button:has-text("Login")',
            '.btn-primary'
        ]
        
        for selector in submit_selectors:
            try:
                if page.locator(selector).count() > 0:
                    page.click(selector)
                    submitted = True
                    current_app.logger.info(f"Submit con selector: {selector}")
                    break
            except:
                continue
        
        if not submitted:
            page.screenshot(path="login_debug.png")
            raise RuntimeError("No se pudo hacer click en el botón de login")
        
        # Esperar a que se complete el login
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3000)
        
        # Verificar que el login fue exitoso
        current_url = page.url
        current_app.logger.info(f"URL después del login: {current_url}")
        
        if "login" in current_url.lower():
            page.screenshot(path="login_failed.png")
            raise RuntimeError("Login falló - aún en página de login. Verificá credenciales.")
        
        # Ir a la página de bandejas
        current_app.logger.info("Navegando a página de bandejas...")
        try:
            page.goto(MY_TRAYS_URL, wait_until="networkidle")
        except Exception as e:
            current_app.logger.error(f"Error navegando a bandejas: {e}")
            # Intentar navegar por menu si falla la URL directa
            try:
                # Buscar enlace a bandejas en el menú
                page.click('a:has-text("Bandejas")', timeout=5000)
                page.wait_for_load_state("networkidle")
            except:
                page.screenshot(path="navigation_failed.png")
                raise RuntimeError("No se pudo acceder a la página de bandejas")
        
        # Extraer datos de la tabla
        current_app.logger.info("Extrayendo datos de la tabla...")
        page.wait_for_timeout(3000)
        
        try:
            # Buscar la tabla - probar múltiples selectores
            table_found = False
            table_selectors = [
                "table tbody tr",
                ".table tbody tr",
                "#grid tbody tr",
                "tr"
            ]
            
            rows = None
            for selector in table_selectors:
                try:
                    rows = page.locator(selector)
                    count = rows.count()
                    if count > 0:
                        current_app.logger.info(f"Tabla encontrada con selector '{selector}': {count} filas")
                        table_found = True
                        break
                except:
                    continue
            
            if not table_found:
                page.screenshot(path="table_not_found.png")
                current_app.logger.warning("No se encontró tabla de datos")
                return out_csv  # Retornar CSV vacío
            
            count = rows.count()
            current_app.logger.info(f"Procesando {count} filas...")
            
            for i in range(count):
                try:
                    row = rows.nth(i)
                    cells = row.locator("td")
                    cell_count = cells.count()
                    
                    if cell_count >= 6:  # Al menos 6 columnas para datos útiles
                        # Extraer datos de cada celda
                        nro_sistema = cells.nth(0).inner_text().strip() if cell_count > 0 else ""
                        expediente = cells.nth(1).inner_text().strip() if cell_count > 1 else ""
                        estado = cells.nth(2).inner_text().strip() if cell_count > 2 else ""
                        profesional = cells.nth(3).inner_text().strip() if cell_count > 3 else ""
                        nomenclatura = cells.nth(4).inner_text().strip() if cell_count > 4 else ""
                        bandeja_actual = cells.nth(5).inner_text().strip() if cell_count > 5 else ""
                        fecha_entrada = cells.nth(6).inner_text().strip() if cell_count > 6 else ""
                        usuario_asignado = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
                        
                        # Solo agregar si tiene datos útiles
                        if nro_sistema or expediente:
                            all_rows.append({
                                "nro_sistema": nro_sistema,
                                "expediente": expediente,
                                "estado": estado,
                                "profesional": profesional,
                                "nomenclatura": nomenclatura,
                                "bandeja_actual": bandeja_actual,
                                "fecha_entrada": fecha_entrada,
                                "usuario_asignado": usuario_asignado,
                            })
                    
                except Exception as e:
                    current_app.logger.warning(f"Error procesando fila {i}: {e}")
                    continue
                    
        except Exception as e:
            current_app.logger.error(f"Error extrayendo datos: {e}")
            page.screenshot(path="extraction_error.png")
        
        current_app.logger.info(f"Extracción completada: {len(all_rows)} registros")
        
        browser.close()
    
    # Guardar CSV
    df = pd.DataFrame(all_rows)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    
    current_app.logger.info(f"Scraper completado: {len(df)} filas -> {out_csv}")
    return out_csv

def sync_gop_data():
    """
    Ejecuta el scraper GOP y actualiza los expedientes con GOP número.
    Retorna un diccionario con estadísticas del proceso.
    """
    try:
        # Ejecutar el scraper
        csv_path = _run_gop_scraper()
        
        # Leer el CSV generado
        df = pd.read_csv(csv_path)
        
        # Obtener la base de datos desde el contexto de Flask
        from app import _db
        
        # Usar consulta SQL directa en lugar de modelo ORM
        def buscar_expediente_por_gop(gop_numero):
            """Busca un expediente por GOP usando SQL directo."""
            result = _db.session.execute(
                _db.text("SELECT id FROM expedientes WHERE gop_numero = :gop_numero"),
                {"gop_numero": gop_numero}
            ).fetchone()
            return result[0] if result else None
        
        def actualizar_expediente_gop(expediente_id, bandeja, usuario, estado, fecha_entrada):
            """Actualiza los campos GOP de un expediente."""
            _db.session.execute(
                _db.text("""
                    UPDATE expedientes 
                    SET gop_bandeja_actual = :bandeja,
                        gop_usuario_asignado = :usuario,
                        gop_estado = :estado,
                        gop_fecha_entrada = :fecha_entrada,
                        gop_ultima_sincronizacion = :sync_time
                    WHERE id = :expediente_id
                """),
                {
                    "bandeja": bandeja[:200] if bandeja else None,
                    "usuario": usuario[:200] if usuario else None,
                    "estado": estado[:100] if estado else None,
                    "fecha_entrada": fecha_entrada,
                    "sync_time": datetime.utcnow(),
                    "expediente_id": expediente_id
                }
            )
        
        # Estadísticas
        stats = {
            'total_gop_encontrados': len(df),
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': 0,
            'errores': []
        }
        
        # Procesar cada fila del CSV
        for _, row in df.iterrows():
            try:
                nro_sistema = str(row.get('nro_sistema', '')).strip()
                if not nro_sistema:
                    continue
                
                # Buscar expediente por gop_numero
                expediente_id = buscar_expediente_por_gop(nro_sistema)
                
                if expediente_id:
                    # Parsear fecha_entrada si existe
                    fecha_entrada = None
                    fecha_str = str(row.get('fecha_entrada', '')).strip()
                    if fecha_str and fecha_str != 'nan':
                        try:
                            # Intentar varios formatos de fecha
                            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
                                try:
                                    fecha_entrada = datetime.strptime(fecha_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                        except Exception as e:
                            stats['errores'].append(f"Error parseando fecha para GOP {nro_sistema}: {e}")
                    
                    # Actualizar campos GOP
                    actualizar_expediente_gop(
                        expediente_id,
                        str(row.get('bandeja_actual', '')),
                        str(row.get('usuario_asignado', '')),
                        str(row.get('estado', '')),
                        fecha_entrada
                    )
                    
                    stats['expedientes_actualizados'] += 1
                else:
                    stats['expedientes_no_encontrados'] += 1
                    
            except Exception as e:
                stats['errores'].append(f"Error procesando GOP {row.get('nro_sistema', 'desconocido')}: {e}")
        
        # Guardar cambios
        _db.session.commit()
        
        # Limpiar archivo CSV temporal (opcional)
        try:
            os.remove(csv_path)
        except:
            pass
            
        return stats
        
    except Exception as e:
        current_app.logger.error(f"Error en sync_gop_data: {e}")
        return {
            'error': str(e),
            'total_gop_encontrados': 0,
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': 0,
            'errores': [str(e)]
        }