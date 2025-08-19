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
    MY_TRAYS_URL = f"{BASE}/frontend/web/site/my-trays"
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
        # Obtener la base de datos desde el contexto de Flask
        from app import _db
        
        # === PASO 1: OBTENER TODOS LOS GOP DEL CPIM ===
        current_app.logger.info("=== OBTENIENDO NÚMEROS GOP DEL CPIM ===")
        
        gop_numbers = _db.session.execute(
            _db.text("""
                SELECT DISTINCT gop_numero 
                FROM expedientes 
                WHERE gop_numero IS NOT NULL 
                AND gop_numero != '' 
                AND (finalizado = false OR finalizado IS NULL)
            """)
        ).fetchall()
        
        gop_list = [row[0].strip() for row in gop_numbers if row[0] and row[0].strip()]
        current_app.logger.info(f"GOP encontrados en CPIM (no finalizados): {len(gop_list)} -> {gop_list}")
        
        if not gop_list:
            return {
                'total_gop_encontrados': 0,
                'expedientes_actualizados': 0,
                'expedientes_no_encontrados': 0,
                'desde_mis_bandejas': 0,
                'desde_todos_tramites': 0,
                'errores': ['No hay expedientes con números GOP en el CPIM']
            }
        
        # === PASO 2: EJECUTAR SCRAPER DIRIGIDO ===
        current_app.logger.info("=== INICIANDO SCRAPER DIRIGIDO ===")
        resultados = _buscar_gops_especificos(gop_list)
        
        # === PASO 3: ACTUALIZAR BASE DE DATOS ===
        stats = {
            'total_gop_encontrados': len(resultados),
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': len(gop_list) - len(resultados),
            'desde_mis_bandejas': 0,
            'desde_todos_tramites': 0,
            'errores': []
        }
        
        for gop_numero, datos in resultados.items():
            try:
                # Buscar expediente por gop_numero
                expediente_id = _db.session.execute(
                    _db.text("SELECT id FROM expedientes WHERE gop_numero = :gop_numero"),
                    {"gop_numero": gop_numero}
                ).fetchone()
                
                if expediente_id:
                    current_app.logger.info(f"Actualizando expediente ID {expediente_id[0]} con GOP {gop_numero}")
                    
                    # Parsear fecha_entrada si existe
                    fecha_entrada = None
                    fecha_str = str(datos.get('fecha_entrada', '')).strip()
                    if fecha_str and fecha_str != 'nan':
                        try:
                            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S'):
                                try:
                                    if 'T' in fecha_str or ':' in fecha_str:
                                        # Si tiene formato datetime, extraer solo la fecha
                                        fecha_entrada = datetime.strptime(fecha_str[:10], '%Y-%m-%d').date()
                                    else:
                                        fecha_entrada = datetime.strptime(fecha_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                        except Exception as e:
                            stats['errores'].append(f"Error parseando fecha_entrada para GOP {gop_numero}: {e}")
                    
                    # Parsear fecha_en_bandeja si existe (NUEVO)
                    fecha_en_bandeja = None
                    fecha_bandeja_str = str(datos.get('fecha_en_bandeja', '')).strip()
                    if fecha_bandeja_str and fecha_bandeja_str != 'nan':
                        try:
                            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S'):
                                try:
                                    if 'T' in fecha_bandeja_str or ':' in fecha_bandeja_str:
                                        # Si tiene formato datetime, extraer solo la fecha
                                        fecha_en_bandeja = datetime.strptime(fecha_bandeja_str[:10], '%Y-%m-%d').date()
                                    else:
                                        fecha_en_bandeja = datetime.strptime(fecha_bandeja_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                        except Exception as e:
                            stats['errores'].append(f"Error parseando fecha_en_bandeja para GOP {gop_numero}: {e}")
                    
                    # Determinar usuario según la fuente
                    fuente = datos.get('fuente', 'Desconocido')
                    if fuente == 'Mis Bandejas':
                        # Usar el usuario real del scraper
                        usuario_gop = str(datos.get('usuario_asignado', ''))[:200]
                        stats['desde_mis_bandejas'] += 1
                    elif fuente == 'Todos los Trámites':
                        # Usar "Profesional" por defecto
                        usuario_gop = "Profesional"
                        stats['desde_todos_tramites'] += 1
                    else:
                        # Fallback para fuentes desconocidas
                        usuario_gop = str(datos.get('usuario_asignado', ''))[:200]
                    
                    current_app.logger.info(f"GOP {gop_numero} - Fuente: {fuente}, Usuario asignado: {usuario_gop}")
                    current_app.logger.info(f"  Fecha entrada: {fecha_entrada}, Fecha en bandeja: {fecha_en_bandeja}")
                    
                    # Actualizar campos GOP (incluyendo el nuevo campo)
                    _db.session.execute(
                        _db.text("""
                            UPDATE expedientes 
                            SET gop_bandeja_actual = :bandeja,
                                gop_usuario_asignado = :usuario,
                                gop_estado = :estado,
                                gop_fecha_entrada = :fecha_entrada,
                                gop_fecha_en_bandeja = :fecha_en_bandeja,
                                gop_ultima_sincronizacion = :sync_time
                            WHERE id = :expediente_id
                        """),
                        {
                            "bandeja": str(datos.get('bandeja_actual', ''))[:200],
                            "usuario": usuario_gop,  # Usar el usuario determinado según la fuente
                            "estado": str(datos.get('estado', ''))[:100],
                            "fecha_entrada": fecha_entrada,
                            "fecha_en_bandeja": fecha_en_bandeja,  # NUEVO CAMPO
                            "sync_time": datetime.utcnow(),
                            "expediente_id": expediente_id[0]
                        }
                    
                    )
                    try:
                            for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
                                try:
                                    fecha_entrada = datetime.strptime(fecha_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                    except Exception as e:
                            stats['errores'].append(f"Error parseando fecha para GOP {gop_numero}: {e}")
                    
                    # Determinar usuario según la fuente
                    fuente = datos.get('fuente', 'Desconocido')
                    if fuente == 'Mis Bandejas':
                        # Usar el usuario real del scraper
                        usuario_gop = str(datos.get('usuario_asignado', ''))[:200]
                        stats['desde_mis_bandejas'] += 1
                    elif fuente == 'Todos los Trámites':
                        # Usar "Profesional" por defecto
                        usuario_gop = "Profesional"
                        stats['desde_todos_tramites'] += 1
                    else:
                        # Fallback para fuentes desconocidas
                        usuario_gop = str(datos.get('usuario_asignado', ''))[:200]
                    
                    current_app.logger.info(f"GOP {gop_numero} - Fuente: {fuente}, Usuario asignado: {usuario_gop}")
                    
                    # Actualizar campos GOP
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
                            "bandeja": str(datos.get('bandeja_actual', ''))[:200],
                            "usuario": usuario_gop,  # Usar el usuario determinado según la fuente
                            "estado": str(datos.get('estado', ''))[:100],
                            "fecha_entrada": fecha_entrada,
                            "sync_time": datetime.utcnow(),
                            "expediente_id": expediente_id[0]
                        }
                    )
                    
                    stats['expedientes_actualizados'] += 1
                    
            except Exception as e:
                stats['errores'].append(f"Error actualizando GOP {gop_numero}: {e}")
        
        # Guardar cambios
        _db.session.commit()
        current_app.logger.info(f"Sincronización completada: {stats}")
        
        return stats
        
    except Exception as e:
        current_app.logger.error(f"Error en sync_gop_data: {e}")
        return {
            'error': str(e),
            'total_gop_encontrados': 0,
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': 0,
            'desde_mis_bandejas': 0,
            'desde_todos_tramites': 0,
            'errores': [str(e)]
        }


def _buscar_gops_especificos(gop_list):
    """
    Busca números GOP específicos en ambas páginas del sistema municipal.
    Retorna un diccionario con los datos encontrados.
    """
    from dotenv import load_dotenv
    from playwright.sync_api import sync_playwright
    import subprocess
    import sys
    
    # Verificar e instalar navegadores si es necesario
    try:
        current_app.logger.info("Verificando navegadores de Playwright...")
        
        # Intentar crear un contexto para verificar si los navegadores están instalados
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                browser.close()
                current_app.logger.info("✓ Navegadores disponibles")
            except Exception as browser_error:
                current_app.logger.info("Navegadores no encontrados, instalando...")
                
                # Instalar navegadores automáticamente
                result = subprocess.run([
                    sys.executable, "-m", "playwright", "install", "chromium"
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode != 0:
                    raise RuntimeError(f"Error instalando navegadores: {result.stderr}")
                
                current_app.logger.info("✓ Navegadores instalados exitosamente")
    
    except Exception as install_error:
        current_app.logger.error(f"Error con navegadores: {install_error}")
        raise RuntimeError(f"No se pudieron instalar los navegadores de Playwright: {install_error}")
    
    # Cargar variables de entorno
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")
    
    current_app.logger.info(f"Credenciales cargadas - Usuario: {user[:3]}*** Contraseña: {'*' * len(pw) if pw else 'VACÍA'}")
    
    if not user or not pw:
        raise RuntimeError("No se encontraron credenciales USER_MUNI/PASS_MUNI en .env")
    
    if len(user.strip()) < 3:
        raise RuntimeError("El usuario parece demasiado corto. Verificá USER_MUNI en .env")
    
    if len(pw.strip()) < 3:
        raise RuntimeError("La contraseña parece demasiado corta. Verificá PASS_MUNI en .env")
    
    # Configuración
    BASE = "https://posadas.gestiondeobrasprivadas.com.ar"
    LOGIN_URL = f"{BASE}/frontend/web/site/login"
    MY_TRAYS_URL = f"{BASE}/frontend/web/site/my-trays"
    ALL_FORMALITIES_URL = f"{BASE}/frontend/web/formality/index-all"
    HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"  # En producción usar headless
    
    resultados = {}
    gops_pendientes = set(gop_list)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # === LOGIN ===
            current_app.logger.info("=== REALIZANDO LOGIN ===")
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            
            # Login process
            _perform_login(page, user, pw)
            
            # === BUSCAR EN MIS BANDEJAS ===
            current_app.logger.info("=== BUSCANDO EN MIS BANDEJAS ===")
            try:
                page.goto(MY_TRAYS_URL, wait_until="networkidle")
                encontrados_bandejas = _buscar_gops_en_pagina(page, gops_pendientes, "Mis Bandejas")
                resultados.update(encontrados_bandejas)
                
                # Remover GOP encontrados de la lista pendiente
                for gop in encontrados_bandejas.keys():
                    gops_pendientes.discard(gop)
                    
                current_app.logger.info(f"Encontrados en Mis Bandejas: {len(encontrados_bandejas)}")
                current_app.logger.info(f"Pendientes para búsqueda: {len(gops_pendientes)}")
                
            except Exception as e:
                current_app.logger.error(f"Error en Mis Bandejas: {e}")
                page.screenshot(path="mis_bandejas_error.png")
            
            # === BUSCAR EN TODOS LOS TRÁMITES (solo los pendientes) ===
            if gops_pendientes:
                current_app.logger.info(f"=== BUSCANDO EN TODOS LOS TRÁMITES ({len(gops_pendientes)} pendientes) ===")
                try:
                    page.goto(ALL_FORMALITIES_URL, wait_until="networkidle")
                    page.wait_for_timeout(3000)
                    encontrados_todos = _buscar_gops_en_pagina(page, gops_pendientes, "Todos los Trámites")
                    resultados.update(encontrados_todos)
                    
                    current_app.logger.info(f"Encontrados en Todos los Trámites: {len(encontrados_todos)}")
                    
                except Exception as e:
                    current_app.logger.error(f"Error en Todos los Trámites: {e}")
                    page.screenshot(path="todos_tramites_error.png")
        
        finally:
            browser.close()
    
    current_app.logger.info(f"Total encontrados: {len(resultados)} de {len(gop_list)} GOP buscados")
    return resultados


def _perform_login(page, user, pw):
    """Realiza el login en el sistema."""
    current_app.logger.info("Iniciando proceso de login...")
    
    # Verificar que estamos en la página correcta
    if "login" not in page.url.lower():
        raise RuntimeError(f"No se pudo acceder a la página de login. URL actual: {page.url}")
    
    # Llenar usuario - probar múltiples selectores
    filled_user = False
    user_selectors = [
        'input[name="LoginForm[username]"]',
        'input#loginform-username',
        'input[name="username"]',
        'input[type="text"]',
        'input[placeholder*="usuario" i]',
        'input[placeholder*="nombre" i]'
    ]
    
    for selector in user_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.fill(selector, user)
                filled_user = True
                current_app.logger.info(f"Usuario llenado con selector: {selector}")
                break
        except Exception as e:
            current_app.logger.debug(f"Falló selector de usuario {selector}: {e}")
            continue
    
    if not filled_user:
        page.screenshot(path="login_user_debug.png")
        raise RuntimeError("No se pudo llenar el campo de usuario")
    
    # Esperar un poco después de llenar usuario
    page.wait_for_timeout(500)
    
    # Llenar contraseña - probar múltiples selectores
    filled_pass = False
    pass_selectors = [
        'input[name="LoginForm[password]"]',
        'input#loginform-password',
        'input[name="password"]',
        'input[type="password"]',
        'input[placeholder*="contraseña" i]',
        'input[placeholder*="password" i]'
    ]
    
    for selector in pass_selectors:
        try:
            if page.locator(selector).count() > 0:
                page.fill(selector, pw)
                filled_pass = True
                current_app.logger.info(f"Contraseña llenada con selector: {selector}")
                break
        except Exception as e:
            current_app.logger.debug(f"Falló selector de contraseña {selector}: {e}")
            continue
    
    if not filled_pass:
        page.screenshot(path="login_pass_debug.png")
        raise RuntimeError("No se pudo llenar el campo de contraseña")
    
    # Esperar un poco después de llenar contraseña
    page.wait_for_timeout(500)
    
    # Hacer click en submit - probar múltiples selectores
    submitted = False
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Ingresar")',
        'button:has-text("Login")',
        'button:has-text("Entrar")',
        '.btn-primary',
        '.btn[type="submit"]',
        'form button'
    ]
    
    for selector in submit_selectors:
        try:
            if page.locator(selector).count() > 0:
                current_app.logger.info(f"Intentando submit con selector: {selector}")
                page.click(selector)
                submitted = True
                current_app.logger.info(f"Submit exitoso con selector: {selector}")
                break
        except Exception as e:
            current_app.logger.debug(f"Falló selector de submit {selector}: {e}")
            continue
    
    if not submitted:
        page.screenshot(path="login_submit_debug.png")
        raise RuntimeError("No se pudo hacer click en el botón de login")
    
    # Esperar a que se complete el login
    current_app.logger.info("Esperando respuesta del login...")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)
    
    # Verificar que el login fue exitoso
    current_url = page.url
    current_app.logger.info(f"URL después del login: {current_url}")
    
    # Verificar diferentes indicadores de login exitoso
    if "login" in current_url.lower():
        # Tomar screenshot para debug
        page.screenshot(path="login_failed_debug.png")
        
        # Verificar si hay mensajes de error en la página
        try:
            error_messages = page.locator('.alert-danger, .error, .alert-error, .help-block-error').all_inner_texts()
            if error_messages:
                current_app.logger.error(f"Mensajes de error en login: {error_messages}")
        except:
            pass
        
        raise RuntimeError("Login falló - aún en página de login. Verificá credenciales en el .env")
    
    # Verificar que hay contenido de usuario logueado
    page.wait_for_timeout(2000)
    
    # Buscar indicadores de login exitoso
    login_indicators = [
        'a:has-text("Salir")',
        'a:has-text("Logout")',
        'a:has-text("Cerrar Sesión")',
        '.user-menu',
        '.logout',
        'nav .dropdown'
    ]
    
    logged_in = False
    for indicator in login_indicators:
        try:
            if page.locator(indicator).count() > 0:
                logged_in = True
                current_app.logger.info(f"Login confirmado por indicator: {indicator}")
                break
        except:
            continue
    
    if not logged_in:
        current_app.logger.warning("No se encontraron indicadores claros de login exitoso, pero continuando...")
    
    current_app.logger.info("Login completado exitosamente")


def _buscar_gops_en_pagina(page, gops_buscados, fuente):
    """
    Busca números GOP específicos en la página actual.
    Retorna diccionario con los GOP encontrados y sus datos.
    """
    encontrados = {}
    
    try:
        # Buscar tabla
        rows = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr")
        count = rows.count()
        
        current_app.logger.info(f"[{fuente}] Analizando {count} filas...")
        
        for i in range(min(count, 200)):  # Limitar a 200 filas
            try:
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()
                
                if cell_count >= 6:
                    # Obtener número de sistema (primera columna)
                    nro_sistema = cells.nth(0).inner_text().strip()
                    
                    # Verificar si este GOP está en nuestra lista de búsqueda
                    if nro_sistema in gops_buscados:
                        current_app.logger.info(f"[{fuente}] ¡Encontrado GOP {nro_sistema}!")
                        
                        # Extraer datos según la fuente (índices diferentes)
                        if fuente == "Mis Bandejas":
                            # Índices para "Mis Bandejas"
                            fecha_en_bandeja = cells.nth(6).inner_text().strip() if cell_count > 6 else ""  # Índice 6
                            usuario_asignado = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
                        else:
                            # Índices para "Todos los Trámites"
                            fecha_en_bandeja = cells.nth(7).inner_text().strip() if cell_count > 7 else ""  # Índice 7
                            usuario_asignado = cells.nth(8).inner_text().strip() if cell_count > 8 else ""
                        
                        # Extraer todos los datos
                        encontrados[nro_sistema] = {
                            "nro_sistema": nro_sistema,
                            "expediente": cells.nth(1).inner_text().strip() if cell_count > 1 else "",
                            "estado": cells.nth(2).inner_text().strip() if cell_count > 2 else "",
                            "profesional": cells.nth(3).inner_text().strip() if cell_count > 3 else "",
                            "nomenclatura": cells.nth(4).inner_text().strip() if cell_count > 4 else "",
                            "bandeja_actual": cells.nth(5).inner_text().strip() if cell_count > 5 else "",
                            "fecha_entrada": cells.nth(6).inner_text().strip() if cell_count > 6 else "",  # Original
                            "fecha_en_bandeja": fecha_en_bandeja,  # NUEVO CAMPO según fuente
                            "usuario_asignado": usuario_asignado,  # Ajustado según fuente
                            "fuente": fuente
                        }
                        
                        # Log específico según la fuente
                        if fuente == "Mis Bandejas":
                            current_app.logger.info(f"  Datos: Bandeja={encontrados[nro_sistema]['bandeja_actual']}, Usuario={encontrados[nro_sistema]['usuario_asignado']} (desde Mis Bandejas)")
                            current_app.logger.info(f"  Fecha en bandeja (índice 6): {fecha_en_bandeja}")
                        else:
                            current_app.logger.info(f"  Datos: Bandeja={encontrados[nro_sistema]['bandeja_actual']}, Usuario=Profesional (forzado desde Todos los Trámites)")
                            current_app.logger.info(f"  Fecha en bandeja (índice 7): {fecha_en_bandeja}")
                        
                        current_app.logger.info(f"  Estado: {encontrados[nro_sistema]['estado']}, Profesional: {encontrados[nro_sistema]['profesional']}")
                        
            except Exception as e:
                current_app.logger.warning(f"[{fuente}] Error procesando fila {i}: {e}")
                continue
                
    except Exception as e:
        current_app.logger.error(f"[{fuente}] Error general: {e}")
        page.screenshot(path=f"error_{fuente.lower().replace(' ', '_')}.png")
    
    return encontrados