import os
import sys
import time
import pandas as pd
from datetime import datetime, date
from flask import current_app
from pathlib import Path

# ==== NUEVO: util nulo para progreso, mantiene compatibilidad ====
def _noop_progress(curr, total, ok, fail, note=None):
    pass


def _ensure_gop_imports():
    """Configura los imports del módulo GOP."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    gop_dir = os.path.join(current_dir, 'gop_scraper')
    
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    if gop_dir not in sys.path:
        sys.path.insert(0, gop_dir)


def _determinar_bandeja_por_usuario(usuario_gop: str, fuente: str = "") -> str:
    """
    Determina a qué bandeja pertenece un usuario basándose en su nombre y fuente.
    
    Args:
        usuario_gop: Nombre del usuario asignado
        fuente: Fuente de donde viene el dato ("Mis Bandejas" o "Todos los Trámites")
    
    Returns:
        str: 'cpim', 'imlauer', 'onetto', 'profesional'
    
    Reglas:
    - Si viene de "Todos los Trámites" -> SIEMPRE va a 'profesional'
    - Si viene de "Mis Bandejas" -> Se clasifica según el usuario
    """
    
    # REGLA PRINCIPAL: Todos los Trámites -> Profesional
    if fuente == "Todos los Trámites":
        return 'profesional'
    
    # Solo clasificar por usuario si viene de "Mis Bandejas"
    if not usuario_gop:
        return 'profesional'
    
    usuario = str(usuario_gop).lower().strip()
    
    # Patrones para identificar cada bandeja (solo para "Mis Bandejas")
    if any(palabra in usuario for palabra in ['cpim', 'aguinagalde', 'gustavo', 'de jesús', 'santiago', 'javier']):
        return 'cpim'
    elif any(palabra in usuario for palabra in ['imlauer', 'fernando', 'sergio']):
        return 'imlauer'
    elif any(palabra in usuario for palabra in ['onetto']):
        return 'onetto'
    else:
        # Si no coincide con ninguno específico, va a profesional
        return 'profesional'


def _limpiar_campos_bandeja(expediente_id, db_session):
    """
    Limpia todos los campos de bandejas específicas para un expediente.
    """
    from app import _db  # Import necesario
    
    campos_a_limpiar = {
        'bandeja_cpim_nombre': None,
        'bandeja_cpim_usuario': None,
        'bandeja_cpim_fecha': None,
        'bandeja_cpim_sincronizacion': None,
        'bandeja_imlauer_nombre': None,
        'bandeja_imlauer_usuario': None,
        'bandeja_imlauer_fecha': None,
        'bandeja_imlauer_sincronizacion': None,
        'bandeja_onetto_nombre': None,
        'bandeja_onetto_usuario': None,
        'bandeja_onetto_fecha': None,
        'bandeja_onetto_sincronizacion': None,
        'bandeja_profesional_nombre': None,
        'bandeja_profesional_usuario': None,
        'bandeja_profesional_fecha': None,
        'bandeja_profesional_sincronizacion': None,
    }
    
    # Construir la query de actualización - CORREGIDO: usar _db.text() en lugar de db_session.text()
    set_clause = ', '.join([f"{campo} = :{campo}" for campo in campos_a_limpiar.keys()])
    
    db_session.execute(
        _db.text(f"""
            UPDATE expedientes 
            SET {set_clause}
            WHERE id = :expediente_id
        """),
        {**campos_a_limpiar, "expediente_id": expediente_id}
    )


def _parsear_fecha(fecha_str):
    """Parsea una fecha string a objeto date."""
    if not fecha_str or str(fecha_str).strip() in ['', 'nan', 'None']:
        return None
    
    try:
        fecha_str = str(fecha_str).strip()
        
        # Intentar diferentes formatos
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S'):
            try:
                if 'T' in fecha_str or ':' in fecha_str:
                    # Si tiene formato datetime, extraer solo la fecha
                    return datetime.strptime(fecha_str[:10], '%Y-%m-%d').date()
                else:
                    return datetime.strptime(fecha_str, fmt).date()
            except ValueError:
                continue
                
    except Exception as e:
        current_app.logger.warning(f"Error parseando fecha '{fecha_str}': {e}")
    
    return None


# ========= NUEVO: API pensada para el botón "Sincronizar GOP" =========
def sync_all_expedientes(update_progress):
    """
    Orquesta la sincronización GOP completa con callback de progreso.
    Pensada para ser llamada desde un hilo en segundo plano.
    
    - update_progress(curr, total, ok, fail, note?)
    """
    # Reutilizamos la lógica existente y agregamos reporte de progreso
    return sync_gop_data(update_progress=update_progress)


def sync_gop_data(update_progress=None):
    """
    Ejecuta el scraper GOP y actualiza los expedientes con información distribuida por bandejas.
    Incluye lógica de fuente: "Todos los Trámites" -> siempre Bandeja PROFESIONAL.
    EXCLUYE expedientes en formato PAPEL del proceso de sincronización.
    
    Si se provee update_progress(curr, total, ok, fail, note),
    se reporta progreso a cada GOP procesado.
    """
    _report = update_progress or _noop_progress

    try:
        from app import _db
        
        # === PASO 1: OBTENER TODOS LOS GOP DEL CPIM (SOLO DIGITALES) ===
        current_app.logger.info("=== DIAGNÓSTICO: OBTENIENDO NÚMEROS GOP DEL CPIM (SOLO DIGITALES) ===")
        
        gop_numbers = _db.session.execute(
            _db.text("""
                SELECT DISTINCT gop_numero 
                FROM expedientes 
                WHERE gop_numero IS NOT NULL 
                AND gop_numero != '' 
                AND (finalizado = false OR finalizado IS NULL)
                AND formato = 'Digital'
            """)
        ).fetchall()
        
        gop_list = [row[0].strip() for row in gop_numbers if row[0] and row[0].strip()]
        total_gops = len(gop_list)
        _report(0, total_gops, 0, 0, "Listando expedientes digitales con GOP...")
        current_app.logger.info(f"DIAGNÓSTICO: GOP encontrados en CPIM (solo digitales): {total_gops} -> {gop_list}")
        
        if not gop_list:
            _report(0, 0, 0, 0, "No hay expedientes digitales con números GOP")
            return {
                'total_gop_encontrados': 0,
                'expedientes_actualizados': 0,
                'expedientes_no_encontrados': 0,
                'bandejas_cpim': 0,
                'bandejas_imlauer': 0,
                'bandejas_onetto': 0,
                'bandejas_profesional': 0,
                'errores': ['No hay expedientes digitales con números GOP en el CPIM']
            }
        
        # === PASO 2: VERIFICAR CAMPOS EN BD ===
        current_app.logger.info("=== DIAGNÓSTICO: VERIFICANDO CAMPOS EN BD ===")
        try:
            # Verificar que los nuevos campos existen
            _db.session.execute(
                _db.text("""
                    SELECT bandeja_cpim_nombre, bandeja_imlauer_nombre, 
                           bandeja_onetto_nombre, bandeja_profesional_nombre
                    FROM expedientes LIMIT 1
                """)
            ).fetchone()
            current_app.logger.info("✓ Campos de bandejas específicas encontrados en BD")
        except Exception as e:
            current_app.logger.error(f"✗ ERROR: Campos de bandejas NO encontrados: {e}")
            _report(0, total_gops, 0, 0, "Faltan columnas de bandejas en BD")
            return {
                'error': f'Campos de bandejas no encontrados en BD: {e}',
                'total_gop_encontrados': 0,
                'expedientes_actualizados': 0,
                'expedientes_no_encontrados': 0,
                'bandejas_cpim': 0,
                'bandejas_imlauer': 0,
                'bandejas_onetto': 0,
                'bandejas_profesional': 0,
                'errores': [f'Campos faltantes: {e}']
            }
        
        # === PASO 3: EJECUTAR SCRAPER UNA SOLA VEZ PARA TODA LA LISTA ===
        current_app.logger.info("=== DIAGNÓSTICO: EJECUTANDO SCRAPER (MIS BANDEJAS + PENDIENTES EN TODOS LOS TRÁMITES) ===")
        _report(0, total_gops, 0, 0, "Scrapeando desde GOP (esto puede tardar)...")
        resultados_por_gop = _buscar_gops_especificos(gop_list)
        
        current_app.logger.info(f"DIAGNÓSTICO: Resultados del scraper: {len(resultados_por_gop)} registros")
        for key, datos in resultados_por_gop.items():
            current_app.logger.info(f"  {key}: {datos['nro_sistema']} - {datos['usuario_asignado']} - {datos['bandeja_actual']} - {datos['fuente']}")
        
        # === PASO 4: AGRUPAR POR GOP ===
        gop_agrupados = {}
        for gop_key, datos in resultados_por_gop.items():
            gop_numero = datos['nro_sistema']
            if gop_numero not in gop_agrupados:
                gop_agrupados[gop_numero] = []
            gop_agrupados[gop_numero].append(datos)
        
        current_app.logger.info(f"DIAGNÓSTICO: GOP únicos agrupados: {len(gop_agrupados)}")

        # === PASO 5: PROCESAR CADA GOP (SOLO DIGITALES) ===
        stats = {
            'total_gop_encontrados': len(gop_agrupados),
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': len(gop_list) - len(gop_agrupados),
            'bandejas_cpim': 0,
            'bandejas_imlauer': 0,
            'bandejas_onetto': 0,
            'bandejas_profesional': 0,
            'errores': []
        }
        
        ok = 0
        fail = 0
        curr = 0
        total = len(gop_agrupados)

        for gop_numero, lista_datos in gop_agrupados.items():
            curr += 1
            try:
                current_app.logger.info(f"DIAGNÓSTICO: Procesando GOP {gop_numero}")
                
                # Buscar expediente - ASEGURAR QUE SEA DIGITAL
                expediente_result = _db.session.execute(
                    _db.text("SELECT id, formato FROM expedientes WHERE gop_numero = :gop_numero AND formato = 'Digital'"),
                    {"gop_numero": gop_numero}
                ).fetchone()
                
                if not expediente_result:
                    error_msg = f"GOP {gop_numero} no encontrado en BD como expediente digital"
                    current_app.logger.warning(f"DIAGNÓSTICO: {error_msg}")
                    stats['errores'].append(error_msg)
                    fail += 1
                    _report(curr, total, ok, fail, f"No encontrado expediente digital para GOP {gop_numero}")
                    continue
                
                expediente_id = expediente_result[0]
                formato = expediente_result[1]
                
                # Verificación adicional de seguridad
                if formato != 'Digital':
                    error_msg = f"GOP {gop_numero} no es formato digital (formato: {formato}), omitiendo sincronización"
                    current_app.logger.warning(f"DIAGNÓSTICO: {error_msg}")
                    stats['errores'].append(error_msg)
                    fail += 1
                    _report(curr, total, ok, fail, f"Omitido (no digital) GOP {gop_numero}")
                    continue
                
                current_app.logger.info(f"DIAGNÓSTICO: Expediente digital ID {expediente_id} encontrado para GOP {gop_numero}")
                
                # Limpiar bandejas primero
                current_app.logger.info(f"DIAGNÓSTICO: Limpiando bandejas para expediente digital {expediente_id}")
                _limpiar_campos_bandeja(expediente_id, _db.session)
                
                # NUEVO: Recopilar datos para actualizar historial
                datos_bandejas_historial = {}
                
                # Procesar cada bandeja encontrada para este GOP
                for i, datos in enumerate(lista_datos):
                    current_app.logger.info(f"DIAGNÓSTICO: Procesando registro {i+1} de {len(lista_datos)}")
                    current_app.logger.info(f"  Usuario: '{datos.get('usuario_asignado', '')}'")
                    current_app.logger.info(f"  Fuente: '{datos.get('fuente', '')}'")
                    
                    # CAMBIO IMPORTANTE: Pasar la fuente para determinar la bandeja
                    bandeja_tipo = _determinar_bandeja_por_usuario(
                        datos.get('usuario_asignado', ''), 
                        datos.get('fuente', '')
                    )
                    current_app.logger.info(f"  Bandeja determinada: {bandeja_tipo} (Fuente: {datos.get('fuente', '')})")
                    
                    # Si viene de "Todos los Trámites", forzar usuario a "Profesional"
                    if datos.get('fuente') == "Todos los Trámites":
                        usuario_para_guardar = "Profesional"
                        current_app.logger.info(f"  Usuario forzado a 'Profesional' por venir de Todos los Trámites")
                    else:
                        usuario_para_guardar = str(datos.get('usuario_asignado', ''))[:200]
                    
                    # Parsear fechas
                    fecha_entrada = _parsear_fecha(datos.get('fecha_entrada', ''))
                    fecha_en_bandeja = _parsear_fecha(datos.get('fecha_en_bandeja', ''))
                    current_app.logger.info(f"  Fechas: entrada={fecha_entrada}, en_bandeja={fecha_en_bandeja}")
                    
                    # Preparar actualización
                    campos_update = {
                        f"bandeja_{bandeja_tipo}_nombre": str(datos.get('bandeja_actual', ''))[:200],
                        f"bandeja_{bandeja_tipo}_usuario": usuario_para_guardar,
                        f"bandeja_{bandeja_tipo}_fecha": fecha_en_bandeja or fecha_entrada,
                        f"bandeja_{bandeja_tipo}_sincronizacion": datetime.utcnow(),
                    }
                    
                    current_app.logger.info(f"  Campos a actualizar: {campos_update}")
                    
                    # Actualizar BD
                    set_clause = ', '.join([f"{campo} = :{campo}" for campo in campos_update.keys()])
                    query = f"""
                        UPDATE expedientes 
                        SET {set_clause}
                        WHERE id = :expediente_id
                    """
                    
                    current_app.logger.info(f"  Query: {query}")
                    
                    _db.session.execute(
                        _db.text(query),
                        {**campos_update, "expediente_id": expediente_id}
                    )
                    
                    stats[f'bandejas_{bandeja_tipo}'] += 1
                    current_app.logger.info(f"  ✓ Actualizado bandeja {bandeja_tipo} desde {datos.get('fuente', '')}")
                    
                    # NUEVO: Guardar datos para historial
                    datos_bandejas_historial[bandeja_tipo] = {
                        'nombre': str(datos.get('bandeja_actual', ''))[:200],
                        'usuario': usuario_para_guardar,
                        'fecha': fecha_en_bandeja or fecha_entrada or date.today()
                    }
                
                # Actualizar campos GOP originales con el primer resultado
                primer_dato = lista_datos[0]
                fecha_entrada_original = _parsear_fecha(primer_dato.get('fecha_entrada', ''))
                fecha_en_bandeja_original = _parsear_fecha(primer_dato.get('fecha_en_bandeja', ''))
                
                # Para campos GOP originales, usar el usuario real si viene de Mis Bandejas
                if primer_dato.get('fuente') == "Todos los Trámites":
                    usuario_gop_original = "Profesional"
                else:
                    usuario_gop_original = str(primer_dato.get('usuario_asignado', ''))[:200]
                
                _db.session.execute(
                    _db.text("""
                        UPDATE expedientes 
                        SET gop_bandeja_actual = :bandeja,
                            gop_usuario_asignado = :usuario,
                            gop_estado = :estado,
                            gop_fecha_entrada = :fecha_entrada,
                            gop_fecha_en_bandeja = :fecha_en_bandeja,
                            gop_ultima_sincronizacion = :sync_time
                        WHERE id = :expediente_id AND formato = 'Digital'
                    """),
                    {
                        "bandeja": str(primer_dato.get('bandeja_actual', ''))[:200],
                        "usuario": usuario_gop_original,
                        "estado": str(primer_dato.get('estado', ''))[:100],
                        "fecha_entrada": fecha_entrada_original,
                        "fecha_en_bandeja": fecha_en_bandeja_original,
                        "sync_time": datetime.utcnow(),
                        "expediente_id": expediente_id
                    }
                )
                
                # NUEVO: Actualizar historial de bandejas (seguro, no rompe si no existe)
                try:
                    current_app.logger.info(f"DIAGNÓSTICO: Actualizando historial para expediente digital {expediente_id}")
                    _actualizar_historial_tras_sincronizacion(expediente_id, datos_bandejas_historial)
                    current_app.logger.info(f"DIAGNÓSTICO: ✓ Historial actualizado para expediente digital {expediente_id}")
                except Exception as hist_error:
                    current_app.logger.warning(f"DIAGNÓSTICO: Error actualizando historial para {expediente_id}: {hist_error}")
                    # No fallar la sincronización por un error en el historial
                
                stats['expedientes_actualizados'] += 1
                ok += 1
                _report(curr, total, ok, fail, f"GOP {gop_numero} actualizado")
                current_app.logger.info(f"DIAGNÓSTICO: ✓ Expediente digital {expediente_id} actualizado completamente")
                
            except Exception as e:
                error_msg = f"Error actualizando GOP {gop_numero}: {e}"
                current_app.logger.error(f"DIAGNÓSTICO: {error_msg}")
                stats['errores'].append(error_msg)
                import traceback
                current_app.logger.error(f"DIAGNÓSTICO: Traceback: {traceback.format_exc()}")
                fail += 1
                _report(curr, total, ok, fail, f"Fallo al actualizar GOP {gop_numero}")
        
        # Commit final
        current_app.logger.info("DIAGNÓSTICO: Realizando commit...")
        _db.session.commit()
        current_app.logger.info("DIAGNÓSTICO: ✓ Commit exitoso")
        
        current_app.logger.info(f"DIAGNÓSTICO: Estadísticas finales: {stats}")
        _report(total, total, ok, fail, "Finalizado")
        return stats
        
    except Exception as e:
        current_app.logger.error(f"DIAGNÓSTICO: Error general en sync_gop_data: {e}")
        import traceback
        current_app.logger.error(f"DIAGNÓSTICO: Traceback completo: {traceback.format_exc()}")
        _report(0, 0, 0, 1, f"Error general: {e}")
        return {
            'error': str(e),
            'total_gop_encontrados': 0,
            'expedientes_actualizados': 0,
            'expedientes_no_encontrados': 0,
            'bandejas_cpim': 0,
            'bandejas_imlauer': 0,
            'bandejas_onetto': 0,
            'bandejas_profesional': 0,
            'errores': [str(e)]
        }


def _actualizar_historial_tras_sincronizacion(expediente_id, datos_nuevos):
    """
    Actualiza el historial de bandejas después de una sincronización GOP.
    Detecta cambios y registra movimientos entre bandejas.
    Versión segura que maneja el caso donde la tabla no existe.
    
    Args:
        expediente_id: ID del expediente
        datos_nuevos: Dict con las bandejas actualizadas {
            'cpim': {'nombre': '...', 'usuario': '...', 'fecha': date},
            'imlauer': {...}, etc.
        }
    """
    from app import _db
    
    try:
        # Verificar si la tabla existe
        result = _db.session.execute(
            _db.text("SELECT 1 FROM historial_bandejas LIMIT 1")
        )
        result.close()
        
        # Obtener expediente
        expediente_result = _db.session.execute(
            _db.text("SELECT id FROM expedientes WHERE id = :expediente_id"),
            {"expediente_id": expediente_id}
        ).fetchone()
        
        if not expediente_result:
            return
        
        # Procesar cada bandeja que tiene datos nuevos
        for bandeja_tipo, datos in datos_nuevos.items():
            if not datos or not datos.get('nombre'):
                continue
            
            nombre_bandeja = datos.get('nombre', '')
            usuario = datos.get('usuario', '')
            fecha_bandeja = datos.get('fecha') or date.today()
            
            # Verificar si ya existe un registro activo para esta bandeja
            registro_activo = _db.session.execute(
                _db.text("""
                    SELECT id, bandeja_nombre, usuario_asignado, fecha_inicio 
                    FROM historial_bandejas 
                    WHERE expediente_id = :expediente_id 
                    AND bandeja_tipo = :bandeja_tipo 
                    AND fecha_fin IS NULL
                """),
                {
                    "expediente_id": expediente_id,
                    "bandeja_tipo": bandeja_tipo
                }
            ).fetchone()
            
            if registro_activo:
                # Ya existe un registro activo para esta bandeja
                # Verificar si cambió el nombre de la bandeja o usuario
                if (registro_activo[1] != nombre_bandeja or 
                    registro_activo[2] != usuario):
                    
                    # Cerrar el registro anterior
                    dias_en_bandeja = (fecha_bandeja - registro_activo[3]).days
                    _db.session.execute(
                        _db.text("""
                            UPDATE historial_bandejas 
                            SET fecha_fin = :fecha_fin, 
                                dias_en_bandeja = :dias,
                                updated_at = :now
                            WHERE id = :registro_id
                        """),
                        {
                            "fecha_fin": fecha_bandeja,
                            "dias": max(0, dias_en_bandeja),
                            "now": datetime.utcnow(),
                            "registro_id": registro_activo[0]
                        }
                    )
                    
                    # Crear nuevo registro
                    _crear_nuevo_registro_historial(
                        expediente_id, bandeja_tipo, nombre_bandeja, 
                        usuario, fecha_bandeja
                    )
                    
                    current_app.logger.info(
                        f"Historial: Expediente {expediente_id} cambió en bandeja {bandeja_tipo}"
                    )
            else:
                # No existe registro activo, crear uno nuevo
                _crear_nuevo_registro_historial(
                    expediente_id, bandeja_tipo, nombre_bandeja, 
                    usuario, fecha_bandeja
                )
                
                current_app.logger.info(
                    f"Historial: Expediente {expediente_id} entró a bandeja {bandeja_tipo}"
                )
    
    except Exception as e:
        # Si la tabla no existe o hay otro error, hacer rollback y continuar
        _db.session.rollback()
        current_app.logger.warning(f"No se pudo actualizar historial para expediente {expediente_id}: {e}")
        # No propagar el error para que la sincronización continúe


def _crear_nuevo_registro_historial(expediente_id, bandeja_tipo, nombre_bandeja, usuario, fecha_inicio):
    """
    Crea un nuevo registro en el historial de bandejas.
    Versión segura.
    """
    from app import _db
    
    try:
        _db.session.execute(
            _db.text("""
                INSERT INTO historial_bandejas 
                (expediente_id, bandeja_tipo, bandeja_nombre, usuario_asignado, 
                 fecha_inicio, fecha_fin, dias_en_bandeja, created_at, updated_at)
                VALUES 
                (:expediente_id, :bandeja_tipo, :bandeja_nombre, :usuario_asignado,
                 :fecha_inicio, NULL, NULL, :now, :now)
            """),
            {
                "expediente_id": expediente_id,
                "bandeja_tipo": bandeja_tipo,
                "bandeja_nombre": nombre_bandeja[:200],  # Truncar si es muy largo
                "usuario_asignado": usuario[:200],
                "fecha_inicio": fecha_inicio,
                "now": datetime.utcnow()
            }
        )
    except Exception as e:
        _db.session.rollback()
        current_app.logger.warning(f"Error creando registro historial: {e}")

def _buscar_gops_en_pagina_multiple(page, gops_buscados, fuente="Mis Bandejas", max_pages=100):
    """
    Escanea una grilla paginada buscando múltiples GOP.
    - fuente: "Mis Bandejas" o "Todos los Trámites" (afecta los índices de columnas fecha/usuario).
    - gops_buscados: iterable con los GOP (strings) a buscar.
    Retorna: dict { clave_unica: datos_dict } con el mismo formato que _buscar_gops_en_pagina_simple.
    """
    pendientes = set(str(x).strip() for x in gops_buscados if str(x).strip())
    encontrados = {}
    pagina_actual = 1

    def _set_page_size_si_existe():
        # Intenta subir el page-size para reducir paginación
        selectores = [
            "select[name$='-per-page']",
            "select[name*='per-page']",
            "select.per-page",
            ".grid-view select.page-size",
        ]
        for sel in selectores:
            try:
                sel_loc = page.locator(sel).first
                if sel_loc and sel_loc.is_visible():
                    # Probar en orden 100, 200, 50 (lo que exista)
                    for val in ["100", "200", "50"]:
                        try:
                            sel_loc.select_option(val)
                            current_app.logger.info(f"[{fuente}] Page size cambiado a {val} con selector {sel}")
                            page.wait_for_load_state("networkidle")
                            page.wait_for_timeout(800)
                            return
                        except:
                            continue
            except:
                continue

    def _tabla_rows_locator():
        return page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr")

    def _leer_fila(cells):
        """
        Mapea las columnas según la fuente.
        Indices habituales (ajustar si tu grilla cambia):
          Col0: nro_sistema
          Col1: expediente
          Col2: estado
          Col3: profesional
          Col4: nomenclatura
          Col5: bandeja_actual
          Col6: fecha_entrada
          Col7: fecha_en_bandeja (en Mis Bandejas) / o distinto en Todos los Trámites
          Col8: usuario_asignado (en Todos los Trámites)
        """
        cell_count = cells.count()
        if cell_count < 6:
            return None

        nro_sistema   = cells.nth(0).inner_text().strip() if cell_count > 0 else ""
        expediente    = cells.nth(1).inner_text().strip() if cell_count > 1 else ""
        estado        = cells.nth(2).inner_text().strip() if cell_count > 2 else ""
        profesional   = cells.nth(3).inner_text().strip() if cell_count > 3 else ""
        nomenclatura  = cells.nth(4).inner_text().strip() if cell_count > 4 else ""
        bandeja_actual= cells.nth(5).inner_text().strip() if cell_count > 5 else ""
        fecha_entrada = cells.nth(6).inner_text().strip() if cell_count > 6 else ""

        if fuente == "Mis Bandejas":
            fecha_en_bandeja = cells.nth(6).inner_text().strip() if cell_count > 6 else ""  # a veces 6=entrada, 7=en_bandeja
            try:
                # Si hay 8 columnas, la 7 podría ser usuario
                usuario_asignado = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
            except:
                usuario_asignado = ""
        else:  # "Todos los Trámites"
            fecha_en_bandeja = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
            usuario_asignado = cells.nth(8).inner_text().strip() if cell_count > 8 else ""

        return {
            "nro_sistema": nro_sistema,
            "expediente": expediente,
            "estado": estado,
            "profesional": profesional,
            "nomenclatura": nomenclatura,
            "bandeja_actual": bandeja_actual,
            "fecha_entrada": fecha_entrada,
            "fecha_en_bandeja": fecha_en_bandeja,
            "usuario_asignado": usuario_asignado,
            "fuente": fuente
        }

    def _hay_siguiente_pagina_y_click():
        """
        Intenta ir a la siguiente página en una paginación típica de Yii2:
         - ul.pagination li.active + li a
         - li.next a, a[rel='next'], 'Siguiente'
        Devuelve True si clickeó y avanzó; False si no hay más páginas.
        """
        avanzó = False

        # Leer número de página activo (si existe) para comprobar cambio
        active_num = ""
        try:
            active = page.locator("ul.pagination li.active").first
            if active and active.is_visible():
                active_num = active.inner_text().strip()
        except:
            pass

        candidatos = [
            "ul.pagination li.active + li a",
            "ul.pagination li.next:not(.disabled) a",
            "a[rel='next']",
            "a[aria-label*='Siguiente' i]",
            "a:has-text('Siguiente')",
        ]
        for sel in candidatos:
            try:
                link = page.locator(sel).first
                if link and link.is_visible():
                    link.click()
                    # Esperar cambio de página/tabla
                    try:
                        page.wait_for_load_state("networkidle")
                    except:
                        pass
                    page.wait_for_timeout(800)
                    # Confirmar cambio (si podemos leer el activo de nuevo)
                    try:
                        active2 = page.locator("ul.pagination li.active").first
                        if active2 and active2.is_visible():
                            new_num = active2.inner_text().strip()
                            if new_num and new_num != active_num:
                                avanzó = True
                                break
                    except:
                        # Si no hay indicador, pero la tabla cambió, lo damos por bueno
                        avanzó = True
                        break
            except:
                continue

        return avanzó

    # --- Ajustar page size si existe ---
    _set_page_size_si_existe()

    # --- Loop de páginas ---
    while pendientes and pagina_actual <= max_pages:
        try:
            rows = _tabla_rows_locator()
            count = rows.count()
            current_app.logger.info(f"[{fuente}] Página {pagina_actual}: {count} filas")

            # Si hay muy pocas filas, log de ayuda
            if count <= 10:
                try:
                    for i in range(count):
                        txt = rows.nth(i).inner_text().strip()
                        current_app.logger.debug(f"[{fuente}] p{pagina_actual} fila {i}: {txt[:200]}")
                except:
                    pass

            for i in range(count):
                try:
                    row = rows.nth(i)
                    cells = row.locator("td")
                    datos = _leer_fila(cells)
                    if not datos:
                        continue

                    nro = datos["nro_sistema"]
                    if nro in pendientes:
                        clave = f"{nro}_{fuente}_p{pagina_actual}_r{i}"
                        encontrados[clave] = datos
                        pendientes.remove(nro)
                        current_app.logger.info(f"[{fuente}] ✓ Encontrado GOP {nro} en página {pagina_actual}, fila {i}")
                        if not pendientes:
                            break
                except Exception as e:
                    current_app.logger.debug(f"[{fuente}] Error leyendo fila {i} p{pagina_actual}: {e}")
                    continue

            if not pendientes:
                break

            # Intentar pasar a siguiente página
            if not _hay_siguiente_pagina_y_click():
                current_app.logger.info(f"[{fuente}] No hay más páginas. Pendientes: {sorted(list(pendientes))}")
                break

            pagina_actual += 1

        except Exception as e:
            current_app.logger.error(f"[{fuente}] Error procesando página {pagina_actual}: {e}")
            try:
                page.screenshot(path=f"error_{fuente.replace(' ','_').lower()}_p{pagina_actual}.png")
            except:
                pass
            break

    # Log final
    if pendientes:
        current_app.logger.warning(f"[{fuente}] GOP NO encontrados tras paginar: {sorted(list(pendientes))}")

    return encontrados



def _buscar_gops_en_pagina_simple(page, gops_buscados, fuente, gop_especifico):
    """
    Versión simplificada para buscar GOP después de aplicar filtro.
    Busca en menos filas ya que el filtro debería reducir los resultados.
    """
    encontrados = {}
    
    try:
        rows = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr")
        count = rows.count()
        
        current_app.logger.info(f"[{fuente}] Búsqueda filtrada para GOP {gop_especifico}: {count} filas encontradas")
        
        # Si hay pocas filas, mostrar el contenido para debug
        if count <= 10:
            current_app.logger.info(f"[{fuente}] DEBUG: Mostrando todas las {count} filas:")
            for i in range(count):
                try:
                    row = rows.nth(i)
                    row_text = row.inner_text().strip()
                    current_app.logger.info(f"  Fila {i}: {row_text[:150]}")
                except:
                    pass
        
        for i in range(min(count, 50)):  # Buscar en máximo 50 filas (debería ser suficiente)
            try:
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()
                
                if cell_count >= 6:
                    nro_sistema = cells.nth(0).inner_text().strip()
                    
                    current_app.logger.debug(f"[{fuente}] Fila {i}: GOP='{nro_sistema}'")
                    
                    if nro_sistema in gops_buscados:
                        clave_unica = f"{nro_sistema}_{fuente}_filtrado_{i}"
                        
                        current_app.logger.info(f"[{fuente}] ¡ENCONTRADO GOP {nro_sistema} con filtro!")
                        
                        # Extraer datos según la fuente
                        if fuente == "Mis Bandejas":
                            fecha_en_bandeja = cells.nth(6).inner_text().strip() if cell_count > 6 else ""
                            usuario_asignado = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
                        else:  # Todos los Trámites
                            fecha_en_bandeja = cells.nth(7).inner_text().strip() if cell_count > 7 else ""
                            usuario_asignado = cells.nth(8).inner_text().strip() if cell_count > 8 else ""
                        
                        encontrados[clave_unica] = {
                            "nro_sistema": nro_sistema,
                            "expediente": cells.nth(1).inner_text().strip() if cell_count > 1 else "",
                            "estado": cells.nth(2).inner_text().strip() if cell_count > 2 else "",
                            "profesional": cells.nth(3).inner_text().strip() if cell_count > 3 else "",
                            "nomenclatura": cells.nth(4).inner_text().strip() if cell_count > 4 else "",
                            "bandeja_actual": cells.nth(5).inner_text().strip() if cell_count > 5 else "",
                            "fecha_entrada": cells.nth(6).inner_text().strip() if cell_count > 6 else "",
                            "fecha_en_bandeja": fecha_en_bandeja,
                            "usuario_asignado": usuario_asignado,
                            "fuente": fuente
                        }
                        
                        current_app.logger.info(f"[{fuente}] Datos extraídos:")
                        current_app.logger.info(f"  Bandeja: {encontrados[clave_unica]['bandeja_actual']}")
                        current_app.logger.info(f"  Usuario: {encontrados[clave_unica]['usuario_asignado']}")
                        
                        break  # Si encontramos el GOP, no necesitamos seguir buscando
                        
            except Exception as e:
                current_app.logger.warning(f"[{fuente}] Error procesando fila {i}: {e}")
                continue
                
    except Exception as e:
        current_app.logger.error(f"[{fuente}] Error en búsqueda filtrada: {e}")
        page.screenshot(path=f"error_busqueda_filtrada_{gop_especifico}.png")
    
    return encontrados
    

def _buscar_gops_especificos(gop_list):
    """
    Busca números GOP específicos usando estrategia optimizada:
    1. Busca TODOS los GOP en "Mis Bandejas"
    2. Solo busca en "Todos los Trámites" los GOP que NO se encontraron en "Mis Bandejas"
    """
    from dotenv import load_dotenv
    from playwright.sync_api import sync_playwright
    import subprocess
    import sys
    
    # Verificar e instalar navegadores si es necesario
    try:
        current_app.logger.info("=== INICIANDO VERIFICACIÓN DE PLAYWRIGHT ===")
        
        with sync_playwright() as p:
            try:
                # Intento rápido para verificar si funcionan los navegadores
                browser = p.chromium.launch(headless=True)
                browser.close()
                current_app.logger.info("✓ Navegadores disponibles y funcionando")
            except Exception as browser_error:
                current_app.logger.warning(f"Navegadores no disponibles: {browser_error}")
                current_app.logger.info("Instalando navegador Chromium...")
                
                # Instalar solo chromium para ahorrar tiempo
                result = subprocess.run([
                    sys.executable, "-m", "playwright", "install", "chromium"
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode != 0:
                    current_app.logger.error(f"Error instalando navegador: {result.stderr}")
                    # Intentar sin --with-deps si falla
                    result = subprocess.run([
                        sys.executable, "-m", "playwright", "install", "chromium"
                    ], capture_output=True, text=True, timeout=300)
                    
                    if result.returncode != 0:
                        raise RuntimeError(f"No se pudo instalar Chromium: {result.stderr}")
                
                current_app.logger.info("✓ Chromium instalado exitosamente")
    
    except Exception as install_error:
        current_app.logger.error(f"ERROR CRÍTICO con Playwright: {install_error}")
        raise RuntimeError(f"No se pudieron configurar los navegadores: {install_error}")
    
    # Cargar variables de entorno
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")
    
    current_app.logger.info(f"=== CREDENCIALES ===")
    current_app.logger.info(f"Usuario configurado: {user[:3]}*** (longitud: {len(user)})")
    current_app.logger.info(f"Contraseña configurada: {'*' * min(len(pw), 8)} (longitud: {len(pw)})")
    
    if not user or not pw:
        raise RuntimeError("Credenciales USER_MUNI/PASS_MUNI no encontradas en variables de entorno")
    
    if len(user.strip()) < 3:
        raise RuntimeError(f"Usuario muy corto: '{user}'. Verifica USER_MUNI")
    
    if len(pw.strip()) < 3:
        raise RuntimeError(f"Contraseña muy corta. Verifica PASS_MUNI")
    
    # Configuración
    BASE = "https://posadas.gestiondeobrasprivadas.com.ar"
    LOGIN_URL = f"{BASE}/frontend/web/site/login"
    MY_TRAYS_URL = f"{BASE}/frontend/web/site/my-trays"
    ALL_FORMALITIES_URL = f"{BASE}/frontend/web/formality/index-all"
    
    # Detectar si estamos en Railway o local
    IS_RAILWAY = os.getenv("RAILWAY_ENVIRONMENT") is not None
    HEADLESS = IS_RAILWAY or os.getenv("HEADLESS", "true").lower() == "true"
    
    current_app.logger.info(f"Entorno: {'Railway' if IS_RAILWAY else 'Local'}")
    current_app.logger.info(f"Modo headless: {HEADLESS}")
    
    resultados = {}
    gops_pendientes = set(gop_list)
    
    current_app.logger.info(f"=== INICIANDO NAVEGADOR ===")
    
    with sync_playwright() as p:
        # Configuración optimizada para Railway/Docker
        browser_args = [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
        
        # Solo agregar --single-process y --no-zygote en Railway
        if IS_RAILWAY:
            browser_args.extend(['--single-process', '--no-zygote'])
        
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=browser_args
        )
        
        # Context SIN el parámetro timeout (que no existe)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            ignore_https_errors=True
        )
        
        page = context.new_page()
        
        # Configurar timeouts por defecto en la página (no en el context)
        page.set_default_timeout(45000)  # 45 segundos
        page.set_default_navigation_timeout(60000)  # 60 segundos para navegación
        
        try:
            # === LOGIN MEJORADO ===
            current_app.logger.info("=== NAVEGANDO A PÁGINA DE LOGIN ===")
            
            try:
                page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
                current_app.logger.info(f"URL actual después de navegación: {page.url}")
            except Exception as nav_error:
                current_app.logger.error(f"Error navegando al login: {nav_error}")
                # Intentar de nuevo con networkidle
                page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
            
            # Esperar un poco más para asegurar que la página cargó
            page.wait_for_timeout(3000)
            
            # Verificar que estamos en la página de login
            if "login" not in page.url.lower():
                current_app.logger.error(f"No llegamos a la página de login. URL: {page.url}")
                # Tomar screenshot si es posible
                try:
                    page.screenshot(path="no_login_page.png")
                except:
                    pass
                raise RuntimeError(f"No se pudo acceder al login. URL actual: {page.url}")
            
            current_app.logger.info("=== REALIZANDO LOGIN ===")
            _perform_login(page, user, pw)
            
            # Verificar login exitoso con más tiempo
            page.wait_for_timeout(5000)
            current_app.logger.info(f"URL después del login: {page.url}")
            
            # === PASO 1: BUSCAR EN MIS BANDEJAS ===
            current_app.logger.info("=== NAVEGANDO A MIS BANDEJAS ===")
            current_app.logger.info(f"GOP a buscar: {list(gops_pendientes)}")
            
            try:
                # Navegar con múltiples intentos
                for intento in range(3):
                    try:
                        page.goto(MY_TRAYS_URL, wait_until="networkidle", timeout=60000)
                        current_app.logger.info(f"Cargada página Mis Bandejas: {page.url}")
                        break
                    except Exception as e:
                        if intento == 2:
                            raise e
                        current_app.logger.warning(f"Intento {intento + 1} falló, reintentando...")
                        page.wait_for_timeout(2000)
                
                # Esperar que la tabla cargue
                page.wait_for_timeout(5000)
                
                # Verificar si hay tabla
                table_count = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr").count()
                current_app.logger.info(f"Filas encontradas en Mis Bandejas: {table_count}")
                
                if table_count == 0:
                    current_app.logger.warning("No se encontraron filas inicialmente, esperando más...")
                    page.wait_for_timeout(10000)
                    table_count = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr").count()
                    current_app.logger.info(f"Filas después de esperar: {table_count}")
                
                encontrados_bandejas = _buscar_gops_en_pagina_multiple(page, list(gops_pendientes), "Mis Bandejas")
                
                # Procesar resultados
                gops_encontrados_bandejas = set()
                for gop_key, datos in encontrados_bandejas.items():
                    resultados[gop_key] = datos
                    gop_numero = datos['nro_sistema']
                    gops_encontrados_bandejas.add(gop_numero)
                
                gops_pendientes -= gops_encontrados_bandejas
                
                current_app.logger.info(f"✓ Encontrados en Mis Bandejas: {len(encontrados_bandejas)}")
                current_app.logger.info(f"✓ GOP encontrados: {list(gops_encontrados_bandejas)}")
                current_app.logger.info(f"⏳ Pendientes para Todos los Trámites: {list(gops_pendientes)}")
                
            except Exception as e:
                current_app.logger.error(f"Error en Mis Bandejas: {e}")
                import traceback
                current_app.logger.error(f"Traceback: {traceback.format_exc()}")
                try:
                    page.screenshot(path="mis_bandejas_error.png")
                except:
                    pass
            
            # === PASO 2: BUSCAR EN TODOS LOS TRÁMITES (SOLO PENDIENTES) ===
            if gops_pendientes:
                current_app.logger.info(f"=== BUSCANDO EN TODOS LOS TRÁMITES ===")
                current_app.logger.info(f"Buscando GOP pendientes: {list(gops_pendientes)}")
                
                try:
                    encontrados_todos_totales = {}
                    
                    for idx, gop_numero in enumerate(gops_pendientes):
                        current_app.logger.info(f"[{idx+1}/{len(gops_pendientes)}] Buscando GOP {gop_numero}...")
                        
                        # Navegar a la página con reintentos
                        for intento in range(3):
                            try:
                                page.goto(ALL_FORMALITIES_URL, wait_until="networkidle", timeout=60000)
                                break
                            except Exception as e:
                                if intento == 2:
                                    current_app.logger.error(f"No se pudo cargar Todos los Trámites después de 3 intentos")
                                    continue
                                page.wait_for_timeout(2000)
                        
                        page.wait_for_timeout(5000)
                        
                        # Buscar y aplicar filtro
                        filtro_aplicado = False
                        filtro_selectores = [
                            'input[name*="nro" i]',
                            'input[placeholder*="nro" i]',
                            'input[placeholder*="GOP" i]',
                            'input#formality-nro_sistema',
                            'input[name="Formality[nro_sistema]"]',
                            'input[name="FormalitySearch[nro_sistema]"]'
                        ]
                        
                        for selector in filtro_selectores:
                            try:
                                elementos = page.locator(selector)
                                if elementos.count() > 0:
                                    current_app.logger.info(f"Aplicando filtro con selector: {selector}")
                                    elementos.first.fill("")  # Limpiar primero
                                    page.wait_for_timeout(500)
                                    elementos.first.fill(gop_numero)
                                    page.wait_for_timeout(500)
                                    
                                    # Buscar y hacer click en botón de búsqueda
                                    search_buttons = [
                                        'button[type="submit"]',
                                        'button:has-text("Buscar")',
                                        'button:has-text("Filtrar")',
                                        'button:has-text("Search")',
                                        '.btn-primary[type="submit"]',
                                        'input[type="submit"]'
                                    ]
                                    
                                    button_clicked = False
                                    for btn_selector in search_buttons:
                                        try:
                                            btn = page.locator(btn_selector).first
                                            if btn and btn.is_visible():
                                                btn.click()
                                                button_clicked = True
                                                current_app.logger.info(f"Click en botón: {btn_selector}")
                                                break
                                        except:
                                            continue
                                    
                                    if not button_clicked:
                                        # Intentar presionar Enter
                                        elementos.first.press("Enter")
                                        current_app.logger.info("Enviado con Enter")
                                    
                                    # Esperar resultados
                                    page.wait_for_timeout(3000)
                                    filtro_aplicado = True
                                    break
                            except Exception as e:
                                current_app.logger.debug(f"Selector {selector} falló: {e}")
                                continue
                        
                        if not filtro_aplicado:
                            current_app.logger.warning(f"No se pudo aplicar filtro para GOP {gop_numero}")
                            continue
                        
                        # Buscar en tabla filtrada
                        current_app.logger.info(f"Buscando en resultados filtrados...")
                        encontrados_gop = _buscar_gops_en_pagina_simple(page, [gop_numero], "Todos los Trámites", gop_numero)
                        
                        if encontrados_gop:
                            current_app.logger.info(f"✓ GOP {gop_numero} encontrado")
                            encontrados_todos_totales.update(encontrados_gop)
                        else:
                            current_app.logger.warning(f"✗ GOP {gop_numero} NO encontrado")
                    
                    # Agregar resultados
                    for gop_key, datos in encontrados_todos_totales.items():
                        resultados[gop_key] = datos
                    
                    current_app.logger.info(f"✓ Total en Todos los Trámites: {len(encontrados_todos_totales)}")
                    
                except Exception as e:
                    current_app.logger.error(f"Error en Todos los Trámites: {e}")
                    import traceback
                    current_app.logger.error(f"Traceback: {traceback.format_exc()}")
                    try:
                        page.screenshot(path="todos_tramites_error.png")
                    except:
                        pass
            else:
                current_app.logger.info("✓ Todos los GOP encontrados en Mis Bandejas")
        
        finally:
            browser.close()
            current_app.logger.info("=== NAVEGADOR CERRADO ===")
    
    current_app.logger.info(f"=== SCRAPING COMPLETADO ===")
    current_app.logger.info(f"Total registros encontrados: {len(resultados)}")
    
    # Log detallado de resultados
    gops_unicos = set()
    for gop_key, datos in resultados.items():
        gops_unicos.add(datos['nro_sistema'])
        current_app.logger.info(f"  {datos['nro_sistema']} - {datos['fuente']} - {datos['usuario_asignado']}")
    
    gops_no_encontrados = set(gop_list) - gops_unicos
    if gops_no_encontrados:
        current_app.logger.warning(f"GOP NO encontrados: {list(gops_no_encontrados)}")
    
    return resultados


def _buscar_gops_especificos(gop_list):
    """
    Busca números GOP específicos usando estrategia optimizada:
    1) Busca TODOS los GOP en "Mis Bandejas"
    2) Solo busca en "Todos los Trámites" los GOP que NO se encontraron en "Mis Bandejas"
    Incluye verificación de login, re-login si redirige, y selectores amplios/robustos para el filtro.
    """
    import os
    import sys
    import subprocess
    import traceback
    from dotenv import load_dotenv
    from playwright.sync_api import sync_playwright

    # ---------------------------
    # Helpers internos (no tocar)
    # ---------------------------

    def _ensure_logged_in(page):
        """Falla si seguimos en /login o no aparece un indicador de sesión."""
        page.wait_for_load_state("networkidle")
        if "login" in page.url.lower():
            raise RuntimeError(f"Login falló; URL actual: {page.url}")
        # Esperar algo que solo exista con sesión abierta (ajustar a tu UI si hace falta)
        page.wait_for_selector(
            "a[href*='logout'], a:has-text('Salir'), a[href*='my-trays'], a:has-text('Mis Bandejas')",
            timeout=15000
        )

    def _goto_protegido(page, url, user, pw):
        """Navega a URL protegida; si te patea a /login, reloguea y reintenta una vez."""
        page.goto(url, wait_until="networkidle", timeout=60000)
        if "login" in page.url.lower():
            current_app.logger.warning(f"Redirigido a login al ir a {url}. Reintentando con re-login...")
            _perform_login(page, user, pw)
            page.wait_for_load_state("networkidle")
            page.goto(url, wait_until="networkidle", timeout=60000)
        _ensure_logged_in(page)

    def _expandir_y_esperar_filtros(page):
        """Despliega filtros si hay toggle y espera que existan inputs de la fila de filtros."""
        try:
            toggle = page.locator("button:has-text('Filtros'), .filters-toggle, .btn-filters").first
            if toggle and toggle.is_visible():
                toggle.click()
        except:
            pass
        # Esperar fila de filtros o al menos la tabla
        try:
            page.wait_for_selector("tr.filters input, .grid-view tr.filters input, form[action*='index-all'] input",
                                   timeout=10000)
        except:
            page.wait_for_selector("table, .grid-view", timeout=10000)

    SELECTORES_FILTRO = [
        # específicos
        'input#formality-nro_sistema',
        'input[name="Formality[nro_sistema]"]',
        'input[name="FormalitySearch[nro_sistema]"]',
        # por nombre/placeholder parciales (case-insensitive)
        'input[name*="nro" i]',
        'input[name*="numero" i]',
        'input[name*="sistema" i]',
        'input[placeholder*="nro" i]',
        'input[placeholder*="número" i]',
        'input[placeholder*="sistema" i]',
        'input[placeholder*="GOP" i]',
        # comodines que ayudan si cambiaron atributos
        '[data-attribute="nro_sistema"] input',
        '.search-input',
        'input[type="text"]',
    ]

    SEARCH_BUTTONS = [
        'button[type="submit"]',
        'button:has-text("Buscar")',
        'button:has-text("Filtrar")',
        'button:has-text("Search")',
        '.btn-search',
        '.search-btn',
        '.btn-primary[type="submit"]',
        'input[type="submit"]',
    ]

    def _aplicar_filtro_por_gop(page, gop_numero):
        """Intenta completar el input de Nro Sistema y ejecutar la búsqueda."""
        # Si la tabla estuviera en un iframe (defensivo)
        target = page
        try:
            if page.locator("iframe").count() > 0:
                target = page.frame_locator("iframe").first
        except:
            pass

        for selector in SELECTORES_FILTRO:
            try:
                elems = target.locator(selector)
                cnt = elems.count()
                if cnt == 0:
                    continue

                # Probar algunos (por si hay más de uno, p.ej. header/footer)
                to_try = min(cnt, 5)
                for i in range(to_try):
                    el = elems.nth(i)
                    if not (el.is_visible() and el.is_enabled()):
                        continue

                    try:
                        el.focus()
                    except:
                        pass
                    try:
                        el.clear()
                    except:
                        pass
                    el.fill(gop_numero)

                    # Intentar con botón; si no, Enter
                    enviado = False
                    for btn_sel in SEARCH_BUTTONS:
                        try:
                            btn = target.locator(btn_sel).first
                            if btn and btn.is_visible():
                                btn.click()
                                enviado = True
                                break
                        except:
                            continue
                    if not enviado:
                        try:
                            el.press("Enter")
                        except:
                            pass

                    # Esperar resultados/pjax
                    try:
                        target.locator("table, .grid-view").wait_for(state="visible", timeout=10000)
                    except:
                        pass
                    page.wait_for_load_state("networkidle")
                    page.wait_for_timeout(800)
                    return True
            except Exception as e:
                current_app.logger.debug(f"Selector {selector} falló: {e}")
                continue
        return False

    # ---------------------------
    # Instalación/verificación Playwright
    # ---------------------------
    try:
        current_app.logger.info("=== INICIANDO VERIFICACIÓN DE PLAYWRIGHT ===")
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                browser.close()
                current_app.logger.info("✓ Navegadores disponibles y funcionando")
            except Exception as browser_error:
                current_app.logger.warning(f"Navegadores no disponibles: {browser_error}")
                current_app.logger.info("Instalando navegador Chromium...")
                result = subprocess.run(
                    [sys.executable, "-m", "playwright", "install", "chromium"],
                    capture_output=True, text=True, timeout=300
                )
                if result.returncode != 0:
                    raise RuntimeError(f"No se pudo instalar Chromium: {result.stderr}")
                current_app.logger.info("✓ Chromium instalado exitosamente")
    except Exception as install_error:
        current_app.logger.error(f"ERROR CRÍTICO con Playwright: {install_error}")
        raise RuntimeError(f"No se pudieron configurar los navegadores: {install_error}")

    # ---------------------------
    # Credenciales / Config
    # ---------------------------
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")

    current_app.logger.info("=== CREDENCIALES ===")
    current_app.logger.info(f"Usuario configurado: {user[:3]}*** (longitud: {len(user)})")
    current_app.logger.info(f"Contraseña configurada: {'*' * min(len(pw), 8)} (longitud: {len(pw)})")

    if not user or not pw:
        raise RuntimeError("Credenciales USER_MUNI/PASS_MUNI no encontradas en variables de entorno")
    if len(user.strip()) < 3:
        raise RuntimeError(f"Usuario muy corto: '{user}'. Verifica USER_MUNI")
    if len(pw.strip()) < 3:
        raise RuntimeError("Contraseña muy corta. Verifica PASS_MUNI")

    BASE = "https://posadas.gestiondeobrasprivadas.com.ar"
    LOGIN_URL = f"{BASE}/frontend/web/site/login"
    MY_TRAYS_URL = f"{BASE}/frontend/web/site/my-trays"
    ALL_FORMALITIES_URL = f"{BASE}/frontend/web/formality/index-all"

    IS_RAILWAY = os.getenv("RAILWAY_ENVIRONMENT") is not None
    HEADLESS = IS_RAILWAY or os.getenv("HEADLESS", "true").lower() == "true"

    current_app.logger.info(f"Entorno: {'Railway' if IS_RAILWAY else 'Local'}")
    current_app.logger.info(f"Modo headless: {HEADLESS}")

    resultados = {}
    gops_pendientes = set(gop_list)

    # ---------------------------
    # Navegación principal
    # ---------------------------
    current_app.logger.info("=== INICIANDO NAVEGADOR ===")
    with sync_playwright() as p:
        # Flags mínimos y seguros (evitar --disable-web-security/IsolateOrigins)
        browser_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        if IS_RAILWAY:
            browser_args += ['--single-process', '--no-zygote']

        browser = p.chromium.launch(headless=HEADLESS, args=browser_args)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            ignore_https_errors=True
        )
        page = context.new_page()
        page.set_default_timeout(45000)
        page.set_default_navigation_timeout(60000)

        try:
            # -------- LOGIN --------
            current_app.logger.info("=== NAVEGANDO A LOGIN ===")
            try:
                page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
                current_app.logger.info(f"URL tras goto(login): {page.url}")
            except Exception as nav_error:
                current_app.logger.error(f"Error navegando al login: {nav_error}")
                page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)

            page.wait_for_timeout(1500)
            if "login" not in page.url.lower():
                try:
                    page.screenshot(path="no_login_page.png")
                except:
                    pass
                raise RuntimeError(f"No se pudo acceder al login. URL actual: {page.url}")

            current_app.logger.info("=== REALIZANDO LOGIN ===")
            _perform_login(page, user, pw)
            _ensure_logged_in(page)
            current_app.logger.info(f"URL después del login: {page.url}")

            # -------- PASO 1: MIS BANDEJAS --------
            current_app.logger.info("=== MIS BANDEJAS ===")
            current_app.logger.info(f"GOP a buscar: {list(gops_pendientes)}")

            try:
                _goto_protegido(page, MY_TRAYS_URL, user, pw)
                page.wait_for_timeout(2000)

                # Espera defensiva por la tabla
                try:
                    page.wait_for_selector("table tbody tr, .table tbody tr, .grid-view tbody tr", timeout=10000)
                except:
                    current_app.logger.warning("No se encontraron filas rápido; esperando extra...")
                    page.wait_for_timeout(8000)

                encontrados_bandejas = _buscar_gops_en_pagina_multiple(page, list(gops_pendientes), "Mis Bandejas")

                gops_encontrados_bandejas = set()
                for gop_key, datos in encontrados_bandejas.items():
                    resultados[gop_key] = datos
                    gops_encontrados_bandejas.add(datos['nro_sistema'])

                gops_pendientes -= gops_encontrados_bandejas

                current_app.logger.info(f"✓ Encontrados en Mis Bandejas: {len(encontrados_bandejas)}")
                current_app.logger.info(f"✓ GOP encontrados: {list(gops_encontrados_bandejas)}")
                current_app.logger.info(f"⏳ Pendientes para Todos los Trámites: {list(gops_pendientes)}")

            except Exception as e:
                current_app.logger.error(f"Error en Mis Bandejas: {e}")
                current_app.logger.error(f"Traceback: {traceback.format_exc()}")
                try:
                    page.screenshot(path="mis_bandejas_error.png")
                except:
                    pass

            # -------- PASO 2: TODOS LOS TRÁMITES (solo pendientes) --------
            if gops_pendientes:
                current_app.logger.info("=== TODOS LOS TRÁMITES (pendientes) ===")
                current_app.logger.info(f"Buscando GOP pendientes: {list(gops_pendientes)}")

                try:
                    encontrados_todos_totales = {}

                    for idx, gop_numero in enumerate(gops_pendientes):
                        current_app.logger.info(f"[{idx+1}/{len(gops_pendientes)}] GOP {gop_numero}")

                        # Navegación protegida
                        try:
                            _goto_protegido(page, ALL_FORMALITIES_URL, user, pw)
                        except Exception as nav_e:
                            current_app.logger.error(f"No se pudo cargar Todos los Trámites: {nav_e}")
                            continue

                        page.wait_for_timeout(1500)
                        _expandir_y_esperar_filtros(page)

                        ok = _aplicar_filtro_por_gop(page, gop_numero)
                        if not ok:
                            current_app.logger.warning(f"No se pudo aplicar filtro para GOP {gop_numero}")
                            try:
                                page.screenshot(path=f"debug_sin_filtro_{gop_numero}.png")
                                with open(f"debug_{gop_numero}.html", "w", encoding="utf-8") as f:
                                    f.write(page.content())
                            except:
                                pass

                            # Si estamos en login, reloguear y un reintento
                            if "login" in page.url.lower():
                                _perform_login(page, user, pw)
                                _goto_protegido(page, ALL_FORMALITIES_URL, user, pw)
                                page.wait_for_timeout(1000)
                                _expandir_y_esperar_filtros(page)
                                ok = _aplicar_filtro_por_gop(page, gop_numero)
                                if not ok:
                                    current_app.logger.warning(f"Reintento fallido de filtro para GOP {gop_numero}")
                                    continue
                            else:
                                continue

                        # Buscar en la grilla ya filtrada
                        current_app.logger.info("Buscando en resultados filtrados...")
                        encontrados_gop = _buscar_gops_en_pagina_simple(page, [gop_numero], "Todos los Trámites", gop_numero)
                        if encontrados_gop:
                            current_app.logger.info(f"✓ GOP {gop_numero} encontrado en Todos los Trámites")
                            encontrados_todos_totales.update(encontrados_gop)
                        else:
                            current_app.logger.warning(f"✗ GOP {gop_numero} NO encontrado en Todos los Trámites")

                    # Agregar a resultados
                    for gop_key, datos in encontrados_todos_totales.items():
                        resultados[gop_key] = datos

                    current_app.logger.info(f"✓ Total en Todos los Trámites: {len(encontrados_todos_totales)}")

                except Exception as e:
                    current_app.logger.error(f"Error en Todos los Trámites: {e}")
                    current_app.logger.error(f"Traceback: {traceback.format_exc()}")
                    try:
                        page.screenshot(path="todos_tramites_error.png")
                    except:
                        pass
            else:
                current_app.logger.info("✓ Todos los GOP fueron hallados en Mis Bandejas")

        finally:
            browser.close()
            current_app.logger.info("=== NAVEGADOR CERRADO ===")

    # ---------------------------
    # Logging final y retorno
    # ---------------------------
    current_app.logger.info("=== SCRAPING COMPLETADO ===")
    current_app.logger.info(f"Total registros encontrados: {len(resultados)}")

    gops_unicos = set()
    gops_desde_bandejas = set()
    gops_desde_todos = set()

    for gop_key, datos in resultados.items():
        gops_unicos.add(datos['nro_sistema'])
        if datos.get('fuente') == 'Mis Bandejas':
            gops_desde_bandejas.add(datos['nro_sistema'])
        else:
            gops_desde_todos.add(datos['nro_sistema'])
        current_app.logger.info(f"  {datos['nro_sistema']} - {datos.get('fuente')} - {datos.get('usuario_asignado')}")

    gops_no_encontrados = set(gop_list) - gops_unicos
    if gops_no_encontrados:
        current_app.logger.warning(f"GOP NO encontrados: {sorted(list(gops_no_encontrados))}")

    current_app.logger.info(f"GOP únicos encontrados: {len(gops_unicos)} de {len(gop_list)} buscados")
    current_app.logger.info(f"  - Desde Mis Bandejas: {sorted(list(gops_desde_bandejas))}")
    current_app.logger.info(f"  - Desde Todos los Trámites: {sorted(list(gops_desde_todos))}")

    return resultados



def _perform_login(page, user, pw):
    """Realiza el login en el sistema - Versión optimizada para Railway."""
    current_app.logger.info("=== INICIANDO PROCESO DE LOGIN ===")
    
    # Verificar que estamos en la página correcta
    current_url = page.url
    current_app.logger.info(f"URL actual: {current_url}")
    
    if "login" not in current_url.lower():
        current_app.logger.error(f"No estamos en página de login. URL: {current_url}")
        # Intentar navegar de nuevo
        page.goto("https://posadas.gestiondeobrasprivadas.com.ar/frontend/web/site/login", 
                 wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(3000)
    
    # Tomar screenshot para debug
    try:
        page.screenshot(path="login_page_before.png")
        current_app.logger.info("Screenshot de página de login tomado")
    except:
        pass
    
    # PASO 1: Llenar usuario
    current_app.logger.info("Llenando campo de usuario...")
    filled_user = False
    
    # Lista expandida de selectores para el campo usuario
    user_selectors = [
        'input[name="LoginForm[username]"]',
        'input#loginform-username',
        '#loginform-username',
        'input[name="username"]',
        'input[type="text"][name*="user" i]',
        'input[type="text"][placeholder*="usuario" i]',
        'input[type="text"]:not([type="hidden"])',
        'form input[type="text"]:first-of-type'
    ]
    
    for selector in user_selectors:
        try:
            element = page.locator(selector).first
            if element and element.is_visible():
                element.click()  # Hacer click primero
                page.wait_for_timeout(500)
                element.fill("")  # Limpiar
                page.wait_for_timeout(500)
                element.type(user, delay=100)  # Escribir con delay
                filled_user = True
                current_app.logger.info(f"✓ Usuario llenado con selector: {selector}")
                break
        except Exception as e:
            current_app.logger.debug(f"Selector usuario falló {selector}: {e}")
            continue
    
    if not filled_user:
        current_app.logger.error("ERROR: No se pudo llenar el campo de usuario")
        page.screenshot(path="login_user_error.png")
        raise RuntimeError("No se pudo llenar el campo de usuario. Revisa las credenciales.")
    
    # Esperar un poco
    page.wait_for_timeout(1000)
    
    # PASO 2: Llenar contraseña
    current_app.logger.info("Llenando campo de contraseña...")
    filled_pass = False
    
    # Lista expandida de selectores para contraseña
    pass_selectors = [
        'input[name="LoginForm[password]"]',
        'input#loginform-password',
        '#loginform-password',
        'input[name="password"]',
        'input[type="password"]',
        'input[type="password"]:not([type="hidden"])',
        'form input[type="password"]:first-of-type'
    ]
    
    for selector in pass_selectors:
        try:
            element = page.locator(selector).first
            if element and element.is_visible():
                element.click()  # Hacer click primero
                page.wait_for_timeout(500)
                element.fill("")  # Limpiar
                page.wait_for_timeout(500)
                element.type(pw, delay=100)  # Escribir con delay
                filled_pass = True
                current_app.logger.info(f"✓ Contraseña llenada con selector: {selector}")
                break
        except Exception as e:
            current_app.logger.debug(f"Selector contraseña falló {selector}: {e}")
            continue
    
    if not filled_pass:
        current_app.logger.error("ERROR: No se pudo llenar el campo de contraseña")
        page.screenshot(path="login_pass_error.png")
        raise RuntimeError("No se pudo llenar el campo de contraseña")
    
    # Esperar un poco
    page.wait_for_timeout(1000)
    
    # PASO 3: Hacer submit
    current_app.logger.info("Enviando formulario de login...")
    submitted = False
    
    # Opciones de submit
    submit_methods = [
        # Método 1: Click en botón submit
        lambda: page.locator('button[type="submit"]').first.click(),
        lambda: page.locator('input[type="submit"]').first.click(),
        lambda: page.locator('button:has-text("Ingresar")').first.click(),
        lambda: page.locator('button:has-text("Login")').first.click(),
        lambda: page.locator('.btn-primary[type="submit"]').first.click(),
        # Método 2: Presionar Enter en el campo de contraseña
        lambda: page.locator('input[type="password"]').first.press("Enter"),
        # Método 3: Submit del formulario
        lambda: page.evaluate('document.querySelector("form").submit()'),
    ]
    
    for i, method in enumerate(submit_methods):
        try:
            current_app.logger.info(f"Intentando método de submit #{i+1}")
            method()
            submitted = True
            current_app.logger.info(f"✓ Submit exitoso con método #{i+1}")
            break
        except Exception as e:
            current_app.logger.debug(f"Método #{i+1} falló: {e}")
            continue
    
    if not submitted:
        current_app.logger.error("ERROR: No se pudo enviar el formulario")
        page.screenshot(path="login_submit_error.png")
        raise RuntimeError("No se pudo enviar el formulario de login")
    
    # PASO 4: Esperar respuesta del servidor
    current_app.logger.info("Esperando respuesta del servidor...")
    
    try:
        # Esperar navegación o cambio de URL
        page.wait_for_url(lambda url: "login" not in url.lower(), timeout=30000)
        current_app.logger.info("✓ Navegación detectada")
    except:
        # Si no hay navegación, esperar un poco y verificar
        current_app.logger.info("No se detectó navegación automática, esperando...")
        page.wait_for_timeout(10000)
    
    # Verificar login exitoso
    current_url = page.url
    current_app.logger.info(f"URL después del login: {current_url}")
    
    # Verificar si seguimos en login
    if "login" in current_url.lower():
        # Buscar mensajes de error
        error_messages = []
        error_selectors = [
            '.alert-danger',
            '.error',
            '.alert-error',
            '.help-block-error',
            '.invalid-feedback',
            '[class*="error"]'
        ]
        
        for selector in error_selectors:
            try:
                elements = page.locator(selector).all()
                for elem in elements:
                    text = elem.inner_text().strip()
                    if text:
                        error_messages.append(text)
            except:
                pass
        
        if error_messages:
            current_app.logger.error(f"Errores en login: {error_messages}")
            raise RuntimeError(f"Login falló: {'; '.join(error_messages)}")
        
        current_app.logger.warning("Aún en página de login pero sin errores visibles")
    
    # Verificar indicadores de éxito
    success_indicators = [
        'a:has-text("Salir")',
        'a:has-text("Logout")',
        'a:has-text("Cerrar")',
        'button:has-text("Salir")',
        '.user-menu',
        '.navbar',
        '#main-menu'
    ]
    
    login_success = False
    for indicator in success_indicators:
        try:
            if page.locator(indicator).count() > 0:
                login_success = True
                current_app.logger.info(f"✓ Login confirmado por: {indicator}")
                break
        except:
            continue
    
    if not login_success:
        current_app.logger.warning("⚠ No se encontraron indicadores claros de login, pero continuando...")
    else:
        current_app.logger.info("✓✓✓ LOGIN EXITOSO ✓✓✓")
    
    # Screenshot final
    try:
        page.screenshot(path="login_success.png")
    except:
        pass


# === FUNCIONES HEREDADAS (PARA COMPATIBILIDAD) ===

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
