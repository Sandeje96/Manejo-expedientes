import os
import sys
import time
import logging
from datetime import datetime, date
from flask import current_app
from pathlib import Path

# -----------------------------------------------------------------------------
# Logging seguro (funciona con o sin app context)
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    # Config básico solo si no hay handlers (no pisa config de Flask/Gunicorn)
    logging.basicConfig(level=logging.INFO)

def _log_info(msg): 
    try:
        current_app.logger.info(msg)  # puede fallar fuera de app context
    except Exception:
        logger.info(msg)

def _log_warning(msg): 
    try:
        current_app.logger.warning(msg)
    except Exception:
        logger.warning(msg)

def _log_error(msg): 
    try:
        current_app.logger.error(msg)
    except Exception:
        logger.error(msg)

def _log_debug(msg): 
    try:
        current_app.logger.debug(msg)
    except Exception:
        logger.debug(msg)

# -----------------------------------------------------------------------------
# NO ejecutar nada en import time. Este módulo es seguro para usar en create_app
# -----------------------------------------------------------------------------

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
        _log_warning(f"Error parseando fecha '{fecha_str}': {e}")
    
    return None

def sync_gop_data():
    """
    Ejecuta el scraper GOP y actualiza los expedientes con información distribuida por bandejas.
    Incluye lógica de fuente: "Todos los Trámites" -> siempre Bandeja PROFESIONAL.
    EXCLUYE expedientes en formato PAPEL del proceso de sincronización.

    IMPORTANTE: esta función requiere app context para acceder a _db. No la llames
    durante create_app(); ejecutala luego con `with app.app_context(): sync_gop_data()`
    o en un worker.
    """
    try:
        from app import _db
        
        # === PASO 1: OBTENER TODOS LOS GOP DEL CPIM (SOLO DIGITALES) ===
        _log_info("=== DIAGNÓSTICO: OBTENIENDO NÚMEROS GOP DEL CPIM (SOLO DIGITALES) ===")
        
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
        _log_info(f"DIAGNÓSTICO: GOP encontrados en CPIM (solo digitales): {len(gop_list)} -> {gop_list}")
        
        if not gop_list:
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
        _log_info("=== DIAGNÓSTICO: VERIFICANDO CAMPOS EN BD ===")
        
        try:
            # Verificar que los nuevos campos existen
            _db.session.execute(
                _db.text("""
                    SELECT bandeja_cpim_nombre, bandeja_imlauer_nombre, 
                           bandeja_onetto_nombre, bandeja_profesional_nombre
                    FROM expedientes LIMIT 1
                """)
            ).fetchone()
            _log_info("✓ Campos de bandejas específicas encontrados en BD")
        except Exception as e:
            _log_error(f"✗ ERROR: Campos de bandejas NO encontrados: {e}")
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
        
        # === PASO 3: EJECUTAR SCRAPER ===
        _log_info("=== DIAGNÓSTICO: EJECUTANDO SCRAPER ===")
        resultados_por_gop = _buscar_gops_especificos(gop_list)
        
        _log_info(f"DIAGNÓSTICO: Resultados del scraper: {len(resultados_por_gop)} registros")
        for key, datos in resultados_por_gop.items():
            _log_info(f"  {key}: {datos['nro_sistema']} - {datos['usuario_asignado']} - {datos['bandeja_actual']} - {datos['fuente']}")
        
        # === PASO 4: AGRUPAR POR GOP ===
        gop_agrupados = {}
        for gop_key, datos in resultados_por_gop.items():
            gop_numero = datos['nro_sistema']
            if gop_numero not in gop_agrupados:
                gop_agrupados[gop_numero] = []
            gop_agrupados[gop_numero].append(datos)
        
        _log_info(f"DIAGNÓSTICO: GOP únicos agrupados: {len(gop_agrupados)}")

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
        
        for gop_numero, lista_datos in gop_agrupados.items():
            try:
                _log_info(f"DIAGNÓSTICO: Procesando GOP {gop_numero}")
                
                # Buscar expediente - ASEGURAR QUE SEA DIGITAL
                expediente_result = _db.session.execute(
                    _db.text("SELECT id, formato FROM expedientes WHERE gop_numero = :gop_numero AND formato = 'Digital'"),
                    {"gop_numero": gop_numero}
                ).fetchone()
                
                if not expediente_result:
                    error_msg = f"GOP {gop_numero} no encontrado en BD como expediente digital"
                    _log_warning(f"DIAGNÓSTICO: {error_msg}")
                    stats['errores'].append(error_msg)
                    continue
                
                expediente_id = expediente_result[0]
                formato = expediente_result[1]
                
                # Verificación adicional de seguridad
                if formato != 'Digital':
                    error_msg = f"GOP {gop_numero} no es formato digital (formato: {formato}), omitiendo sincronización"
                    _log_warning(f"DIAGNÓSTICO: {error_msg}")
                    stats['errores'].append(error_msg)
                    continue
                
                _log_info(f"DIAGNÓSTICO: Expediente digital ID {expediente_id} encontrado para GOP {gop_numero}")
                
                # Limpiar bandejas primero
                _log_info(f"DIAGNÓSTICO: Limpiando bandejas para expediente digital {expediente_id}")
                _limpiar_campos_bandeja(expediente_id, _db.session)
                
                # NUEVO: Recopilar datos para actualizar historial
                datos_bandejas_historial = {}
                
                # Procesar cada bandeja encontrada para este GOP
                for i, datos in enumerate(lista_datos):
                    _log_info(f"DIAGNÓSTICO: Procesando registro {i+1} de {len(lista_datos)}")
                    _log_info(f"  Usuario: '{datos.get('usuario_asignado', '')}'")
                    _log_info(f"  Fuente: '{datos.get('fuente', '')}'")
                    
                    # CAMBIO IMPORTANTE: Pasar la fuente para determinar la bandeja
                    bandeja_tipo = _determinar_bandeja_por_usuario(
                        datos.get('usuario_asignado', ''), 
                        datos.get('fuente', '')
                    )
                    _log_info(f"  Bandeja determinada: {bandeja_tipo} (Fuente: {datos.get('fuente', '')})")
                    
                    # Si viene de "Todos los Trámites", forzar usuario a "Profesional"
                    if datos.get('fuente') == "Todos los Trámites":
                        usuario_para_guardar = "Profesional"
                        _log_info(f"  Usuario forzado a 'Profesional' por venir de Todos los Trámites")
                    else:
                        usuario_para_guardar = str(datos.get('usuario_asignado', ''))[:200]
                    
                    # Parsear fechas
                    fecha_entrada = _parsear_fecha(datos.get('fecha_entrada', ''))
                    fecha_en_bandeja = _parsear_fecha(datos.get('fecha_en_bandeja', ''))
                    _log_info(f"  Fechas: entrada={fecha_entrada}, en_bandeja={fecha_en_bandeja}")
                    
                    # Preparar actualización
                    campos_update = {
                        f"bandeja_{bandeja_tipo}_nombre": str(datos.get('bandeja_actual', ''))[:200],
                        f"bandeja_{bandeja_tipo}_usuario": usuario_para_guardar,
                        f"bandeja_{bandeja_tipo}_fecha": fecha_en_bandeja or fecha_entrada,
                        f"bandeja_{bandeja_tipo}_sincronizacion": datetime.utcnow(),
                    }
                    
                    _log_info(f"  Campos a actualizar: {campos_update}")
                    
                    # Actualizar BD
                    set_clause = ', '.join([f"{campo} = :{campo}" for campo in campos_update.keys()])
                    query = f"""
                        UPDATE expedientes 
                        SET {set_clause}
                        WHERE id = :expediente_id
                    """
                    
                    _log_info(f"  Query: {query}")
                    
                    _db.session.execute(
                        _db.text(query),
                        {**campos_update, "expediente_id": expediente_id}
                    )
                    
                    stats[f'bandejas_{bandeja_tipo}'] += 1
                    _log_info(f"  ✓ Actualizado bandeja {bandeja_tipo} desde {datos.get('fuente', '')}")
                    
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
                
                # NUEVO: Actualizar historial de bandejas
                try:
                    _log_info(f"DIAGNÓSTICO: Actualizando historial para expediente digital {expediente_id}")
                    _actualizar_historial_tras_sincronizacion(expediente_id, datos_bandejas_historial)
                    _log_info(f"DIAGNÓSTICO: ✓ Historial actualizado para expediente digital {expediente_id}")
                except Exception as hist_error:
                    _log_warning(f"DIAGNÓSTICO: Error actualizando historial para {expediente_id}: {hist_error}")
                    # No fallar la sincronización por un error en el historial
                
                stats['expedientes_actualizados'] += 1
                _log_info(f"DIAGNÓSTICO: ✓ Expediente digital {expediente_id} actualizado completamente")
                
            except Exception as e:
                error_msg = f"Error actualizando GOP {gop_numero}: {e}"
                _log_error(f"DIAGNÓSTICO: {error_msg}")
                stats['errores'].append(error_msg)
                import traceback
                _log_error(f"DIAGNÓSTICO: Traceback: {traceback.format_exc()}")
        
        # Commit final
        _log_info("DIAGNÓSTICO: Realizando commit...")
        _db.session.commit()
        _log_info("DIAGNÓSTICO: ✓ Commit exitoso")
        
        _log_info(f"DIAGNÓSTICO: Estadísticas finales: {stats}")
        return stats
        
    except Exception as e:
        _log_error(f"DIAGNÓSTICO: Error general en sync_gop_data: {e}")
        import traceback
        _log_error(f"DIAGNÓSTICO: Traceback completo: {traceback.format_exc()}")
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
    VERSIÓN CORREGIDA: Cierra bandejas que ya no están activas.
    
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
        
        # Primero: Cerrar TODOS los registros activos que NO estén en datos_nuevos
        # o que estén vacíos en datos_nuevos
        bandejas_con_datos = set()
        for bandeja_tipo, datos in datos_nuevos.items():
            if datos and datos.get('nombre'):
                bandejas_con_datos.add(bandeja_tipo)
        
        # Obtener todos los registros activos actuales
        registros_activos = _db.session.execute(
            _db.text("""
                SELECT id, bandeja_tipo, fecha_inicio
                FROM historial_bandejas
                WHERE expediente_id = :expediente_id
                AND fecha_fin IS NULL
            """),
            {"expediente_id": expediente_id}
        ).fetchall()
        
        # Cerrar los que ya no están activos
        for registro in registros_activos:
            if registro[1] not in bandejas_con_datos:
                # Esta bandeja ya no tiene datos, cerrarla
                dias_en_bandeja = (date.today() - registro[2]).days
                _db.session.execute(
                    _db.text("""
                        UPDATE historial_bandejas
                        SET fecha_fin = :fecha_fin,
                            dias_en_bandeja = :dias,
                            updated_at = :now
                        WHERE id = :registro_id
                    """),
                    {
                        "fecha_fin": date.today(),
                        "dias": max(0, dias_en_bandeja),
                        "now": datetime.utcnow(),
                        "registro_id": registro[0]
                    }
                )
                _log_info(f"Historial: Cerrado registro de bandeja {registro[1]} para expediente {expediente_id}")
        
        # Segundo: Procesar cada bandeja que tiene datos nuevos
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
                # Solo actualizar si cambió el nombre o usuario
                if (registro_activo[1] != nombre_bandeja or 
                    registro_activo[2] != usuario):
                    
                    # Actualizar el registro existente con los nuevos datos
                    _db.session.execute(
                        _db.text("""
                            UPDATE historial_bandejas
                            SET bandeja_nombre = :nombre,
                                usuario_asignado = :usuario,
                                updated_at = :now
                            WHERE id = :registro_id
                        """),
                        {
                            "nombre": nombre_bandeja[:200],
                            "usuario": usuario[:200],
                            "now": datetime.utcnow(),
                            "registro_id": registro_activo[0]
                        }
                    )
                    _log_info(f"Historial: Actualizado registro en bandeja {bandeja_tipo} para expediente {expediente_id}")
            else:
                # No existe registro activo, crear uno nuevo
                _crear_nuevo_registro_historial(
                    expediente_id, bandeja_tipo, nombre_bandeja, 
                    usuario, fecha_bandeja
                )
                
                _log_info(f"Historial: Expediente {expediente_id} entró a bandeja {bandeja_tipo}")
    
    except Exception as e:
        # Si hay error, hacer rollback y continuar
        _db.session.rollback()
        _log_warning(f"No se pudo actualizar historial para expediente {expediente_id}: {e}")

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
        _log_warning(f"Error creando registro historial: {e}")

def _buscar_gops_en_pagina_simple(page, gops_buscados, fuente, gop_especifico):
    """
    Versión simplificada para buscar GOP después de aplicar filtro.
    Busca en menos filas ya que el filtro debería reducir los resultados.
    """
    encontrados = {}
    
    try:
        rows = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr")
        count = rows.count()
        
        _log_info(f"[{fuente}] Búsqueda filtrada para GOP {gop_especifico}: {count} filas encontradas")
        
        # Si hay pocas filas, mostrar el contenido para debug
        if count <= 10:
            _log_info(f"[{fuente}] DEBUG: Mostrando todas las {count} filas:")
            for i in range(count):
                try:
                    row = rows.nth(i)
                    row_text = row.inner_text().strip()
                    _log_info(f"  Fila {i}: {row_text[:150]}")
                except:
                    pass
        
        for i in range(min(count, 50)):  # Buscar en máximo 50 filas (debería ser suficiente)
            try:
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()
                
                if cell_count >= 6:
                    nro_sistema = cells.nth(0).inner_text().strip()
                    
                    _log_debug(f"[{fuente}] Fila {i}: GOP='{nro_sistema}'")
                    
                    if nro_sistema in gops_buscados:
                        clave_unica = f"{nro_sistema}_{fuente}_filtrado_{i}"
                        
                        _log_info(f"[{fuente}] ¡ENCONTRADO GOP {nro_sistema} con filtro!")
                        
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
                        
                        _log_info(f"[{fuente}] Datos extraídos:")
                        _log_info(f"  Bandeja: {encontrados[clave_unica]['bandeja_actual']}")
                        _log_info(f"  Usuario: {encontrados[clave_unica]['usuario_asignado']}")
                        
                        break  # Si encontramos el GOP, no necesitamos seguir buscando
                        
            except Exception as e:
                _log_warning(f"[{fuente}] Error procesando fila {i}: {e}")
                continue
                
    except Exception as e:
        _log_error(f"[{fuente}] Error en búsqueda filtrada: {e}")
        page.screenshot(path=f"error_busqueda_filtrada_{gop_especifico}.png")
    
    return encontrados
    
def _buscar_gops_especificos(gop_list):
    """
    Busca números GOP específicos con lógica optimizada:
    1. Busca TODOS los GOP en "Mis Bandejas"
    2. Solo busca en "Todos los Trámites" los GOP que NO se encontraron en "Mis Bandejas"
    """
    from dotenv import load_dotenv
    from playwright.sync_api import sync_playwright
    import subprocess
    import sys as _sys  # evitar sombra de sys global
    
    # Verificar e instalar navegadores si es necesario
    try:
        _log_info("Verificando navegadores de Playwright...")
        
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                browser.close()
                _log_info("✓ Navegadores disponibles")
            except Exception as browser_error:
                _log_info("Navegadores no encontrados, instalando...")
                
                result = subprocess.run([
                    _sys.executable, "-m", "playwright", "install", "chromium"
                ], capture_output=True, text=True, timeout=300)
                
                if result.returncode != 0:
                    raise RuntimeError(f"Error instalando navegadores: {result.stderr}")
                
                _log_info("✓ Navegadores instalados exitosamente")
    
    except Exception as install_error:
        _log_error(f"Error con navegadores: {install_error}")
        raise RuntimeError(f"No se pudieron instalar los navegadores de Playwright: {install_error}")
    
    # Cargar variables de entorno
    load_dotenv(override=True)
    user = os.getenv("USER_MUNI", "")
    pw = os.getenv("PASS_MUNI", "")
    
    _log_info(f"Credenciales cargadas - Usuario: {user[:3]}*** Contraseña: {'*' * len(pw) if pw else 'VACÍA'}")
    
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
    HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
    
    resultados = {}
    gops_pendientes = set(gop_list)  # Conjunto de GOP que aún necesitan buscarse
    
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # opcional, por claridad

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # === LOGIN ===
            _log_info("=== REALIZANDO LOGIN ===")
            page.goto(LOGIN_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            
            _perform_login(page, user, pw)
            
            # === PASO 1: BUSCAR EN MIS BANDEJAS ===
            _log_info("=== PASO 1: BUSCANDO EN MIS BANDEJAS ===")
            _log_info(f"GOP a buscar en Mis Bandejas: {list(gops_pendientes)}")
            
            try:
                page.goto(MY_TRAYS_URL, wait_until="networkidle")
                encontrados_bandejas = _buscar_gops_en_pagina_multiple(page, list(gops_pendientes), "Mis Bandejas")
                
                # Agregar resultados y REMOVER de pendientes
                gops_encontrados_bandejas = set()
                for gop_key, datos in encontrados_bandejas.items():
                    resultados[gop_key] = datos
                    gop_numero = datos['nro_sistema']
                    gops_encontrados_bandejas.add(gop_numero)
                
                # Actualizar lista de pendientes
                gops_pendientes -= gops_encontrados_bandejas
                
                _log_info(f"✓ Encontrados en Mis Bandejas: {len(encontrados_bandejas)} registros")
                _log_info(f"✓ GOP encontrados en Mis Bandejas: {list(gops_encontrados_bandejas)}")
                _log_info(f"⏳ GOP pendientes para Todos los Trámites: {list(gops_pendientes)}")
                
            except Exception as e:
                _log_error(f"Error en Mis Bandejas: {e}")
                page.screenshot(path="mis_bandejas_error.png")
            
            # === PASO 2: BUSCAR EN TODOS LOS TRÁMITES (SOLO LOS PENDIENTES) ===
            if gops_pendientes:
                _log_info(f"=== PASO 2: BUSCANDO EN TODOS LOS TRÁMITES ===")
                _log_info(f"Solo buscando GOP pendientes: {list(gops_pendientes)}")
                
                try:
                    # Buscar cada GOP pendiente individualmente usando filtro
                    encontrados_todos_totales = {}
                    
                    for gop_numero in gops_pendientes:
                        _log_info(f"DEBUG: Buscando GOP {gop_numero} en Todos los Trámites...")
                        
                        # Navegar a la página
                        page.goto(ALL_FORMALITIES_URL, wait_until="networkidle")
                        _log_info(f"DEBUG: URL actual: {page.url}")
                        page.wait_for_timeout(3000)
                        
                        # Buscar campo de filtro por "Nro. Sistema" o similar
                        filtro_aplicado = False
                        
                        # Intentar diferentes selectores para el campo de filtro
                        selectores_filtro = [
                            'input[name*="numero"]',
                            'input[name*="sistema"]', 
                            'input[name*="nro"]',
                            'input[placeholder*="número" i]',
                            'input[placeholder*="sistema" i]',
                            'input[placeholder*="nro" i]',
                            'input[placeholder*="Número"]',
                            'input[placeholder*="Sistema"]',
                            'input[placeholder*="Nro"]',
                            'input[id*="numero"]',
                            'input[id*="sistema"]',
                            'input[id*="nro"]',
                            '.search-input',
                            '[data-attribute="nro_sistema"]',
                            'input[type="text"]'
                        ]
                        
                        for selector in selectores_filtro:
                            try:
                                filtro_elements = page.locator(selector)
                                count = filtro_elements.count()
                                
                                if count > 0:
                                    _log_info(f"DEBUG: Encontrados {count} elementos con selector: {selector}")
                                    
                                    # Probar cada elemento encontrado
                                    for i in range(count):
                                        try:
                                            filtro_element = filtro_elements.nth(i)
                                            
                                            # Verificar si es visible y habilitado
                                            if filtro_element.is_visible() and filtro_element.is_enabled():
                                                _log_info(f"DEBUG: Intentando filtro con selector: {selector} (elemento {i})")
                                                
                                                # Limpiar y escribir el GOP
                                                filtro_element.clear()
                                                filtro_element.fill(gop_numero)
                                                _log_info(f"DEBUG: Escrito '{gop_numero}' en filtro")
                                                
                                                # Buscar botón de búsqueda o presionar Enter
                                                try:
                                                    # Intentar presionar Enter
                                                    filtro_element.press("Enter")
                                                    _log_info("DEBUG: Presionado Enter en filtro")
                                                except Exception:
                                                    # Si no funciona Enter, buscar botón
                                                    botones_buscar = [
                                                        'button[type="submit"]',
                                                        'button:has-text("Buscar")',
                                                        'button:has-text("Filtrar")',
                                                        'button:has-text("Search")',
                                                        '.btn-search',
                                                        '.search-btn',
                                                        'input[type="submit"]'
                                                    ]
                                                    
                                                    for btn_selector in botones_buscar:
                                                        try:
                                                            btn = page.locator(btn_selector).first
                                                            if btn.count() > 0 and btn.is_visible():
                                                                btn.click()
                                                                _log_info(f"DEBUG: Clicked botón búsqueda: {btn_selector}")
                                                                break
                                                        except Exception:
                                                            continue
                                                
                                                # Esperar a que se aplique el filtro
                                                page.wait_for_timeout(3000)
                                                page.wait_for_load_state("networkidle")
                                                
                                                filtro_aplicado = True
                                                _log_info(f"DEBUG: ✓ Filtro aplicado para GOP {gop_numero}")
                                                break
                                            
                                        except Exception as e:
                                            _log_debug(f"DEBUG: Elemento {i} falló: {e}")
                                            continue
                                    
                                    if filtro_aplicado:
                                        break
                                        
                            except Exception as e:
                                _log_debug(f"DEBUG: Selector {selector} falló: {e}")
                                continue
                        
                        if not filtro_aplicado:
                            _log_warning(f"DEBUG: ✗ No se pudo aplicar filtro para GOP {gop_numero}")
                            page.screenshot(path=f"debug_filtro_fallo_{gop_numero}.png")
                        
                        # Buscar en la tabla después del filtro
                        _log_info(f"DEBUG: Buscando GOP {gop_numero} en tabla filtrada...")
                        page.wait_for_timeout(2000)
                        
                        # Buscar en la tabla (ahora debería tener pocos resultados)
                        encontrados_gop = _buscar_gops_en_pagina_simple(page, [gop_numero], "Todos los Trámites", gop_numero)
                        
                        if encontrados_gop:
                            _log_info(f"DEBUG: ✓ GOP {gop_numero} encontrado en Todos los Trámites")
                            encontrados_todos_totales.update(encontrados_gop)
                        else:
                            _log_warning(f"DEBUG: ✗ GOP {gop_numero} NO encontrado en Todos los Trámites")
                    
                    _log_info(f"✓ Encontrados en Todos los Trámites: {len(encontrados_todos_totales)} registros")
                    
                    # Agregar a resultados
                    for gop_key, datos in encontrados_todos_totales.items():
                        resultados[gop_key] = datos
                    
                except Exception as e:
                    _log_error(f"Error en Todos los Trámites: {e}")
                    import traceback
                    _log_error(f"Traceback: {traceback.format_exc()}")
                    page.screenshot(path="todos_tramites_error.png")
            else:
                _log_info("=== TODOS LOS GOP ENCONTRADOS EN MIS BANDEJAS ===")
                _log_info("✓ No es necesario buscar en Todos los Trámites")
        
        finally:
            browser.close()
    
    _log_info(f"Total registros encontrados: {len(resultados)}")
    
    # LOGGING DETALLADO de los resultados
    gops_unicos_encontrados = set()
    gops_desde_bandejas = set()
    gops_desde_todos_tramites = set()
    
    for gop_key, datos in resultados.items():
        gop_numero = datos['nro_sistema']
        gops_unicos_encontrados.add(gop_numero)
        
        if datos['fuente'] == 'Mis Bandejas':
            gops_desde_bandejas.add(gop_numero)
        else:
            gops_desde_todos_tramites.add(gop_numero)
        
        _log_info(f"Resultado: {gop_numero} desde {datos['fuente']} - Usuario: {datos['usuario_asignado']}")
    
    _log_info(f"GOP únicos encontrados: {len(gops_unicos_encontrados)} de {len(gop_list)} buscados")
    _log_info(f"  - Desde Mis Bandejas: {list(gops_desde_bandejas)}")
    _log_info(f"  - Desde Todos los Trámites: {list(gops_desde_todos_tramites)}")
    
    # Mostrar GOP no encontrados
    gops_no_encontrados = set(gop_list) - gops_unicos_encontrados
    if gops_no_encontrados:
        _log_warning(f"GOP NO encontrados en ninguna fuente: {list(gops_no_encontrados)}")
    
    return resultados

def _buscar_gops_en_pagina_multiple(page, gops_buscados, fuente):
    """
    Busca números GOP específicos en la página actual.
    VERSIÓN DEBUG: Con logging extra para diagnosticar "Todos los Trámites"
    """
    encontrados = {}
    
    try:
        # Esperar un poco más para que cargue la tabla
        page.wait_for_timeout(5000)
        
        rows = page.locator("table tbody tr, .table tbody tr, .grid-view tbody tr")
        count = rows.count()
        
        _log_info(f"[{fuente}] DEBUG: Analizando {count} filas...")
        _log_info(f"[{fuente}] DEBUG: Buscando GOP: {gops_buscados}")
        
        if count == 0:
            _log_warning(f"[{fuente}] DEBUG: ¡No se encontraron filas en la tabla!")
            # Tomar screenshot para debug
            page.screenshot(path=f"debug_{fuente.lower().replace(' ', '_')}_no_rows.png")
            
            # Intentar otros selectores de tabla
            alt_selectors = [
                "tr",
                ".grid-row",
                "[data-key]",
                ".item"
            ]
            
            for alt_sel in alt_selectors:
                try:
                    alt_rows = page.locator(alt_sel)
                    alt_count = alt_rows.count()
                    if alt_count > 0:
                        _log_info(f"[{fuente}] DEBUG: Encontradas {alt_count} filas con selector alternativo: {alt_sel}")
                        rows = alt_rows
                        count = alt_count
                        break
                except:
                    continue
        
        # Procesar primeras 10 filas para debug
        debug_limit = min(count, 10)
        _log_info(f"[{fuente}] DEBUG: Mostrando contenido de primeras {debug_limit} filas:")
        
        for i in range(debug_limit):
            try:
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()
                
                # Obtener contenido de la primera celda (número GOP)
                primera_celda = cells.nth(0).inner_text().strip() if cell_count > 0 else "VACÍA"
                _log_info(f"[{fuente}] DEBUG Fila {i}: {cell_count} celdas, Primera celda: '{primera_celda}'")
                
                # Si es una de las primeras 3 filas, mostrar todas las celdas
                if i < 3:
                    contenido_fila = []
                    for j in range(min(cell_count, 8)):  # Primeras 8 celdas
                        try:
                            celda_contenido = cells.nth(j).inner_text().strip()
                            contenido_fila.append(f"[{j}]='{celda_contenido[:30]}'")
                        except:
                            contenido_fila.append(f"[{j}]=ERROR")
                    _log_info(f"[{fuente}] DEBUG Fila {i} completa: {' | '.join(contenido_fila)}")
                
            except Exception as e:
                _log_warning(f"[{fuente}] DEBUG: Error procesando fila {i} para debug: {e}")
        
        # Ahora buscar los GOP específicos
        _log_info(f"[{fuente}] DEBUG: Iniciando búsqueda específica de GOP...")
        
        for i in range(min(count, 200)):
            try:
                row = rows.nth(i)
                cells = row.locator("td")
                cell_count = cells.count()
                
                if cell_count >= 6:
                    nro_sistema = cells.nth(0).inner_text().strip()
                    
                    # DEBUG: Mostrar todos los números encontrados
                    if nro_sistema:
                        _log_debug(f"[{fuente}] DEBUG: Fila {i} - GOP encontrado: '{nro_sistema}'")
                    
                    if nro_sistema in gops_buscados:
                        # Crear clave única para cada registro
                        clave_unica = f"{nro_sistema}_{fuente}_{i}"
                        
                        _log_info(f"[{fuente}] ¡¡¡ENCONTRADO GOP {nro_sistema} (registro {i})!!!")
                        
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
                        
                        _log_info(f"[{fuente}] Datos extraídos:")
                        _log_info(f"  Bandeja: {encontrados[clave_unica]['bandeja_actual']}")
                        _log_info(f"  Usuario: {encontrados[clave_unica]['usuario_asignado']}")
                        _log_info(f"  Estado: {encontrados[clave_unica]['estado']}")
                        
            except Exception as e:
                _log_warning(f"[{fuente}] Error procesando fila {i}: {e}")
                continue
                
    except Exception as e:
        _log_error(f"[{fuente}] Error general: {e}")
        page.screenshot(path=f"error_{fuente.lower().replace(' ', '_')}_general.png")
    
    _log_info(f"[{fuente}] DEBUG: Búsqueda completada. Encontrados: {len(encontrados)} registros")
    return encontrados

def _perform_login(page, user, pw):
    """Realiza el login en el sistema."""
    _log_info("Iniciando proceso de login...")
    
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
                _log_info(f"Usuario llenado con selector: {selector}")
                break
        except Exception as e:
            _log_debug(f"Falló selector de usuario {selector}: {e}")
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
                _log_info(f"Contraseña llenada con selector: {selector}")
                break
        except Exception as e:
            _log_debug(f"Falló selector de contraseña {selector}: {e}")
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
                _log_info(f"Intentando submit con selector: {selector}")
                page.click(selector)
                submitted = True
                _log_info(f"Submit exitoso con selector: {selector}")
                break
        except Exception as e:
            _log_debug(f"Falló selector de submit {selector}: {e}")
            continue
    
    if not submitted:
        page.screenshot(path="login_submit_debug.png")
        raise RuntimeError("No se pudo hacer click en el botón de login")
    
    # Esperar a que se complete el login
    _log_info("Esperando respuesta del login...")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)
    
    # Verificar que el login fue exitoso
    current_url = page.url
    _log_info(f"URL después del login: {current_url}")
    
    # Verificar diferentes indicadores de login exitoso
    if "login" in current_url.lower():
        # Tomar screenshot para debug
        page.screenshot(path="login_failed_debug.png")
        
        # Verificar si hay mensajes de error en la página
        try:
            error_messages = page.locator('.alert-danger, .error, .alert-error, .help-block-error').all_inner_texts()
            if error_messages:
                _log_error(f"Mensajes de error en login: {error_messages}")
        except Exception:
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
                _log_info(f"Login confirmado por indicator: {indicator}")
                break
        except Exception:
            continue
    
    if not logged_in:
        _log_warning("No se encontraron indicadores claros de login exitoso, pero continuando...")
    
    _log_info("Login completado exitosamente")

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
    # Import pesado movido aquí para evitar fallas durante create_app/import
    import pandas as pd
    
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
        _log_info("Navegando a página de login...")
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
                    _log_info(f"Usuario llenado con selector: {selector}")
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
                    _log_info(f"Contraseña llenada con selector: {selector}")
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
                    _log_info(f"Submit con selector: {selector}")
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
        _log_info(f"URL después del login: {current_url}")
        
        if "login" in current_url.lower():
            page.screenshot(path="login_failed.png")
            raise RuntimeError("Login falló - aún en página de login. Verificá credenciales.")
        
        # Ir a la página de bandejas
        _log_info("Navegando a página de bandejas...")
        try:
            page.goto(MY_TRAYS_URL, wait_until="networkidle")
        except Exception as e:
            _log_error(f"Error navegando a bandejas: {e}")
            # Intentar navegar por menu si falla la URL directa
            try:
                # Buscar enlace a bandejas en el menú
                page.click('a:has-text("Bandejas")', timeout=5000)
                page.wait_for_load_state("networkidle")
            except:
                page.screenshot(path="navigation_failed.png")
                raise RuntimeError("No se pudo acceder a la página de bandejas")
        
        # Extraer datos de la tabla
        _log_info("Extrayendo datos de la tabla...")
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
                        _log_info(f"Tabla encontrada con selector '{selector}': {count} filas")
                        table_found = True
                        break
                except:
                    continue
            
            if not table_found:
                page.screenshot(path="table_not_found.png")
                _log_warning("No se encontró tabla de datos")
                # Guardar CSV vacío igual
                pd.DataFrame([]).to_csv(out_csv, index=False, encoding="utf-8-sig")
                return out_csv  # Retornar CSV vacío
            
            count = rows.count()
            _log_info(f"Procesando {count} filas...")
            
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
                    _log_warning(f"Error procesando fila {i}: {e}")
                    continue
                    
        except Exception as e:
            _log_error(f"Error extrayendo datos: {e}")
            page.screenshot(path="extraction_error.png")
        
        _log_info(f"Extracción completada: {len(all_rows)} registros")
        
        browser.close()
    
    # Guardar CSV
    df = pd.DataFrame(all_rows)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    
    _log_info(f"Scraper completado: {len(df)} filas -> {out_csv}")
    return out_csv
