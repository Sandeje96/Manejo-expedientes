import json
from datetime import datetime, date
from decimal import Decimal
from flask import current_app
from sqlalchemy import and_, or_

class TasasAnalyzer:
    """Clase para analizar tasas de visado y calcular honorarios de ingenieros."""
    
    def __init__(self, db_session):
        self.db = db_session
    
    def analizar_periodo(self, fecha_desde, fecha_hasta, incluir_no_pagados=True):
        """
        Analiza las tasas de visado en un período específico.
        
        Args:
            fecha_desde: Fecha inicio del período
            fecha_hasta: Fecha fin del período
            incluir_no_pagados: Si incluir expedientes no pagados en el análisis
            
        Returns:
            dict: Diccionario con el análisis completo
        """
        # Obtener el modelo Expediente del contexto de la aplicación
        with current_app.app_context():
            # Acceder al modelo a través del registro de SQLAlchemy de la app actual
            from flask import g
            if not hasattr(g, '_expediente_model'):
                # Buscar el modelo en el contexto actual
                for name, obj in current_app.__dict__.items():
                    if hasattr(obj, '__name__') and obj.__name__ == 'Expediente':
                        g._expediente_model = obj
                        break
                
                # Si no lo encontramos en app, buscarlo en las extensiones
                if not hasattr(g, '_expediente_model'):
                    db = current_app.extensions['sqlalchemy']
                    # Buscar en las clases definidas
                    for table_name, model_class in db.Model.registry._class_registry.items():
                        if hasattr(model_class, '__tablename__') and model_class.__tablename__ == 'expedientes':
                            g._expediente_model = model_class
                            break
            
            if hasattr(g, '_expediente_model'):
                Expediente = g._expediente_model
            else:
                # Fallback: usar texto SQL directo
                return self._analizar_con_sql_directo(fecha_desde, fecha_hasta, incluir_no_pagados)
        
        # Construir consulta base
        query_base = Expediente.query.filter(
            and_(
                Expediente.fecha_salida >= fecha_desde,
                Expediente.fecha_salida <= fecha_hasta,
                Expediente.incluido_en_cierre_id.is_(None)  # No incluidos en cierres anteriores
            )
        )
        
        # Expedientes pagados
        expedientes_pagados = query_base.filter(
            Expediente.estado_pago_visado == 'pagado'
        ).all()
        
        # Expedientes no pagados (si se solicita)
        expedientes_no_pagados = []
        if incluir_no_pagados:
            expedientes_no_pagados = query_base.filter(
                or_(
                    Expediente.estado_pago_visado == 'pendiente',
                    Expediente.estado_pago_visado.is_(None)
                )
            ).all()
        
        # Calcular totales por tipo de visado (solo pagados)
        totales_por_tipo = self._calcular_totales_por_tipo(expedientes_pagados)
        
        # Calcular honorarios por ingeniero
        honorarios = self._calcular_honorarios(totales_por_tipo)
        
        # Preparar datos de expedientes para mostrar
        datos_pagados = self._preparar_datos_expedientes(expedientes_pagados, True)
        datos_no_pagados = self._preparar_datos_expedientes(expedientes_no_pagados, False)
        
        return {
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
            'expedientes_pagados': datos_pagados,
            'expedientes_no_pagados': datos_no_pagados,
            'totales_por_tipo': totales_por_tipo,
            'honorarios': honorarios,
            'resumen': self._crear_resumen(totales_por_tipo, honorarios, len(datos_pagados), len(datos_no_pagados))
        }
    
    def _analizar_con_sql_directo(self, fecha_desde, fecha_hasta, incluir_no_pagados):
        """Análisis usando SQL directo si no podemos acceder al modelo."""
        from sqlalchemy import text
        
        # Consulta SQL para expedientes pagados
        sql_pagados = text("""
            SELECT id, fecha_salida, nombre_profesional, nombre_comitente, 
                   nro_expediente_cpim, gop_numero,
                   COALESCE(tasa_visado_gas_monto, 0) as gas,
                   COALESCE(tasa_visado_salubridad_monto, 0) as salubridad,
                   COALESCE(tasa_visado_electrica_monto, 0) as electrica,
                   COALESCE(tasa_visado_electromecanica_monto, 0) as electromecanica
            FROM expedientes 
            WHERE fecha_salida >= :fecha_desde 
            AND fecha_salida <= :fecha_hasta
            AND estado_pago_visado = 'pagado'
            AND (incluido_en_cierre_id IS NULL)
        """)
        
        expedientes_pagados = self.db.execute(sql_pagados, {
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta
        }).fetchall()
        
        # Convertir a formato de datos
        datos_pagados = []
        totales_por_tipo = {
            'gas': Decimal('0'),
            'salubridad': Decimal('0'),
            'electrica': Decimal('0'),
            'electromecanica': Decimal('0')
        }
        
        for exp in expedientes_pagados:
            gas = Decimal(str(exp.gas or 0))
            salubridad = Decimal(str(exp.salubridad or 0))
            electrica = Decimal(str(exp.electrica or 0))
            electromecanica = Decimal(str(exp.electromecanica or 0))
            
            totales_por_tipo['gas'] += gas
            totales_por_tipo['salubridad'] += salubridad
            totales_por_tipo['electrica'] += electrica
            totales_por_tipo['electromecanica'] += electromecanica
            
            datos_pagados.append({
                'id': exp.id,
                'fecha': exp.fecha_salida,
                'profesional': exp.nombre_profesional,
                'comitente': exp.nombre_comitente,
                'nro_expediente_cpim': exp.nro_expediente_cpim,
                'gop_numero': exp.gop_numero,
                'gas': gas,
                'salubridad': salubridad,
                'electrica': electrica,
                'electromecanica': electromecanica,
                'total_visados': gas + salubridad + electrica + electromecanica,
                'estado_pago': 'Pagado'
            })
        
        # Calcular honorarios
        honorarios = self._calcular_honorarios(totales_por_tipo)
        
        # Expedientes no pagados (si se solicita)
        datos_no_pagados = []
        if incluir_no_pagados:
            sql_no_pagados = text("""
                SELECT id, fecha_salida, nombre_profesional, nombre_comitente, 
                       nro_expediente_cpim, gop_numero,
                       COALESCE(tasa_visado_gas_monto, 0) as gas,
                       COALESCE(tasa_visado_salubridad_monto, 0) as salubridad,
                       COALESCE(tasa_visado_electrica_monto, 0) as electrica,
                       COALESCE(tasa_visado_electromecanica_monto, 0) as electromecanica
                FROM expedientes 
                WHERE fecha_salida >= :fecha_desde 
                AND fecha_salida <= :fecha_hasta
                AND (estado_pago_visado = 'pendiente' OR estado_pago_visado IS NULL)
                AND (incluido_en_cierre_id IS NULL)
            """)
            
            expedientes_no_pagados = self.db.execute(sql_no_pagados, {
                'fecha_desde': fecha_desde,
                'fecha_hasta': fecha_hasta
            }).fetchall()
            
            for exp in expedientes_no_pagados:
                gas = Decimal(str(exp.gas or 0))
                salubridad = Decimal(str(exp.salubridad or 0))
                electrica = Decimal(str(exp.electrica or 0))
                electromecanica = Decimal(str(exp.electromecanica or 0))
                
                datos_no_pagados.append({
                    'id': exp.id,
                    'fecha': exp.fecha_salida,
                    'profesional': exp.nombre_profesional,
                    'comitente': exp.nombre_comitente,
                    'nro_expediente_cpim': exp.nro_expediente_cpim,
                    'gop_numero': exp.gop_numero,
                    'gas': gas,
                    'salubridad': salubridad,
                    'electrica': electrica,
                    'electromecanica': electromecanica,
                    'total_visados': gas + salubridad + electrica + electromecanica,
                    'estado_pago': 'No pagado'
                })
        
        return {
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
            'expedientes_pagados': datos_pagados,
            'expedientes_no_pagados': datos_no_pagados,
            'totales_por_tipo': totales_por_tipo,
            'honorarios': honorarios,
            'resumen': self._crear_resumen(totales_por_tipo, honorarios, len(datos_pagados), len(datos_no_pagados))
        }
    
    def _calcular_totales_por_tipo(self, expedientes):
        """Calcula totales por cada tipo de visado."""
        totales = {
            'gas': Decimal('0'),
            'salubridad': Decimal('0'),
            'electrica': Decimal('0'),
            'electromecanica': Decimal('0')
        }
        
        for exp in expedientes:
            if hasattr(exp, 'tasa_visado_gas_monto') and exp.tasa_visado_gas_monto:
                totales['gas'] += exp.tasa_visado_gas_monto
            if hasattr(exp, 'tasa_visado_salubridad_monto') and exp.tasa_visado_salubridad_monto:
                totales['salubridad'] += exp.tasa_visado_salubridad_monto
            if hasattr(exp, 'tasa_visado_electrica_monto') and exp.tasa_visado_electrica_monto:
                totales['electrica'] += exp.tasa_visado_electrica_monto
            if hasattr(exp, 'tasa_visado_electromecanica_monto') and exp.tasa_visado_electromecanica_monto:
                totales['electromecanica'] += exp.tasa_visado_electromecanica_monto
        
        return totales
    
    def _calcular_honorarios(self, totales_por_tipo):
        """Calcula honorarios para cada ingeniero y el CPIM."""
        # Totales por ingeniero
        total_imlauer = totales_por_tipo['gas'] + totales_por_tipo['salubridad']
        total_onetto = totales_por_tipo['electrica'] + totales_por_tipo['electromecanica']
        total_general = total_imlauer + total_onetto
        
        # Cálculo de honorarios (70% para ingenieros, 30% para CPIM)
        honorarios = {
            'imlauer': {
                'total_tasas': total_imlauer,
                'para_ingeniero': total_imlauer * Decimal('0.70'),
                'para_cpim': total_imlauer * Decimal('0.30'),
                'tipos_visado': ['Gas', 'Salubridad']
            },
            'onetto': {
                'total_tasas': total_onetto,
                'para_ingeniero': total_onetto * Decimal('0.70'),
                'para_cpim': total_onetto * Decimal('0.30'),
                'tipos_visado': ['Eléctrica', 'Electromecánica']
            },
            'totales_generales': {
                'total_todas_tasas': total_general,
                'total_para_ingenieros': (total_imlauer + total_onetto) * Decimal('0.70'),
                'total_para_cpim': (total_imlauer + total_onetto) * Decimal('0.30')
            }
        }
        
        return honorarios
    
    def _preparar_datos_expedientes(self, expedientes, es_pagado):
        """Prepara los datos de expedientes para mostrar en el análisis."""
        datos = []
        
        for exp in expedientes:
            # Calcular total de visados para este expediente
            total_visados = Decimal('0')
            visados_detalle = {}
            
            # Manejar tanto objetos como resultados SQL
            if hasattr(exp, 'tasa_visado_gas_monto'):
                # Es un objeto modelo
                if exp.tasa_visado_gas_monto:
                    total_visados += exp.tasa_visado_gas_monto
                    visados_detalle['gas'] = exp.tasa_visado_gas_monto
                
                if exp.tasa_visado_salubridad_monto:
                    total_visados += exp.tasa_visado_salubridad_monto
                    visados_detalle['salubridad'] = exp.tasa_visado_salubridad_monto
                
                if exp.tasa_visado_electrica_monto:
                    total_visados += exp.tasa_visado_electrica_monto
                    visados_detalle['electrica'] = exp.tasa_visado_electrica_monto
                
                if exp.tasa_visado_electromecanica_monto:
                    total_visados += exp.tasa_visado_electromecanica_monto
                    visados_detalle['electromecanica'] = exp.tasa_visado_electromecanica_monto
            
            datos.append({
                'id': exp.id,
                'fecha': exp.fecha_salida if hasattr(exp, 'fecha_salida') else exp.fecha,
                'profesional': exp.nombre_profesional if hasattr(exp, 'nombre_profesional') else exp.profesional,
                'comitente': exp.nombre_comitente if hasattr(exp, 'nombre_comitente') else exp.comitente,
                'nro_expediente_cpim': exp.nro_expediente_cpim,
                'gop_numero': exp.gop_numero,
                'gas': visados_detalle.get('gas', Decimal('0')),
                'salubridad': visados_detalle.get('salubridad', Decimal('0')),
                'electrica': visados_detalle.get('electrica', Decimal('0')),
                'electromecanica': visados_detalle.get('electromecanica', Decimal('0')),
                'total_visados': total_visados,
                'estado_pago': 'Pagado' if es_pagado else 'No pagado'
            })
        
        return datos
    
    def _crear_resumen(self, totales_por_tipo, honorarios, cant_pagados, cant_no_pagados):
        """Crea un resumen del análisis."""
        return {
            'cantidad_expedientes_pagados': cant_pagados,
            'cantidad_expedientes_no_pagados': cant_no_pagados,
            'total_expedientes': cant_pagados + cant_no_pagados,
            'totales_por_tipo': totales_por_tipo,
            'total_general': sum(totales_por_tipo.values()),
            'honorarios_imlauer': honorarios['imlauer']['para_ingeniero'],
            'honorarios_onetto': honorarios['onetto']['para_ingeniero'],
            'honorarios_cpim': honorarios['totales_generales']['total_para_cpim']
        }
    
    def crear_cierre(self, analisis_datos, nombre_cierre, usuario_cierre=None, observaciones=None):
        """
        MÉTODO TEMPORAL: Esta funcionalidad estará disponible próximamente.
        """
        raise NotImplementedError("La funcionalidad de cierres estará disponible próximamente")
    
    def obtener_cierres_anteriores(self, limite=10):
        """
        MÉTODO TEMPORAL: Retorna lista vacía hasta que se implemente completamente.
        """
        return []