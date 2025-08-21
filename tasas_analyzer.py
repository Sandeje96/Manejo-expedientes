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
        from app import Expediente  # Import local para evitar circular imports
        
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
    
    def _calcular_totales_por_tipo(self, expedientes):
        """Calcula totales por cada tipo de visado."""
        totales = {
            'gas': Decimal('0'),
            'salubridad': Decimal('0'),
            'electrica': Decimal('0'),
            'electromecanica': Decimal('0')
        }
        
        for exp in expedientes:
            if exp.tasa_visado_gas_monto:
                totales['gas'] += exp.tasa_visado_gas_monto
            if exp.tasa_visado_salubridad_monto:
                totales['salubridad'] += exp.tasa_visado_salubridad_monto
            if exp.tasa_visado_electrica_monto:
                totales['electrica'] += exp.tasa_visado_electrica_monto
            if exp.tasa_visado_electromecanica_monto:
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
                'fecha': exp.fecha_salida,
                'profesional': exp.nombre_profesional,
                'comitente': exp.nombre_comitente,
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
        Crea un cierre oficial de tasas, marcando los expedientes como procesados.
        
        Args:
            analisis_datos: Datos del análisis realizado
            nombre_cierre: Nombre descriptivo del cierre
            usuario_cierre: Usuario que realiza el cierre
            observaciones: Observaciones adicionales
            
        Returns:
            CierreTasas: Objeto del cierre creado
        """
        from app import CierreTasas, Expediente  # Import local
        
        # Obtener IDs de expedientes pagados
        expedientes_ids = [exp['id'] for exp in analisis_datos['expedientes_pagados']]
        
        # Crear el registro de cierre
        cierre = CierreTasas(
            nombre_cierre=nombre_cierre,
            fecha_desde=analisis_datos['fecha_desde'],
            fecha_hasta=analisis_datos['fecha_hasta'],
            fecha_cierre=datetime.utcnow(),
            usuario_cierre=usuario_cierre,
            total_imlauer=analisis_datos['honorarios']['imlauer']['para_ingeniero'],
            total_onetto=analisis_datos['honorarios']['onetto']['para_ingeniero'],
            total_cpim=analisis_datos['honorarios']['totales_generales']['total_para_cpim'],
            total_general=analisis_datos['honorarios']['totales_generales']['total_todas_tasas'],
            expedientes_incluidos=json.dumps(expedientes_ids),
            observaciones=observaciones
        )
        
        self.db.add(cierre)
        self.db.flush()  # Para obtener el ID
        
        # Marcar expedientes como incluidos en este cierre
        Expediente.query.filter(Expediente.id.in_(expedientes_ids)).update({
            'incluido_en_cierre_id': cierre.id,
            'fecha_inclusion_cierre': datetime.utcnow()
        }, synchronize_session=False)
        
        self.db.commit()
        
        current_app.logger.info(f"Cierre de tasas creado: {nombre_cierre} (ID: {cierre.id}) con {len(expedientes_ids)} expedientes")
        
        return cierre
    
    def obtener_cierres_anteriores(self, limite=10):
        """Obtiene los cierres anteriores realizados."""
        from app import CierreTasas  # Import local
        
        return CierreTasas.query.order_by(CierreTasas.fecha_cierre.desc()).limit(limite).all()