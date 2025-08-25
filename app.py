import os
import json
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
from flask import Flask, render_template, request, redirect, url_for, flash, current_app, send_file, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import or_
from dotenv import load_dotenv
from werkzeug.utils import secure_filename


# Extensiones globales
_db = SQLAlchemy()
_migrate = Migrate()


def _normalize_db_url(url: str) -> str:
    # Railway a veces entrega postgres:// en lugar de postgresql://
    if url and url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def create_app():
    load_dotenv()

    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

    # Tamaño máximo de subida (por defecto 20 MB). Cambiá con MAX_UPLOAD_MB en .env
    app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_MB", "20")) * 1024 * 1024

    db_url = os.getenv("DATABASE_URL", "sqlite:///cpim.db")
    db_url = _normalize_db_url(db_url)
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url

    # Engine options (si es Postgres)
    if db_url.startswith("postgresql"):
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "pool_pre_ping": True,
            "pool_recycle": 300,
            "pool_size": 5,
            "max_overflow": 0,
            "connect_args": {
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
                "sslmode": "require",
            },
        }

    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    _db.init_app(app)
    _migrate.init_app(app, _db)

    # === Filtro Jinja para formatear moneda ARS ===
    @app.template_filter("ars")
    def _fmt_ars(value):
        if value is None:
            return "-"
        try:
            # Trabaja bien con Decimal
            n = Decimal(value)
        except Exception:
            return "-"
        s = f"{n:,.2f}"                  # 1,234,567.89
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"$ {s}"

    # === Modelos ===
    class Expediente(_db.Model):
        __tablename__ = "expedientes"
        id = _db.Column(_db.Integer, primary_key=True)

        # Básicos
        fecha = _db.Column(_db.Date, nullable=True)
        profesion = _db.Column(_db.String(120), nullable=True)
        formato = _db.Column(_db.String(50), nullable=True)  # Papel o Digital
        nro_copias = _db.Column(_db.Integer, nullable=True)
        tipo_trabajo = _db.Column(_db.String(200), nullable=True)

        # Identificación / actores
        nro_expediente_cpim = _db.Column(_db.String(100), unique=True, nullable=True)
        nombre_profesional = _db.Column(_db.String(200), nullable=True)
        nombre_comitente = _db.Column(_db.String(200), nullable=True)
        ubicacion = _db.Column(_db.String(255), nullable=True)
        partida_inmobiliaria = _db.Column(_db.String(100), nullable=True)
        nro_expediente_municipal = _db.Column(_db.String(100), nullable=True)

        # (Antiguos) flags de visado — ya no se usan para cálculo, pero los dejamos por compatibilidad
        visado_gas = _db.Column(_db.Boolean, default=False)
        visado_salubridad = _db.Column(_db.Boolean, default=False)
        visado_electrica = _db.Column(_db.Boolean, default=False)
        visado_electromecanica = _db.Column(_db.Boolean, default=False)
        en_oficina_tecnica = _db.Column(_db.Boolean, default=False, nullable=True)

        # Estados de pago (siguen vigentes)
        estado_pago_sellado = _db.Column(_db.String(50), nullable=False, default="pendiente")
        estado_pago_visado  = _db.Column(_db.String(50), nullable=False, default="pendiente")

        # NUEVOS: montos de tasas (ARG) — Numeric(12,2)
        tasa_sellado_monto = _db.Column(_db.Numeric(12, 2), nullable=True)
        tasa_visado_electrica_monto = _db.Column(_db.Numeric(12, 2), nullable=True)
        tasa_visado_salubridad_monto = _db.Column(_db.Numeric(12, 2), nullable=True)
        tasa_visado_gas_monto = _db.Column(_db.Numeric(12, 2), nullable=True)
        tasa_visado_electromecanica_monto = _db.Column(_db.Numeric(12, 2), nullable=True)

        # Salida / caja / ubicación
        fecha_salida = _db.Column(_db.Date, nullable=True)
        persona_retira = _db.Column(_db.String(200), nullable=True)
        nro_caja = _db.Column(_db.Integer, nullable=True)
        ruta_carpeta = _db.Column(_db.String(255), nullable=True)

        # Campos de formato Digital
        gop_numero = _db.Column(_db.String(100), nullable=True)

        # Nuevos campos GOP (información del scraper)
        gop_bandeja_actual = _db.Column(_db.String(200), nullable=True)
        gop_usuario_asignado = _db.Column(_db.String(200), nullable=True)
        gop_estado = _db.Column(_db.String(100), nullable=True)
        gop_fecha_entrada = _db.Column(_db.Date, nullable=True)
        gop_fecha_en_bandeja = _db.Column(_db.Date, nullable=True)  # NUEVO CAMPO
        gop_ultima_sincronizacion = _db.Column(_db.DateTime, nullable=True)

        # AGREGAR ESTOS NUEVOS CAMPOS PARA BANDEJAS ESPECÍFICAS:
        # Bandeja CPIM
        bandeja_cpim_nombre = _db.Column(_db.String(200), nullable=True)
        bandeja_cpim_usuario = _db.Column(_db.String(200), nullable=True)
        bandeja_cpim_fecha = _db.Column(_db.Date, nullable=True)
        bandeja_cpim_sincronizacion = _db.Column(_db.DateTime, nullable=True)
        
        # Bandeja IMLAUER
        bandeja_imlauer_nombre = _db.Column(_db.String(200), nullable=True)
        bandeja_imlauer_usuario = _db.Column(_db.String(200), nullable=True)
        bandeja_imlauer_fecha = _db.Column(_db.Date, nullable=True)
        bandeja_imlauer_sincronizacion = _db.Column(_db.DateTime, nullable=True)
        
        # Bandeja ONETTO
        bandeja_onetto_nombre = _db.Column(_db.String(200), nullable=True)
        bandeja_onetto_usuario = _db.Column(_db.String(200), nullable=True)
        bandeja_onetto_fecha = _db.Column(_db.Date, nullable=True)
        bandeja_onetto_sincronizacion = _db.Column(_db.DateTime, nullable=True)
        
        # Bandeja PROFESIONAL
        bandeja_profesional_nombre = _db.Column(_db.String(200), nullable=True)
        bandeja_profesional_usuario = _db.Column(_db.String(200), nullable=True)
        bandeja_profesional_fecha = _db.Column(_db.Date, nullable=True)
        bandeja_profesional_sincronizacion = _db.Column(_db.DateTime, nullable=True)

        # Contactos
        whatsapp_profesional = _db.Column(_db.String(50), nullable=True)
        whatsapp_tramitador = _db.Column(_db.String(50), nullable=True)

        # Estado del expediente
        finalizado = _db.Column(_db.Boolean, default=False, nullable=False)
        en_oficina_tecnica = _db.Column(_db.Boolean, default=False, nullable=True)
        fecha_finalizado = _db.Column(_db.DateTime, nullable=True)

        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        incluido_en_cierre_id = _db.Column(_db.Integer, nullable=True)
        fecha_inclusion_cierre = _db.Column(_db.DateTime, nullable=True)

        # Relación con archivos
        archivos = _db.relationship("Archivo", backref="expediente", cascade="all, delete-orphan")
        profesionales_adicionales = _db.relationship("ProfesionalAdicional", backref="expediente", 
                                                    cascade="all, delete-orphan", 
                                                    order_by="ProfesionalAdicional.orden")
        
        @property
        def todos_los_profesionales(self):
            """
            Retorna una lista con todos los profesionales del expediente.
            El primer elemento es siempre el profesional principal.
            """
            profesionales = []
            
            # Agregar profesional principal
            if self.nombre_profesional:
                profesionales.append({
                    'nombre': self.nombre_profesional,
                    'whatsapp': self.whatsapp_profesional,
                    'es_principal': True
                })
            
            # Agregar profesionales adicionales
            for prof_adic in self.profesionales_adicionales:
                profesionales.append({
                    'nombre': prof_adic.nombre_profesional,
                    'whatsapp': prof_adic.whatsapp_profesional,
                    'es_principal': False
                })
            
            return profesionales
        
        @property
        def nombres_profesionales_concatenados(self):
            """
            Retorna todos los nombres de profesionales concatenados para mostrar en la tabla.
            """
            nombres = []
            
            # Agregar profesional principal
            if self.nombre_profesional:
                nombres.append(self.nombre_profesional)
            
            # Agregar profesionales adicionales
            for prof_adic in self.profesionales_adicionales:
                nombres.append(prof_adic.nombre_profesional)
            
            return nombres

        # Conveniencia: total de visados
        @property
        def total_visados(self):
            vals = [
                self.tasa_visado_electrica_monto,
                self.tasa_visado_salubridad_monto,
                self.tasa_visado_gas_monto,
                self.tasa_visado_electromecanica_monto,
            ]
            return sum((v or 0) for v in vals)

        @property
        def dias_en_bandeja(self):
            """
            Calcula los días transcurridos desde que el expediente está en la bandeja actual.
            Basado en gop_fecha_en_bandeja.
            """
            if not self.gop_fecha_en_bandeja:
                return 0
            
            hoy = date.today()
            delta = hoy - self.gop_fecha_en_bandeja
            return delta.days
        
        @property
        def bandeja_gop_limpia(self):
            """
            Devuelve la bandeja GOP procesada, mostrando solo la bandeja actual.
            """
            if not self.gop_bandeja_actual:
                return ""
            
            # Usar la función de limpieza que definimos arriba
            # Necesitamos acceder a la función desde el contexto de la app
            return current_app._limpiar_bandeja_gop(self.gop_bandeja_actual)
        
        @property
        def dias_en_bandeja_cpim(self):
            """Calcula días en bandeja CPIM."""
            if not self.bandeja_cpim_fecha:
                return 0
            return (date.today() - self.bandeja_cpim_fecha).days

        @property
        def dias_en_bandeja_imlauer(self):
            """Calcula días en bandeja IMLAUER."""
            if not self.bandeja_imlauer_fecha:
                return 0
            return (date.today() - self.bandeja_imlauer_fecha).days

        @property
        def dias_en_bandeja_onetto(self):
            """Calcula días en bandeja ONETTO."""
            if not self.bandeja_onetto_fecha:
                return 0
            return (date.today() - self.bandeja_onetto_fecha).days

        @property
        def dias_en_bandeja_profesional(self):
            """Calcula días en bandeja PROFESIONAL."""
            if not self.bandeja_profesional_fecha:
                return 0
            return (date.today() - self.bandeja_profesional_fecha).days
        
        def get_historial_bandejas(self):
            """
            Retorna el historial completo de bandejas ordenado por fecha.
            Versión segura que maneja el caso donde la tabla no existe.
            """
            try:
                # Verificar si la tabla existe usando una consulta directa
                from sqlalchemy import text
                result = _db.session.execute(text("SELECT 1 FROM historial_bandejas LIMIT 1"))
                result.close()  # Cerrar el resultado para evitar problemas de transacción
                
                # Si llegamos aquí, la tabla existe
                return HistorialBandeja.query.filter_by(expediente_id=self.id).order_by(
                    HistorialBandeja.fecha_inicio.desc(),
                    HistorialBandeja.created_at.desc()
                ).all()
            except Exception as e:
                # Si la tabla no existe o hay otro error, hacer rollback y retornar lista vacía
                _db.session.rollback()
                if current_app:
                    current_app.logger.warning(f"Tabla historial_bandejas no disponible: {e}")
                return []
        
        def get_dias_totales_por_bandeja(self):
            """
            Retorna un diccionario con el total de días acumulados por cada tipo de bandeja.
            Versión segura que maneja el caso donde la tabla no existe.
            
            Returns:
                dict: {'cpim': 15, 'imlauer': 8, 'onetto': 0, 'profesional': 12}
            """
            try:
                historial = self.get_historial_bandejas()
                totales = {'cpim': 0, 'imlauer': 0, 'onetto': 0, 'profesional': 0}
                
                for registro in historial:
                    if registro.dias_en_bandeja:
                        totales[registro.bandeja_tipo] += registro.dias_en_bandeja
                    elif registro.esta_activo:
                        # Si está activo, calcular días hasta hoy
                        totales[registro.bandeja_tipo] += registro.dias_calculados
                
                return totales
            except Exception as e:
                if current_app:
                    current_app.logger.warning(f"Error calculando días por bandeja: {e}")
                return {'cpim': 0, 'imlauer': 0, 'onetto': 0, 'profesional': 0}
        
        def get_total_dias_en_sistema(self):
            """
            Retorna el total de días que el expediente ha estado en el sistema.
            Versión segura.
            """
            try:
                totales = self.get_dias_totales_por_bandeja()
                return sum(totales.values())
            except Exception:
                return 0
        
        def get_bandeja_actual_historial(self):
            """
            Retorna el registro activo del historial (donde fecha_fin es NULL).
            Versión segura.
            """
            try:
                # Verificar si la tabla existe
                from sqlalchemy import text
                result = _db.session.execute(text("SELECT 1 FROM historial_bandejas LIMIT 1"))
                result.close()
                
                return HistorialBandeja.query.filter_by(
                    expediente_id=self.id,
                    fecha_fin=None
                ).first()
            except Exception:
                _db.session.rollback()
                return None
        
        def actualizar_historial_bandeja(self, bandeja_tipo, bandeja_nombre, usuario_asignado, fecha_actual=None):
            """
            Actualiza el historial cuando cambia de bandeja.
            Versión segura que maneja el caso donde la tabla no existe.
            
            Args:
                bandeja_tipo: 'cpim', 'imlauer', 'onetto', 'profesional'
                bandeja_nombre: Nombre descriptivo de la bandeja
                usuario_asignado: Usuario asignado
                fecha_actual: Fecha del cambio (por defecto hoy)
            """
            try:
                if fecha_actual is None:
                    fecha_actual = date.today()
                
                # Verificar si la tabla existe
                from sqlalchemy import text
                result = _db.session.execute(text("SELECT 1 FROM historial_bandejas LIMIT 1"))
                result.close()
                
                # Cerrar el registro activo anterior si existe
                registro_activo = self.get_bandeja_actual_historial()
                if registro_activo:
                    registro_activo.fecha_fin = fecha_actual
                    registro_activo.dias_en_bandeja = registro_activo.dias_calculados
                    _db.session.add(registro_activo)
                
                # Crear nuevo registro para la bandeja actual
                nuevo_registro = HistorialBandeja(
                    expediente_id=self.id,
                    bandeja_tipo=bandeja_tipo,
                    bandeja_nombre=bandeja_nombre,
                    usuario_asignado=usuario_asignado,
                    fecha_inicio=fecha_actual,
                    fecha_fin=None,  # Activo
                    dias_en_bandeja=None  # Se calculará cuando se cierre
                )
                
                _db.session.add(nuevo_registro)
                return nuevo_registro
                
            except Exception as e:
                _db.session.rollback()
                if current_app:
                    current_app.logger.warning(f"No se pudo actualizar historial de bandejas: {e}")
                return None

        def __repr__(self):
            return f"<Expediente {self.id} - {self.nro_expediente_cpim or ''}>"

    class Archivo(_db.Model):
        __tablename__ = "archivos"
        id = _db.Column(_db.Integer, primary_key=True)
        expediente_id = _db.Column(_db.Integer, _db.ForeignKey("expedientes.id"), nullable=False)
        filename = _db.Column(_db.String(255), nullable=False)
        gcs_path = _db.Column(_db.String(512), nullable=False)  # gs://bucket/objeto o ruta interna
        public_url = _db.Column(_db.String(512), nullable=True)  # URL pública si se habilita
        mime_type = _db.Column(_db.String(100), nullable=True)
        size_bytes = _db.Column(_db.Integer, nullable=True)
        uploaded_at = _db.Column(_db.DateTime, default=datetime.utcnow)

    class ProfesionalAdicional(_db.Model):
        """Modelo para profesionales adicionales de un expediente."""
        __tablename__ = "profesionales_adicionales"
        
        id = _db.Column(_db.Integer, primary_key=True)
        expediente_id = _db.Column(_db.Integer, _db.ForeignKey("expedientes.id", ondelete="CASCADE"), nullable=False)
        nombre_profesional = _db.Column(_db.String(200), nullable=False)
        whatsapp_profesional = _db.Column(_db.String(50), nullable=True)
        orden = _db.Column(_db.Integer, nullable=True)  # Para ordenar los profesionales
        
        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        def __repr__(self):
            return f"<ProfesionalAdicional {self.id} - {self.nombre_profesional}>"

    class HistorialBandeja(_db.Model):
        """Modelo para tracking del historial de días por bandeja de cada expediente."""
        __tablename__ = "historial_bandejas"
        
        id = _db.Column(_db.Integer, primary_key=True)
        expediente_id = _db.Column(_db.Integer, _db.ForeignKey("expedientes.id", ondelete="CASCADE"), nullable=False)
        
        # Tipo de bandeja: 'cpim', 'imlauer', 'onetto', 'profesional'
        bandeja_tipo = _db.Column(_db.String(50), nullable=False)
        
        # Datos específicos de la bandeja
        bandeja_nombre = _db.Column(_db.String(200), nullable=True)  # Nombre descriptivo de la bandeja
        usuario_asignado = _db.Column(_db.String(200), nullable=True)  # Usuario asignado en ese período
        
        # Fechas del período en la bandeja
        fecha_inicio = _db.Column(_db.Date, nullable=False)  # Cuándo entró a esta bandeja
        fecha_fin = _db.Column(_db.Date, nullable=True)  # Cuándo salió (NULL si sigue ahí)
        
        # Días calculados
        dias_en_bandeja = _db.Column(_db.Integer, nullable=True)  # Días que estuvo en esta bandeja
        
        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        # Relación con expediente
        expediente = _db.relationship("Expediente", backref=_db.backref("historial_bandejas", cascade="all, delete-orphan"))
        
        @property
        def dias_calculados(self):
            """Calcula los días en la bandeja basado en fechas."""
            if not self.fecha_inicio:
                return 0
            
            fecha_final = self.fecha_fin or date.today()
            delta = fecha_final - self.fecha_inicio
            return max(0, delta.days)
        
        @property
        def esta_activo(self):
            """Determina si este registro representa el período actual (fecha_fin es NULL)."""
            return self.fecha_fin is None
        
        def __repr__(self):
            return f"<HistorialBandeja {self.expediente_id} - {self.bandeja_tipo} ({self.fecha_inicio} a {self.fecha_fin or 'actual'})>"
        

    class CierreTasas(_db.Model):
        """Modelo para registrar cierres de tasas de visado."""
        __tablename__ = "cierres_tasas"
        
        id = _db.Column(_db.Integer, primary_key=True)
        nombre_cierre = _db.Column(_db.String(200), nullable=False)
        fecha_desde = _db.Column(_db.Date, nullable=False)
        fecha_hasta = _db.Column(_db.Date, nullable=False)
        fecha_cierre = _db.Column(_db.DateTime, nullable=False)
        usuario_cierre = _db.Column(_db.String(100), nullable=True)
        
        # Totales calculados
        total_imlauer = _db.Column(_db.Numeric(12, 2), nullable=True)
        total_onetto = _db.Column(_db.Numeric(12, 2), nullable=True)
        total_cpim = _db.Column(_db.Numeric(12, 2), nullable=True)
        total_general = _db.Column(_db.Numeric(12, 2), nullable=True)
        
        # Expedientes incluidos (JSON)
        expedientes_incluidos = _db.Column(_db.Text, nullable=True)
        observaciones = _db.Column(_db.Text, nullable=True)
        
        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
        
        @property
        def expedientes_incluidos_list(self):
            """Convierte el JSON de expedientes a lista."""
            if self.expedientes_incluidos:
                try:
                    import json
                    return json.loads(self.expedientes_incluidos)
                except:
                    return []
            return []
        
        @expedientes_incluidos_list.setter
        def expedientes_incluidos_list(self, value):
            """Convierte lista a JSON para almacenar."""
            import json
            self.expedientes_incluidos = json.dumps(value)
        
        def __repr__(self):
            return f"<CierreTasas {self.id} - {self.nombre_cierre} ({self.fecha_desde} a {self.fecha_hasta})>"

    # Valores permitidos para campos con opciones
    FORMATO_PERMITIDOS = ["Papel", "Digital"]
    ESTADOS_PAGO = ["pendiente", "pagado", "exento"]  # si no usás "exento", podés quitarlo
    PROFESIONES_PERMITIDAS = ["Ingeniero/a", "Licenciado/a", "Maestro Mayor de Obras", "Técnico/a"]
    TIPOS_TRABAJO_PERMITIDOS = ["REGISTRACION", "AMPLIACION", "OBRA NUEVA"]

    # === Rutas ===
    @app.get("/")
    def home():
        return redirect(url_for("lista_expedientes"))

    @app.get("/expedientes")
    def lista_expedientes():
        q = request.args.get("q", "").strip()
        formato_f = (request.args.get("formato", "").strip().title() or "")
        page = int(request.args.get("page", 1))
        query = Expediente.query
        if q:
            like = f"%{q}%"
            query = query.filter(
                or_(
                    Expediente.nro_expediente_cpim.ilike(like),
                    Expediente.nombre_profesional.ilike(like),
                    Expediente.nombre_comitente.ilike(like),
                    Expediente.tipo_trabajo.ilike(like),
                    Expediente.ubicacion.ilike(like),
                )
            )
        if formato_f in FORMATO_PERMITIDOS:
            query = query.filter(Expediente.formato == formato_f)
        else:
            formato_f = ""
        items = query.order_by(Expediente.created_at.desc()).paginate(page=page, per_page=20)
        return render_template("expedientes_list.html", items=items, q=q, formato=formato_f)

    @app.get("/expedientes/nuevo")
    def nuevo_expediente():
        return render_template("expediente_form.html", item=None, formatos=FORMATO_PERMITIDOS, profesiones=PROFESIONES_PERMITIDAS, tipos_trabajo=TIPOS_TRABAJO_PERMITIDOS)

    @app.post("/expedientes/nuevo")
    def crear_expediente():
        data = _parse_form(request.form)
        prof = (data.get("profesion") or "").strip()
        if prof not in PROFESIONES_PERMITIDAS:
            # si no coincide, forzamos a vacío para que el form vuelva a mostrarse
            flash("Profesión inválida. Elegí una opción del desplegable.", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))
        data["profesion"] = prof

        # Validación de formato
        formato = (data.get("formato") or "").strip().title()
        if formato not in FORMATO_PERMITIDOS:
            flash("Formato inválido. Debe ser Papel o Digital.", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))
        data["formato"] = formato

        exp = Expediente(**data)
        _db.session.add(exp)
        try:
            # Necesitamos ID para asociar archivos
            _db.session.flush()

            _save_profesionales_adicionales(exp, request.form)

            # Si es Digital, subimos PDFs (si vinieron)
            if formato == "Digital":
                _save_pdfs_for_expediente(exp, request.files.getlist("pdfs"))

            _db.session.commit()
        except Exception as e:
            _db.session.rollback()
            flash(f"Error guardando en la base: {e}", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))

        flash("Expediente creado", "success")
        return redirect(url_for("lista_expedientes"))

    @app.get("/expedientes/<int:item_id>")
    def detalle_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_detail.html", item=item)

    @app.get("/expedientes/<int:item_id>/editar")
    def editar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_form.html", item=item, formatos=FORMATO_PERMITIDOS, profesiones=PROFESIONES_PERMITIDAS, tipos_trabajo=TIPOS_TRABAJO_PERMITIDOS)
    
    @app.post("/gop/sincronizar")
    def sincronizar_gop():
        """Ejecuta el scraper GOP y actualiza expedientes con información distribuida por bandejas."""
        try:
            from gop_integration import sync_gop_data
            stats = sync_gop_data()
            
            if 'error' in stats:
                flash(f"Error en la sincronización: {stats['error']}", "danger")
            else:
                mensaje = (f"Sincronización completada. "
                          f"Encontrados: {stats['total_gop_encontrados']} GOP, "
                          f"Actualizados: {stats['expedientes_actualizados']} expedientes")
                
                # Agregar detalles de bandejas específicas
                detalles_bandejas = []
                if stats.get('bandejas_cpim', 0) > 0:
                    detalles_bandejas.append(f"CPIM: {stats['bandejas_cpim']}")
                if stats.get('bandejas_imlauer', 0) > 0:
                    detalles_bandejas.append(f"Imlauer: {stats['bandejas_imlauer']}")
                if stats.get('bandejas_onetto', 0) > 0:
                    detalles_bandejas.append(f"Onetto: {stats['bandejas_onetto']}")
                if stats.get('bandejas_profesional', 0) > 0:
                    detalles_bandejas.append(f"Profesional: {stats['bandejas_profesional']}")
                
                if detalles_bandejas:
                    mensaje += f" ({', '.join(detalles_bandejas)})"
                
                if stats['expedientes_no_encontrados'] > 0:
                    mensaje += f", No encontrados: {stats['expedientes_no_encontrados']}"
                
                if stats['errores']:
                    mensaje += f", Errores: {len(stats['errores'])}"
                    flash(mensaje, "warning")
                else:
                    flash(mensaje, "success")
            
        except Exception as e:
            flash(f"Error ejecutando sincronización: {e}", "danger")
        
        return redirect(url_for("lista_expedientes"))
    


    @app.get("/gop/estado")
    def estado_gop():
        """Muestra estadísticas de sincronización GOP."""
        # Contar expedientes con y sin datos GOP usando SQL directo
        total_con_gop = _db.session.execute(
            _db.text("SELECT COUNT(*) FROM expedientes WHERE gop_numero IS NOT NULL AND gop_numero != ''")
        ).scalar()
        
        total_sincronizados = _db.session.execute(
            _db.text("SELECT COUNT(*) FROM expedientes WHERE gop_ultima_sincronizacion IS NOT NULL")
        ).scalar()
        
        ultima_sync = _db.session.execute(
            _db.text("SELECT MAX(gop_ultima_sincronizacion) FROM expedientes")
        ).scalar()
        
        stats = {
            'total_con_gop': total_con_gop,
            'total_sincronizados': total_sincronizados,
            'ultima_sincronizacion': ultima_sync
        }
        
        return render_template("gop_estado.html", stats=stats)

    @app.post("/expedientes/<int:item_id>/editar")
    def actualizar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        data = _parse_form(request.form)
        prof = (data.get("profesion") or "").strip()
        if prof not in PROFESIONES_PERMITIDAS:
            # si no coincide, forzamos a vacío para que el form vuelva a mostrarse
            flash("Profesión inválida. Elegí una opción del desplegable.", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))
        data["profesion"] = prof

        formato = (data.get("formato") or "").strip().title()
        if formato not in FORMATO_PERMITIDOS:
            flash("Formato inválido. Debe ser Papel o Digital.", "danger")
            return redirect(request.referrer or url_for("editar_expediente", item_id=item.id))
        data["formato"] = formato
        tipo_trabajo = (data.get("tipo_trabajo") or "").strip().upper()
        if tipo_trabajo and tipo_trabajo not in TIPOS_TRABAJO_PERMITIDOS:
            flash("Tipo de trabajo inválido. Debe ser REGISTRACION, AMPLIACION u OBRA NUEVA.", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))
        data["tipo_trabajo"] = tipo_trabajo if tipo_trabajo else None

        for k, v in data.items():
            setattr(item, k, v)

        _save_profesionales_adicionales(item, request.form)

        try:
            # Si es Digital y llegan nuevos PDFs, súbelos
            if formato == "Digital":
                _save_pdfs_for_expediente(item, request.files.getlist("pdfs"))
            _db.session.commit()
        except Exception as e:
            _db.session.rollback()
            flash(f"Error guardando en la base: {e}", "danger")
            return redirect(request.referrer or url_for("editar_expediente", item_id=item.id))

        flash("Expediente actualizado", "success")
        return redirect(url_for("detalle_expediente", item_id=item.id))

    @app.post("/expedientes/<int:item_id>/eliminar")
    def eliminar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        _db.session.delete(item)
        _db.session.commit()
        flash("Expediente eliminado", "info")
        return redirect(url_for("lista_expedientes"))
    
    @app.post("/expedientes/<int:item_id>/oficina-tecnica")
    def actualizar_oficina_tecnica(item_id):
        """Actualiza el estado del checkbox 'En Oficina Técnica' de un expediente."""
        try:
            expediente = Expediente.query.get_or_404(item_id)
            
            # Obtener datos del request JSON
            data = request.get_json()
            if not data:
                return jsonify({"success": False, "error": "No se recibieron datos"}), 400
            
            # Actualizar el estado
            en_oficina_tecnica = bool(data.get("en_oficina_tecnica", False))
            expediente.en_oficina_tecnica = en_oficina_tecnica
            
            # Guardar cambios
            _db.session.add(expediente)
            _db.session.commit()
            
            # Log para auditoria
            current_app.logger.info(f"Expediente {expediente.nro_expediente_cpim or item_id} - Oficina Técnica: {'✓ Marcado' if en_oficina_tecnica else '✗ Desmarcado'}")
            
            return jsonify({
                "success": True, 
                "en_oficina_tecnica": en_oficina_tecnica,
                "mensaje": f"Expediente {'marcado como en' if en_oficina_tecnica else 'removido de'} Oficina Técnica"
            })
            
        except Exception as e:
            _db.session.rollback()
            current_app.logger.error(f"Error al actualizar oficina técnica para expediente {item_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500
    
    @app.post("/expedientes/<int:item_id>/finalizar")
    def finalizar_expediente(item_id: int):
        """Marca un expediente como finalizado y registra la fecha."""
        item = Expediente.query.get_or_404(item_id)
        item.finalizado = True
        item.fecha_finalizado = datetime.utcnow()  # Registrar fecha y hora actual
        _db.session.commit()
        flash(f"Expediente {item.nro_expediente_cpim or item.id} marcado como finalizado", "success")
        return redirect(url_for("lista_expedientes"))

    @app.post("/expedientes/<int:item_id>/reactivar")
    def reactivar_expediente(item_id: int):
        """Reactiva un expediente finalizado y limpia la fecha de finalización."""
        item = Expediente.query.get_or_404(item_id)
        item.finalizado = False
        item.fecha_finalizado = None  # Limpiar fecha de finalización
        _db.session.commit()
        flash(f"Expediente {item.nro_expediente_cpim or item.id} reactivado", "info")
        return redirect(url_for("lista_expedientes"))
    
    @app.post("/expedientes/<int:item_id>/generar-word")
    def generar_word_expediente(item_id: int):
        """Genera y descarga un documento Word con los datos del expediente."""
        from word_generator import generar_documento_expediente
        from flask import send_file
        
        try:
            # Obtener el expediente
            item = Expediente.query.get_or_404(item_id)
            
            # Generar el documento Word
            doc_stream = generar_documento_expediente(item)
            
            # Crear nombre del archivo
            nombre_archivo = f"Expediente_CPIM_{item.nro_expediente_cpim or item.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
            
            # Retornar el archivo para descarga
            return send_file(
                doc_stream,
                as_attachment=True,
                download_name=nombre_archivo,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
            
        except Exception as e:
            current_app.logger.error(f"Error generando documento Word para expediente {item_id}: {e}")
            flash(f"Error generando documento Word: {e}", "danger")
            return redirect(url_for('detalle_expediente', item_id=item_id))
        
    @app.post("/expedientes/<int:item_id>/generar-visado")
    def generar_visado_expediente(item_id: int):
        """Genera y descarga un documento Word de visado con los datos del expediente."""
        from word_generator import generar_documento_visado
        from flask import send_file
        
        try:
            # Obtener el expediente
            item = Expediente.query.get_or_404(item_id)
            
            # Generar el documento Word de visado
            doc_stream = generar_documento_visado(item)
            
            # Crear nombre del archivo
            nombre_archivo = f"Visado_CPIM_{item.nro_expediente_cpim or item.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
            
            # Retornar el archivo para descarga
            return send_file(
                doc_stream,
                as_attachment=True,
                download_name=nombre_archivo,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
            
        except Exception as e:
            current_app.logger.error(f"Error generando documento de visado para expediente {item_id}: {e}")
            flash(f"Error generando documento de visado: {e}", "danger")
            return redirect(url_for('detalle_expediente', item_id=item_id))
        
    @app.post("/expedientes/<int:item_id>/generar-adicional")
    def generar_adicional_expediente(item_id: int):
        """Genera y descarga un documento Word adicional con los datos del expediente."""
        from word_generator import generar_documento_adicional
        from flask import send_file
        
        try:
            # Obtener el expediente
            item = Expediente.query.get_or_404(item_id)
            
            # Generar el documento Word adicional
            doc_stream = generar_documento_adicional(item)
            
            # Crear nombre del archivo
            nombre_archivo = f"Adicional_CPIM_{item.nro_expediente_cpim or item.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
            
            # Retornar el archivo para descarga
            return send_file(
                doc_stream,
                as_attachment=True,
                download_name=nombre_archivo,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
            
        except Exception as e:
            current_app.logger.error(f"Error generando documento adicional para expediente {item_id}: {e}")
            flash(f"Error generando documento adicional: {e}", "danger")
            return redirect(url_for('detalle_expediente', item_id=item_id))
        
    # === RUTAS PARA ANÁLISIS DE TASAS ===
    @app.get("/analisis-tasas")
    def analisis_tasas():
        """Página principal del análisis de tasas."""
        # Importar aquí para evitar imports circulares
        from tasas_analyzer import TasasAnalyzer
        
        # Obtener cierres anteriores para mostrar
        analyzer = TasasAnalyzer(_db.session)
        cierres_anteriores = analyzer.obtener_cierres_anteriores(5)
        
        return render_template("analisis_tasas.html", cierres_anteriores=cierres_anteriores)
    
    @app.post("/analisis-tasas/ejecutar")
    def ejecutar_analisis_tasas():
        """Ejecuta el análisis de tasas según los parámetros recibidos."""
        from tasas_analyzer import TasasAnalyzer
        
        try:
            # Obtener parámetros del formulario
            fecha_desde_str = request.form.get('fecha_desde')
            fecha_hasta_str = request.form.get('fecha_hasta')
            incluir_no_pagados = request.form.get('incluir_no_pagados') == 'on'
            
            # Validar fechas
            if not fecha_desde_str or not fecha_hasta_str:
                flash('Debe seleccionar fechas de inicio y fin', 'danger')
                return redirect(url_for('analisis_tasas'))
            
            # Convertir fechas
            fecha_desde = datetime.strptime(fecha_desde_str, '%Y-%m-%d').date()
            fecha_hasta = datetime.strptime(fecha_hasta_str, '%Y-%m-%d').date()
            
            # Validar rango de fechas
            if fecha_desde > fecha_hasta:
                flash('La fecha de inicio no puede ser mayor a la fecha de fin', 'danger')
                return redirect(url_for('analisis_tasas'))
            
            # Ejecutar análisis
            analyzer = TasasAnalyzer(_db.session)
            resultado = analyzer.analizar_periodo(fecha_desde, fecha_hasta, incluir_no_pagados)
            
            # Obtener cierres anteriores
            cierres_anteriores = analyzer.obtener_cierres_anteriores(5)
            
            # Agregar datos del análisis a la sesión para usar en exportación
            from flask import session
            session['ultimo_analisis'] = {
                'fecha_desde': fecha_desde_str,
                'fecha_hasta': fecha_hasta_str,
                'incluir_no_pagados': incluir_no_pagados,
                'resultado': {
                    'fecha_desde': fecha_desde,  # Usar el objeto date, no el string
                    'fecha_hasta': fecha_hasta,  # Usar el objeto date, no el string
                    'expedientes_pagados': resultado['expedientes_pagados'],
                    'expedientes_no_pagados': resultado['expedientes_no_pagados'],
                    'totales_por_tipo': resultado['totales_por_tipo'],
                    'honorarios': resultado['honorarios'],
                    'resumen': resultado['resumen']
                }
            }
            
            return render_template("analisis_tasas.html", 
                                 resultado=resultado, 
                                 cierres_anteriores=cierres_anteriores,
                                 fecha_desde=fecha_desde_str,
                                 fecha_hasta=fecha_hasta_str,
                                 incluir_no_pagados=incluir_no_pagados)
        
        except Exception as e:
            current_app.logger.error(f"Error en análisis de tasas: {e}")
            flash(f"Error ejecutando análisis: {e}", 'danger')
            return redirect(url_for('analisis_tasas'))
    
    @app.post("/analisis-tasas/cerrar")
    def cerrar_analisis_tasas():
        """Cierra oficialmente un análisis de tasas, marcando expedientes como procesados."""
        from tasas_analyzer import TasasAnalyzer
        from flask import session
        
        try:
            # Obtener datos del análisis desde la sesión
            ultimo_analisis = session.get('ultimo_analisis')
            if not ultimo_analisis:
                flash('No hay análisis pendiente para cerrar', 'warning')
                return redirect(url_for('analisis_tasas'))
            
            # Obtener parámetros del cierre
            nombre_cierre = request.form.get('nombre_cierre', '').strip()
            observaciones = request.form.get('observaciones', '').strip()
            
            if not nombre_cierre:
                flash('Debe proporcionar un nombre para el cierre', 'danger')
                return redirect(url_for('analisis_tasas'))
            
            # Verificar que hay expedientes pagados para cerrar
            resultado = ultimo_analisis['resultado']
            if not resultado.get('expedientes_pagados'):
                flash('No hay expedientes pagados en este período para cerrar', 'warning')
                return redirect(url_for('analisis_tasas'))
            
            # Ejecutar cierre
            analyzer = TasasAnalyzer(_db.session)
            cierre = analyzer.crear_cierre(
                analisis_datos=resultado,
                nombre_cierre=nombre_cierre,
                usuario_cierre="Sistema",  # Podrías implementar autenticación aquí
                observaciones=observaciones
            )
            
            # Limpiar sesión
            session.pop('ultimo_analisis', None)
            
            flash(f'✅ Cierre "{nombre_cierre}" creado exitosamente. {len(resultado["expedientes_pagados"])} expedientes procesados y marcados como cerrados.', 'success')
            return redirect(url_for('ver_cierre_tasas', cierre_id=cierre.id))
            
        except Exception as e:
            current_app.logger.error(f"Error creando cierre: {e}")
            import traceback
            current_app.logger.error(f"Traceback: {traceback.format_exc()}")
            flash(f"Error creando cierre: {e}", 'danger')
            return redirect(url_for('analisis_tasas'))
    
    @app.get("/analisis-tasas/exportar/<formato>")
    def exportar_analisis_tasas(formato):
        """Exporta el último análisis en el formato especificado (excel o pdf)."""
        from flask import session, send_file
        from datetime import datetime, date
        import io
        
        try:
            # Obtener datos del análisis desde la sesión
            ultimo_analisis = session.get('ultimo_analisis')
            if not ultimo_analisis:
                flash('No hay análisis para exportar', 'warning')
                return redirect(url_for('analisis_tasas'))
            
            resultado = ultimo_analisis['resultado'].copy()
            
            # IMPORTANTE: Convertir las fechas de string a objetos date
            # Flask puede usar diferentes formatos dependiendo de cómo se serializan
            def parse_fecha(fecha_str):
                if not fecha_str or not isinstance(fecha_str, str):
                    return fecha_str
                
                formatos = [
                    '%Y-%m-%d',  # Formato ISO
                    '%a, %d %b %Y %H:%M:%S GMT',  # Formato GMT
                    '%Y-%m-%d %H:%M:%S',  # Formato datetime
                ]
                
                for formato in formatos:
                    try:
                        return datetime.strptime(fecha_str, formato).date()
                    except ValueError:
                        continue
                
                # Si ningún formato funciona, intentar parsearlo de otra manera
                try:
                    return datetime.fromisoformat(fecha_str.replace('T', ' ').replace('Z', '')).date()
                except:
                    return fecha_str
            
            resultado['fecha_desde'] = parse_fecha(resultado['fecha_desde'])
            resultado['fecha_hasta'] = parse_fecha(resultado['fecha_hasta'])
            
            # También convertir fechas en expedientes
            for exp in resultado.get('expedientes_pagados', []):
                if exp.get('fecha'):
                    exp['fecha'] = parse_fecha(exp['fecha'])
            
            for exp in resultado.get('expedientes_no_pagados', []):
                if exp.get('fecha'):
                    exp['fecha'] = parse_fecha(exp['fecha'])
            
            if formato.lower() == 'excel':
                return _generar_excel_tasas(resultado)
            elif formato.lower() == 'pdf':
                return _generar_pdf_tasas(resultado)
            else:
                flash('Formato de exportación no válido', 'danger')
                return redirect(url_for('analisis_tasas'))
        
        except Exception as e:
            current_app.logger.error(f"Error exportando análisis: {e}")
            import traceback
            current_app.logger.error(f"Traceback: {traceback.format_exc()}")
            flash(f"Error generando exportación: {e}", 'danger')
            return redirect(url_for('analisis_tasas'))
    
    @app.get("/analisis-tasas/cierre/<int:cierre_id>")
    def ver_cierre_tasas(cierre_id):
        """Muestra los detalles de un cierre específico."""
        cierre = CierreTasas.query.get_or_404(cierre_id)
        
        # Obtener expedientes incluidos en el cierre
        expedientes_ids = cierre.expedientes_incluidos_list
        expedientes = Expediente.query.filter(Expediente.id.in_(expedientes_ids)).all() if expedientes_ids else []
        
        return render_template("detalle_cierre_tasas.html", cierre=cierre, expedientes=expedientes)
    
    # === FUNCIONES AUXILIARES PARA EXPORTACIÓN ===
    def _generar_excel_tasas(resultado):
        """Genera archivo Excel con el análisis de tasas."""
        import pandas as pd
        from io import BytesIO
        
        # Crear archivo Excel en memoria
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Hoja 1: Expedientes Pagados
            if resultado['expedientes_pagados']:
                df_pagados = pd.DataFrame(resultado['expedientes_pagados'])
                # Formatear columnas de dinero
                for col in ['gas', 'salubridad', 'electrica', 'electromecanica', 'total_visados']:
                    if col in df_pagados.columns:
                        df_pagados[col] = df_pagados[col].astype(float)
                
                df_pagados.to_excel(writer, sheet_name='Obras Pagadas', index=False)
            
            # Hoja 2: Expedientes No Pagados
            if resultado['expedientes_no_pagados']:
                df_no_pagados = pd.DataFrame(resultado['expedientes_no_pagados'])
                # Formatear columnas de dinero
                for col in ['gas', 'salubridad', 'electrica', 'electromecanica', 'total_visados']:
                    if col in df_no_pagados.columns:
                        df_no_pagados[col] = df_no_pagados[col].astype(float)
                
                df_no_pagados.to_excel(writer, sheet_name='Obras No Pagadas', index=False)
            
            # Hoja 3: Resumen de Honorarios
            honorarios_data = []
            
            # Totales por tipo de visado
            honorarios_data.append(['TOTALES POR TIPO DE VISADO (Solo obras pagadas)', '', '', ''])
            honorarios_data.append(['Tipo de Visado', 'Total Pagado', 'Ingeniero Responsable', ''])
            honorarios_data.append(['Gas', float(resultado['totales_por_tipo']['gas']), 'IMLAUER FERNANDO', ''])
            honorarios_data.append(['Salubridad', float(resultado['totales_por_tipo']['salubridad']), 'IMLAUER FERNANDO', ''])
            honorarios_data.append(['Eléctrica', float(resultado['totales_por_tipo']['electrica']), 'ONETTO JOSÉ', ''])
            honorarios_data.append(['Electromecánica', float(resultado['totales_por_tipo']['electromecanica']), 'ONETTO JOSÉ', ''])
            honorarios_data.append(['', '', '', ''])
            
            # Cálculo de honorarios
            honorarios_data.append(['CÁLCULO DE HONORARIOS POR INGENIERO', '', '', ''])
            honorarios_data.append(['Ingeniero', 'Total Tasas', 'Para Consejo (30%)', 'Para Ingeniero (70%)'])
            
            imlauer_data = resultado['honorarios']['imlauer']
            onetto_data = resultado['honorarios']['onetto']
            
            honorarios_data.append(['IMLAUER FERNANDO', 
                                   float(imlauer_data['total_tasas']), 
                                   float(imlauer_data['para_cpim']), 
                                   float(imlauer_data['para_ingeniero'])])
            
            honorarios_data.append(['ONETTO JOSÉ', 
                                   float(onetto_data['total_tasas']), 
                                   float(onetto_data['para_cpim']), 
                                   float(onetto_data['para_ingeniero'])])
            
            honorarios_data.append(['', '', '', ''])
            
            # Totales generales
            totales_generales = resultado['honorarios']['totales_generales']
            honorarios_data.append(['TOTALES GENERALES', '', '', ''])
            honorarios_data.append(['Total de todas las tasas:', float(totales_generales['total_todas_tasas']), '', ''])
            honorarios_data.append(['Total para el Consejo (30%):', float(totales_generales['total_para_cpim']), '', ''])
            honorarios_data.append(['Total para ingenieros (70%):', float(totales_generales['total_para_ingenieros']), '', ''])
            
            df_honorarios = pd.DataFrame(honorarios_data)
            df_honorarios.to_excel(writer, sheet_name='Cálculo Honorarios', index=False, header=False)
        
        output.seek(0)
        
        # Generar nombre de archivo
        fecha_desde = resultado['fecha_desde'].strftime('%d%m%Y')
        fecha_hasta = resultado['fecha_hasta'].strftime('%d%m%Y')
        filename = f"Analisis_Tasas_{fecha_desde}_{fecha_hasta}.xlsx"
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    def _generar_pdf_tasas(resultado):
        """Genera archivo PDF con el análisis de tasas."""
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.units import inch
        from io import BytesIO
        
        # Crear PDF en memoria
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), topMargin=0.5*inch)
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#1f4e79'),
            alignment=1  # Centrado
        )
        
        subtitle_style = ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#1f4e79'),
            alignment=1
        )
        
        # Contenido del PDF
        story = []
        
        # Título
        fecha_desde = resultado['fecha_desde'].strftime('%d/%m/%Y')
        fecha_hasta = resultado['fecha_hasta'].strftime('%d/%m/%Y')
        titulo = f"ANÁLISIS DE TASAS DE VISADO - {fecha_desde} - {fecha_hasta}"
        story.append(Paragraph(titulo, title_style))
        story.append(Spacer(1, 20))
        
        # Resumen ejecutivo
        resumen = resultado['resumen']
        story.append(Paragraph("RESUMEN EJECUTIVO", subtitle_style))
        resumen_data = [
            ['Concepto', 'Cantidad/Monto'],
            ['Expedientes Pagados', str(resumen['cantidad_expedientes_pagados'])],
            ['Expedientes No Pagados', str(resumen['cantidad_expedientes_no_pagados'])],
            ['Total General de Tasas', f"$ {resumen['total_general']:,.2f}"],
            ['Honorarios Ing. Imlauer', f"$ {resumen['honorarios_imlauer']:,.2f}"],
            ['Honorarios Ing. Onetto', f"$ {resumen['honorarios_onetto']:,.2f}"],
            ['Para CPIM (30%)', f"$ {resumen['honorarios_cpim']:,.2f}"]
        ]
        
        resumen_table = Table(resumen_data, colWidths=[3*inch, 2*inch])
        resumen_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4e79')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(resumen_table)
        story.append(Spacer(1, 30))
        
        # Tabla de cálculo de honorarios
        story.append(Paragraph("CÁLCULO DE HONORARIOS POR INGENIERO", subtitle_style))
        
        honorarios_data = [
            ['Ingeniero', 'Total Tasas', 'Para Consejo (30%)', 'Para Ingeniero (70%)', 'Tipos de Visado']
        ]
        
        imlauer = resultado['honorarios']['imlauer']
        onetto = resultado['honorarios']['onetto']
        
        honorarios_data.append([
            'IMLAUER FERNANDO',
            f"$ {imlauer['total_tasas']:,.2f}",
            f"$ {imlauer['para_cpim']:,.2f}",
            f"$ {imlauer['para_ingeniero']:,.2f}",
            'Gas, Salubridad'
        ])
        
        honorarios_data.append([
            'ONETTO JOSÉ',
            f"$ {onetto['total_tasas']:,.2f}",
            f"$ {onetto['para_cpim']:,.2f}",
            f"$ {onetto['para_ingeniero']:,.2f}",
            'Eléctrica, Electromecánica'
        ])
        
        honorarios_table = Table(honorarios_data, colWidths=[2*inch, 1.5*inch, 1.5*inch, 1.5*inch, 2*inch])
        honorarios_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4e79')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(honorarios_table)
        
        # Construir PDF
        doc.build(story)
        buffer.seek(0)
        
        # Generar nombre de archivo
        fecha_desde_str = resultado['fecha_desde'].strftime('%d%m%Y')
        fecha_hasta_str = resultado['fecha_hasta'].strftime('%d%m%Y')
        filename = f"Analisis_Tasas_{fecha_desde_str}_{fecha_hasta_str}.pdf"
        
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

    # === Parsers ===
    def _parse_bool(value: str) -> bool:
        return str(value).lower() in {"1", "true", "t", "si", "sí", "on", "x"}

    def _parse_date(value: str):
        value = (value or "").strip()
        if not value:
            return None
        # intentamos YYYY-MM-DD (input type="date") y luego DD/MM/YYYY
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            try:
                return datetime.strptime(value, "%d/%m/%Y").date()
            except ValueError:
                return None

    def _parse_int(value: str):
        try:
            return int(value)
        except Exception:
            return None

    def _parse_decimal_ars(value: str):
        """
        Acepta:
          - "1.234,56"  (estándar AR)
          - "1234.56"   (punto decimal)
          - "$ 1.234,56" / espacios
        Devuelve Decimal o None.
        """
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        s = s.replace("$", "").replace(" ", "")
        # Si tiene coma como decimal, reemplazar por punto (y quitar miles)
        if "," in s and "." in s:
            # asume miles con punto y decimales con coma
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None
        
    def _parse_datetime(value: str):
        """Parse datetime string to datetime object."""
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M")
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None

    def _parse_form(form):
        def _estado_norm(k):
            v = (form.get(k) or "").strip().lower()
            return v if v in {"pendiente", "pagado", "exento"} else "pendiente"
        
        return {
            "fecha": _parse_date(form.get("fecha")),
            "profesion": form.get("profesion"),
            "formato": form.get("formato"),
            "nro_copias": _parse_int(form.get("nro_copias")),
            "tipo_trabajo": form.get("tipo_trabajo"),
            "nro_expediente_cpim": form.get("nro_expediente_cpim"),
            "nombre_profesional": form.get("nombre_profesional"),
            "nombre_comitente": form.get("nombre_comitente"),
            "ubicacion": form.get("ubicacion"),
            "partida_inmobiliaria": form.get("partida_inmobiliaria"),
            "nro_expediente_municipal": form.get("nro_expediente_municipal"),
            "visado_gas": _parse_bool(form.get("visado_gas")),
            "visado_salubridad": _parse_bool(form.get("visado_salubridad")),
            "visado_electrica": _parse_bool(form.get("visado_electrica")),
            "visado_electromecanica": _parse_bool(form.get("visado_electromecanica")),
            "estado_pago_sellado": _estado_norm("estado_pago_sellado"),
            "estado_pago_visado": _estado_norm("estado_pago_visado"),
            "fecha_salida": _parse_date(form.get("fecha_salida")),
            "persona_retira": form.get("persona_retira"),
            "nro_caja": _parse_int(form.get("nro_caja")),
            "ruta_carpeta": form.get("ruta_carpeta"),
            "gop_numero": form.get("gop_numero"),
            "whatsapp_profesional": form.get("whatsapp_profesional"),
            "whatsapp_tramitador": form.get("whatsapp_tramitador"),
            "finalizado": _parse_bool(form.get("finalizado")),
            "fecha_finalizado": _parse_datetime(form.get("fecha_finalizado")),
        }
    
    def _save_profesionales_adicionales(expediente, form):
        """Guarda los profesionales adicionales de un expediente."""
        # Obtener los datos de profesionales adicionales del formulario
        nombres = form.getlist('profesionales_adicionales_nombre[]')
        whatsapps = form.getlist('profesionales_adicionales_whatsapp[]')
        
        # Eliminar profesionales adicionales existentes
        for prof_existente in expediente.profesionales_adicionales:
            _db.session.delete(prof_existente)
        
        # Agregar nuevos profesionales adicionales
        for i, nombre in enumerate(nombres):
            nombre = nombre.strip()
            if nombre:  # Solo agregar si el nombre no está vacío
                whatsapp = whatsapps[i].strip() if i < len(whatsapps) else ""
                
                profesional_adicional = ProfesionalAdicional(
                    expediente_id=expediente.id,
                    nombre_profesional=nombre,
                    whatsapp_profesional=whatsapp if whatsapp else None,
                    orden=i + 1
                )
                _db.session.add(profesional_adicional)
    
    def _limpiar_bandeja_gop(bandeja_texto: str) -> str:
        """
        Extrae solo la bandeja actual del texto completo de bandeja GOP.
        
        Ejemplos:
        - "07 - Ampliación 7 - 178 Visado final CPIM" → "Visado final CPIM"
        - "04 - Registración 4 - 174 Visado de salubridad" → "Visado de salubridad" 
        - "03 - Obra Nueva 3 - 167 Visado final CPIM" → "Visado final CPIM"
        """
        if not bandeja_texto:
            return ""
        
        texto = str(bandeja_texto).strip()
        
        # Dividir por saltos de línea si los hay
        lineas = texto.split('\n')
        if len(lineas) > 1:
            # Si hay múltiples líneas, procesar la última
            texto = lineas[-1].strip()
        
        # Buscar el patrón: [tipo de trabajo] [número] - [número] [bandeja]
        # Queremos extraer solo la parte de [bandeja]
        import re
        
        # Patrón que busca: cualquier cosa, luego número - número texto
        # El texto después del último "número - " es lo que queremos
        matches = re.findall(r'\d+\s*-\s*(.+?)(?=\s+\d+\s*-|$)', texto)
        
        if len(matches) >= 2:
            # Si encontramos múltiples matches, el último es la bandeja
            return matches[-1].strip()
        elif len(matches) == 1:
            # Si solo hay un match, verificar si es la bandeja o el tipo de trabajo
            match = matches[0].strip()
            # Si contiene palabras típicas de bandeja, devolverlo
            palabras_bandeja = ['visado', 'firma', 'liquidación', 'registración', 'final']
            if any(palabra in match.lower() for palabra in palabras_bandeja):
                return match
        
        # Método de respaldo: buscar todo después del último guión
        partes = texto.split(' - ')
        if len(partes) >= 2:
            # Tomar la última parte
            ultima_parte = partes[-1].strip()
            # Verificar que no sea solo un número
            if not ultima_parte.isdigit():
                return ultima_parte
        
        # Si todo falla, devolver texto original
        return texto
    
    def _determinar_bandeja_por_usuario(usuario_gop: str) -> str:
        """
        Determina a qué bandeja pertenece un usuario basándose en su nombre.
        
        Returns:
            str: 'cpim', 'imlauer', 'onetto', 'profesional', o 'desconocido'
        """
        if not usuario_gop:
            return 'desconocido'
        
        usuario = str(usuario_gop).lower().strip()
        
        # Patrones para identificar cada bandeja
        if any(palabra in usuario for palabra in ['cpim', 'aguinagalde', 'gustavo', 'de jesús', 'santiago', 'javier']):
            return 'cpim'
        elif any(palabra in usuario for palabra in ['imlauer', 'fernando', 'sergio']):
            return 'imlauer'
        elif any(palabra in usuario for palabra in ['onetto']):
            return 'onetto'
        else:
            # Si no coincide con ninguno específico, va a profesional
            return 'profesional'
        
    # Registrar las funciones como métodos de la app
    app._limpiar_bandeja_gop = _limpiar_bandeja_gop
    app._determinar_bandeja_por_usuario = _determinar_bandeja_por_usuario

    # Filtro Jinja para limpiar bandeja
    @app.template_filter("limpiar_bandeja")
    def _filtro_limpiar_bandeja(bandeja_texto):
        """Filtro Jinja para limpiar texto de bandeja GOP."""
        return _limpiar_bandeja_gop(bandeja_texto)

    # === Helpers GCS ===
    def _get_gcs_client():
        """
        Obtiene un cliente de GCS.
        Busca credenciales en este orden:
        1. GCS_CREDENTIALS_JSON (contenido JSON en .env)
        2. GOOGLE_APPLICATION_CREDENTIALS (ruta en .env)
        3. Archivo local gcs-credentials.json
        4. Credenciales por defecto del sistema
        """
        from google.cloud import storage
        
        # Opción 1: JSON directo en variable de entorno
        creds_json = os.getenv("GCS_CREDENTIALS_JSON")
        if creds_json:
            from google.oauth2 import service_account
            info = json.loads(creds_json)
            creds = service_account.Credentials.from_service_account_info(info)
            return storage.Client(credentials=creds)
        
        # Opción 2: Ruta específica en variable de entorno
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            return storage.Client()
        
        # Opción 3: Buscar archivo local en la carpeta del proyecto
        local_creds = os.path.join(os.path.dirname(__file__), "gcs-credentials.json")
        if os.path.exists(local_creds):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_creds
            return storage.Client()
        
        # Opción 4: Credenciales por defecto
        return storage.Client()

    def _upload_pdf_to_gcs(file_storage, dest_prefix: str):
        """
        Sube un PDF a GCS. Para buckets con UBLA, usamos URL pública por IAM:
        https://storage.googleapis.com/<bucket>/<key>
        """
        if not file_storage or not getattr(file_storage, "filename", ""):
            return None

        filename = secure_filename(file_storage.filename)
        if not filename.lower().endswith(".pdf"):
            raise ValueError("Solo se permiten archivos .pdf")

        bucket_name = os.getenv("GCS_BUCKET_NAME")
        if not bucket_name:
            raise RuntimeError("Configura GCS_BUCKET_NAME en el entorno")

        client = _get_gcs_client()
        bucket = client.bucket(bucket_name)

        key = f"{dest_prefix}/{uuid.uuid4().hex}_{filename}"
        blob = bucket.blob(key)

        # Subir el stream directamente
        blob.upload_from_file(file_storage.stream, content_type="application/pdf")

        public_url_candidate = f"https://storage.googleapis.com/{bucket_name}/{key}"
        public_url = public_url_candidate  # con UBLA + IAM público funciona

        return {
            "filename": filename,
            "gcs_path": f"gs://{bucket_name}/{key}",
            "public_url": public_url,
            "size_bytes": getattr(file_storage, "content_length", None),
        }

    def _save_pdfs_for_expediente(expediente, files_list):
        """Sube PDFs a GCS y crea filas Archivo."""
        if not files_list:
            return 0
        count = 0
        for f in files_list:
            if not f or not getattr(f, "filename", ""):
                continue
            info = _upload_pdf_to_gcs(f, f"expedientes/{expediente.id}")
            if info:
                _db.session.add(Archivo(
                    expediente_id=expediente.id,
                    filename=info["filename"],
                    gcs_path=info["gcs_path"],
                    public_url=info.get("public_url"),
                    mime_type="application/pdf",
                    size_bytes=info.get("size_bytes"),
                ))
                count += 1
        return count

    return app