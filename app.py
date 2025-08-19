import os
import json
import uuid
from decimal import Decimal, InvalidOperation
from datetime import datetime, date

from flask import Flask, render_template, request, redirect, url_for, flash
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

        # Contactos
        whatsapp_profesional = _db.Column(_db.String(50), nullable=True)
        whatsapp_tramitador = _db.Column(_db.String(50), nullable=True)

        # AGREGAR ESTA LÍNEA:
        # Estado del expediente
        finalizado = _db.Column(_db.Boolean, default=False, nullable=False)

        fecha_finalizado = _db.Column(_db.DateTime, nullable=True)


        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relación con archivos
        archivos = _db.relationship("Archivo", backref="expediente", cascade="all, delete-orphan")

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

        # AGREGAR ESTA NUEVA PROPIEDAD AQUÍ:
        @property
        def dias_en_bandeja(self):
            """
            Calcula los días transcurridos desde que el expediente está en la bandeja actual.
            Basado en gop_fecha_en_bandeja.
            """
            if not self.gop_fecha_en_bandeja:
                return 0
            
            from datetime import date
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
            from flask import current_app
            return current_app._limpiar_bandeja_gop(self.gop_bandeja_actual)

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

    # Valores permitidos para campos con opciones
    FORMATO_PERMITIDOS = ["Papel", "Digital"]
    ESTADOS_PAGO = ["pendiente", "pagado", "exento"]  # si no usás "exento", podés quitarlo
    PROFESIONES_PERMITIDAS = ["Ingeniero/a", "Licenciado/a", "Maestro Mayor de Obras", "Técnico/a"]

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
        return render_template("expediente_form.html", item=None, formatos=FORMATO_PERMITIDOS, profesiones=PROFESIONES_PERMITIDAS)

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
        return render_template("expediente_form.html", item=item, formatos=FORMATO_PERMITIDOS, profesiones=PROFESIONES_PERMITIDAS)
    
    @app.post("/gop/sincronizar")
    def sincronizar_gop():
        """Ejecuta el scraper GOP y actualiza expedientes."""
        try:
            from gop_integration import sync_gop_data
            stats = sync_gop_data()
            
            if 'error' in stats:
                flash(f"Error en la sincronización: {stats['error']}", "danger")
            else:
                mensaje = (f"Sincronización completada. "
                          f"Encontrados: {stats['total_gop_encontrados']} GOP, "
                          f"Actualizados: {stats['expedientes_actualizados']} expedientes")
                
                # Agregar detalles de fuentes
                if stats.get('desde_mis_bandejas', 0) > 0 or stats.get('desde_todos_tramites', 0) > 0:
                    mensaje += f" (Mis Bandejas: {stats.get('desde_mis_bandejas', 0)}, Todos: {stats.get('desde_todos_tramites', 0)})"
                
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

        for k, v in data.items():
            setattr(item, k, v)

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
            "partida_inmobiliaria": form.get("partida_inmobiliaria"),  # <— NUEVO
            "nro_expediente_municipal": form.get("nro_expediente_municipal"),  # <— NUEVO
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
    
    

    # O mejor aún, crear un filtro Jinja para usarlo directamente en las plantillas:
    @app.template_filter("limpiar_bandeja")
    def _filtro_limpiar_bandeja(bandeja_texto):
        """Filtro Jinja para limpiar texto de bandeja GOP."""
        return _limpiar_bandeja_gop(bandeja_texto)

    # === Helpers GCS ===
    def _get_gcs_client():
        """
        Obtiene un cliente de GCS.
        - Si GCS_CREDENTIALS_JSON está definido (contenido JSON), lo usa.
        - Si no, usa GOOGLE_APPLICATION_CREDENTIALS (ruta) o credenciales por defecto.
        """
        from google.cloud import storage  # import local
        creds_json = os.getenv("GCS_CREDENTIALS_JSON")
        if creds_json:
            from google.oauth2 import service_account
            info = json.loads(creds_json)
            creds = service_account.Credentials.from_service_account_info(info)
            return storage.Client(credentials=creds)
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
