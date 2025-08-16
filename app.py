import os
import json
import uuid
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import or_
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from google.cloud import storage

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

    # Engine options (seguro para Postgres; SQLite ignora connect_args)
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

        # Estados / visados
        visado_gas = _db.Column(_db.Boolean, default=False)
        visado_salubridad = _db.Column(_db.Boolean, default=False)
        visado_electrica = _db.Column(_db.Boolean, default=False)
        visado_electromecanica = _db.Column(_db.Boolean, default=False)

        estado_pago_sellado = _db.Column(_db.String(50), nullable=True)  # pendiente/pagado/exento
        estado_pago_visado = _db.Column(_db.String(50), nullable=True)

        fecha_salida = _db.Column(_db.Date, nullable=True)
        persona_retira = _db.Column(_db.String(200), nullable=True)
        nro_caja = _db.Column(_db.Integer, nullable=True)
        ruta_carpeta = _db.Column(_db.String(255), nullable=True)

        # Campos de formato Digital
        gop_numero = _db.Column(_db.String(100), nullable=True)

        whatsapp_profesional = _db.Column(_db.String(50), nullable=True)
        whatsapp_tramitador = _db.Column(_db.String(50), nullable=True)

        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        # Relación con archivos
        archivos = _db.relationship("Archivo", backref="expediente", cascade="all, delete-orphan")

        def __repr__(self):
            return f"<Expediente {self.id} - {self.nro_expediente_cpim or ''}>"

    class Archivo(_db.Model):
        __tablename__ = "archivos"
        id = _db.Column(_db.Integer, primary_key=True)
        expediente_id = _db.Column(_db.Integer, _db.ForeignKey("expedientes.id"), nullable=False)
        filename = _db.Column(_db.String(255), nullable=False)
        gcs_path = _db.Column(_db.String(512), nullable=False)  # gs://bucket/objeto
        public_url = _db.Column(_db.String(512), nullable=True)  # URL pública (si el bucket lo permite por IAM)
        mime_type = _db.Column(_db.String(100), nullable=True)
        size_bytes = _db.Column(_db.Integer, nullable=True)
        uploaded_at = _db.Column(_db.DateTime, default=datetime.utcnow)

    # Valores permitidos para campos con opciones
    FORMATO_PERMITIDOS = ["Papel", "Digital"]

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
        return render_template("expediente_form.html", item=None, formatos=FORMATO_PERMITIDOS)

    @app.post("/expedientes/nuevo")
    def crear_expediente():
        data = _parse_form(request.form)

        # Normalizar/validar formato
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

            subidos = 0
            if formato == "Digital":
                subidos = _save_pdfs_for_expediente(exp, request.files.getlist("pdfs"))
                if subidos == 0:
                    flash("No se adjuntó ningún PDF o no se pudieron subir los archivos.", "warning")

            _db.session.commit()
        except Exception as e:
            _db.session.rollback()
            flash(f"Error guardando en la base: {e}", "danger")
            return redirect(request.referrer or url_for("nuevo_expediente"))

        if formato == "Digital" and subidos > 0:
            flash(f"{subidos} archivo(s) PDF subido(s) a GCS.", "success")
        flash("Expediente creado", "success")
        return redirect(url_for("lista_expedientes"))

    @app.get("/expedientes/<int:item_id>")
    def detalle_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_detail.html", item=item)

    @app.get("/expedientes/<int:item_id>/editar")
    def editar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_form.html", item=item, formatos=FORMATO_PERMITIDOS)

    @app.post("/expedientes/<int:item_id>/editar")
    def actualizar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        data = _parse_form(request.form)

        formato = (data.get("formato") or "").strip().title()
        if formato not in FORMATO_PERMITIDOS:
            flash("Formato inválido. Debe ser Papel o Digital.", "danger")
            return redirect(request.referrer or url_for("editar_expediente", item_id=item.id))
        data["formato"] = formato

        for k, v in data.items():
            setattr(item, k, v)

        try:
            subidos = 0
            if formato == "Digital":
                subidos = _save_pdfs_for_expediente(item, request.files.getlist("pdfs"))
            _db.session.commit()
        except Exception as e:
            _db.session.rollback()
            flash(f"Error guardando en la base: {e}", "danger")
            return redirect(request.referrer or url_for("editar_expediente", item_id=item.id))

        if formato == "Digital" and subidos > 0:
            flash(f"{subidos} archivo(s) PDF subido(s) a GCS.", "success")
        flash("Expediente actualizado", "success")
        return redirect(url_for("detalle_expediente", item_id=item.id))

    @app.post("/expedientes/<int:item_id>/eliminar")
    def eliminar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        _db.session.delete(item)
        _db.session.commit()
        flash("Expediente eliminado", "info")
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

    def _parse_form(form):
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
            "visado_gas": _parse_bool(form.get("visado_gas")),
            "visado_salubridad": _parse_bool(form.get("visado_salubridad")),
            "visado_electrica": _parse_bool(form.get("visado_electrica")),
            "visado_electromecanica": _parse_bool(form.get("visado_electromecanica")),
            "estado_pago_sellado": form.get("estado_pago_sellado"),
            "estado_pago_visado": form.get("estado_pago_visado"),
            "fecha_salida": _parse_date(form.get("fecha_salida")),
            "persona_retira": form.get("persona_retira"),
            "nro_caja": _parse_int(form.get("nro_caja")),
            "ruta_carpeta": form.get("ruta_carpeta"),
            "gop_numero": form.get("gop_numero"),
            "whatsapp_profesional": form.get("whatsapp_profesional"),
            "whatsapp_tramitador": form.get("whatsapp_tramitador"),
        }

    # === Helpers GCS ===
    def _get_gcs_client():
        """
        Obtiene un cliente de GCS.
        - Si GCS_CREDENTIALS_JSON está definido (contenido JSON), lo usa.
        - Si no, usa GOOGLE_APPLICATION_CREDENTIALS (ruta) o credenciales por defecto.
        """
        creds_json = os.getenv("GCS_CREDENTIALS_JSON")
        if creds_json:
            from google.oauth2 import service_account
            info = json.loads(creds_json)
            creds = service_account.Credentials.from_service_account_info(info)
            return storage.Client(credentials=creds)
        return storage.Client()

    def _upload_pdf_to_gcs(file_storage, dest_prefix: str):
        """
        Sube un PDF a GCS. Con UBLA activado no usamos ACL por objeto.
        La URL pública se construye como:
        https://storage.googleapis.com/<bucket>/<key>
        (y funciona si el bucket otorga lectura a allUsers por IAM).
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

        # Subir el stream directamente (no usar .save en disco)
        blob.upload_from_file(file_storage.stream, content_type="application/pdf")

        # No llamar a make_public() (UBLA). Construimos URL pública basada en IAM:
        public_url = f"https://storage.googleapis.com/{bucket_name}/{key}"

        return {
            "filename": filename,
            "gcs_path": f"gs://{bucket_name}/{key}",
            "public_url": public_url,
            "size_bytes": getattr(file_storage, "content_length", None),
        }

    def _save_pdfs_for_expediente(expediente, files_list):
        """Sube PDFs a GCS y crea filas Archivo. Devuelve cuántos subió."""
        if not files_list:
            return 0
        count = 0
        for f in files_list:
            try:
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
            except Exception as e:
                # No frenamos toda la operación por un archivo; avisamos
                flash(f"Error subiendo '{getattr(f, 'filename', 'pdf')}' a GCS: {e}", "danger")
        return count

    return app
