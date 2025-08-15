import os
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import or_
from dotenv import load_dotenv

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

    db_url = os.getenv("DATABASE_URL", "sqlite:///cpim.db")
    db_url = _normalize_db_url(db_url)
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
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
        formato = _db.Column(_db.String(50), nullable=True)
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

        whatsapp_profesional = _db.Column(_db.String(50), nullable=True)
        whatsapp_tramitador = _db.Column(_db.String(50), nullable=True)

        # Metadatos
        created_at = _db.Column(_db.DateTime, default=datetime.utcnow)
        updated_at = _db.Column(_db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

        def __repr__(self):
            return f"<Expediente {self.id} - {self.nro_expediente_cpim or ''}>"

    # === Rutas ===
    @app.get("/")
    def home():
        return redirect(url_for("lista_expedientes"))

    @app.get("/expedientes")
    def lista_expedientes():
        q = request.args.get("q", "").strip()
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
        items = query.order_by(Expediente.created_at.desc()).paginate(page=page, per_page=20)
        return render_template("expedientes_list.html", items=items, q=q)

    @app.get("/expedientes/nuevo")
    def nuevo_expediente():
        return render_template("expediente_form.html", item=None)

    @app.post("/expedientes/nuevo")
    def crear_expediente():
        data = _parse_form(request.form)
        exp = Expediente(**data)
        _db.session.add(exp)
        _db.session.commit()
        flash("Expediente creado", "success")
        return redirect(url_for("lista_expedientes"))

    @app.get("/expedientes/<int:item_id>")
    def detalle_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_detail.html", item=item)

    @app.get("/expedientes/<int:item_id>/editar")
    def editar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        return render_template("expediente_form.html", item=item)

    @app.post("/expedientes/<int:item_id>/editar")
    def actualizar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        data = _parse_form(request.form)
        for k, v in data.items():
            setattr(item, k, v)
        _db.session.commit()
        flash("Expediente actualizado", "success")
        return redirect(url_for("detalle_expediente", item_id=item.id))

    @app.post("/expedientes/<int:item_id>/eliminar")
    def eliminar_expediente(item_id: int):
        item = Expediente.query.get_or_404(item_id)
        _db.session.delete(item)
        _db.session.commit()
        flash("Expediente eliminado", "info")
        return redirect(url_for("lista_expedientes"))

    def _parse_bool(value: str) -> bool:
        return str(value).lower() in {"1", "true", "t", "si", "sí", "on", "x"}

    def _parse_date(value: str):
        value = (value or "").strip()
        if not value:
            return None
        # esperamos formato YYYY-MM-DD desde <input type="date">
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            # fallback: DD/MM/YYYY
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
            "whatsapp_profesional": form.get("whatsapp_profesional"),
            "whatsapp_tramitador": form.get("whatsapp_tramitador"),
        }

    return app