import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Cargar app y modelos
from app import create_app
from flask import current_app
from flask_sqlalchemy import SQLAlchemy

# Columnas del Excel -> campos del modelo Expediente
COLUMN_MAP = {
    # Básicos
    "Fecha": "fecha",
    "Profesión": "profesion",
    "Formato": "formato",
    "Nro de Copias": "nro_copias",
    "Tipo de trabajo": "tipo_trabajo",
    # Identificación / actores
    "Nro de expediente CPIM": "nro_expediente_cpim",
    "Nombre del Profesional": "nombre_profesional",
    "Nombre del Comitente": "nombre_comitente",
    "Ubicación": "ubicacion",
    # Visados (ajusta según tu Excel)
    "Visado de instalacion de Gas": "visado_gas",
    "Visado de instalacion de Salubridad": "visado_salubridad",
    "Visado de instalacion electrica": "visado_electrica",
    "Visado de instalacion electromecanica": "visado_electromecanica",
    # Estados / otros
    "Estado pago sellado": "estado_pago_sellado",
    "Estado pago visado": "estado_pago_visado",
    "Fecha de salida": "fecha_salida",
    "Persona que retira": "persona_retira",
    "Nro de Caja": "nro_caja",
    "Ruta de carpeta": "ruta_carpeta",
    "WhatsApp Profesional": "whatsapp_profesional",
    "WhatsApp Tramitador": "whatsapp_tramitador",
}

BOOLEAN_COLS = {"visado_gas", "visado_salubridad", "visado_electrica", "visado_electromecanica"}
INT_COLS = {"nro_copias", "nro_caja"}
DATE_COLS = {"fecha", "fecha_salida"}


def to_bool(val):
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in {"1", "true", "t", "si", "sí", "x", "yes", "y"}


def to_int(val):
    try:
        return int(val)
    except Exception:
        return None


def to_date(val):
    if pd.isna(val) or val == "":
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.date()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(val), fmt).date()
        except ValueError:
            continue
    return None


def main(xlsx_path: str):
    app = create_app()
    with app.app_context():
        db: SQLAlchemy = app.extensions['sqlalchemy'].db
        Expediente = db.Model._decl_class_registry.get('Expediente')
        if Expediente is None:
            # Para SQLAlchemy 2.x, acceso distinto:
            from flask_sqlalchemy.model import DefaultMeta
            for cls in DefaultMeta._decl_class_registry.values():
                if getattr(cls, '__tablename__', None) == 'expedientes':
                    Expediente = cls
                    break
        if Expediente is None:
            raise RuntimeError("No se encontró el modelo Expediente. Ejecuta migraciones primero.")

        df = pd.read_excel(xlsx_path, sheet_name=0)
        if df.empty:
            print("El Excel no tiene filas (solo encabezados). Nada para importar.")
            return

        df = df.rename(columns={c: c.strip() for c in df.columns})

        records = []
        for _, row in df.iterrows():
            data = {}
            for src, dst in COLUMN_MAP.items():
                if src not in df.columns:
                    continue
                val = row[src]
                if dst in BOOLEAN_COLS:
                    data[dst] = to_bool(val)
                elif dst in INT_COLS:
                    data[dst] = to_int(val)
                elif dst in DATE_COLS:
                    data[dst] = to_date(val)
                else:
                    data[dst] = None if pd.isna(val) else str(val)
            records.append(data)

        # Insertar en DB (evitando duplicados por nro_expediente_cpim)
        created = 0
        for rec in records:
            key = rec.get("nro_expediente_cpim")
            exists = None
            if key:
                exists = db.session.execute(db.select(Expediente).filter_by(nro_expediente_cpim=key)).scalar_one_or_none()
            if exists:
                continue
            obj = Expediente(**rec)
            db.session.add(obj)
            created += 1
        db.session.commit()
        print(f"Importación completada. Registros creados: {created}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python import_from_excel.py /ruta/a/registros.xlsx")
        raise SystemExit(1)
    main(sys.argv[1])