# Ejecuta este script para crear la tabla manualmente
# Guarda esto como create_historial_table.py y ejecuta: python create_historial_table.py

import os
import sys
from dotenv import load_dotenv

# Agregar el directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app

def create_historial_table():
    app = create_app()
    
    with app.app_context():
        from app import _db
        
        # SQL para crear la tabla
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS historial_bandejas (
            id SERIAL PRIMARY KEY,
            expediente_id INTEGER NOT NULL,
            bandeja_tipo VARCHAR(50) NOT NULL,
            bandeja_nombre VARCHAR(200),
            usuario_asignado VARCHAR(200),
            fecha_inicio DATE NOT NULL,
            fecha_fin DATE,
            dias_en_bandeja INTEGER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (expediente_id) REFERENCES expedientes(id) ON DELETE CASCADE
        );
        """
        
        try:
            # Ejecutar la consulta
            _db.session.execute(_db.text(sql_create_table))
            _db.session.commit()
            print("✓ Tabla historial_bandejas creada exitosamente")
            
            # Verificar que la tabla existe
            result = _db.session.execute(_db.text("SELECT COUNT(*) FROM historial_bandejas"))
            count = result.scalar()
            print(f"✓ Tabla verificada, contiene {count} registros")
            
        except Exception as e:
            print(f"✗ Error creando tabla: {e}")
            _db.session.rollback()

if __name__ == "__main__":
    create_historial_table()