# limpiar_datos_migracion.py
# Ejecuta este script ANTES de hacer flask db upgrade

import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def limpiar_datos_expedientes():
    """Limpia datos duplicados antes de la migración."""
    
    # Cargar variables de entorno
    load_dotenv()
    
    # Obtener URL de la base de datos
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ Error: DATABASE_URL no encontrada en .env")
        return False
    
    # Normalizar URL (Railway fix)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    try:
        # Conectar a la base de datos
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Verificar cuántos registros tienen el problema
            result = conn.execute(text("""
                SELECT COUNT(*) as total, 
                       SUM(CASE WHEN nro_expediente_cpim IS NULL OR nro_expediente_cpim = '' THEN 1 ELSE 0 END) as vacios
                FROM expedientes
            """))
            
            row = result.fetchone()
            total = row[0]
            vacios = row[1]
            
            print(f"📊 Total expedientes: {total}")
            print(f"📊 Con nro_expediente_cpim vacío: {vacios}")
            
            if vacios == 0:
                print("✅ No hay registros con nro_expediente_cpim vacío. La migración debería funcionar.")
                return True
            
            # Mostrar algunos ejemplos
            print(f"\n🔍 Ejemplos de registros con problema:")
            examples = conn.execute(text("""
                SELECT id, nombre_profesional, fecha, nro_expediente_cpim
                FROM expedientes 
                WHERE nro_expediente_cpim IS NULL OR nro_expediente_cpim = ''
                LIMIT 5
            """)).fetchall()
            
            for ex in examples:
                print(f"  ID {ex[0]}: {ex[1]} - {ex[2]} - '{ex[3]}'")
            
            # Preguntar si continuar
            respuesta = input(f"\n❓ ¿Quieres asignar valores temporales a los {vacios} registros vacíos? (y/N): ").strip().lower()
            
            if respuesta in ['y', 'yes', 'sí', 'si']:
                # Actualizar registros vacíos
                result = conn.execute(text("""
                    UPDATE expedientes 
                    SET nro_expediente_cpim = 'TEMP-' || id::text 
                    WHERE nro_expediente_cpim IS NULL OR nro_expediente_cpim = ''
                """))
                
                conn.commit()
                
                print(f"✅ Actualizados {result.rowcount} registros")
                print("✅ Ahora puedes ejecutar: flask db upgrade")
                return True
            else:
                print("❌ Operación cancelada. No se puede continuar con la migración.")
                return False
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Script de limpieza de datos para migración CPIM")
    print("=" * 50)
    
    if limpiar_datos_expedientes():
        print("\n🎉 Limpieza completada exitosamente!")
        print("💡 Ahora ejecuta: flask db upgrade")
    else:
        print("\n❌ Error en la limpieza. Revisa los datos manualmente.")
        print("💡 Alternativamente, modifica la migración para no requerir unique constraint.")