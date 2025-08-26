# verificar_bd.py
# Ejecuta este script para verificar el estado de la base de datos

import os
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

def verificar_estado_bd():
    """Verifica el estado actual de la base de datos."""
    
    # Cargar variables de entorno
    load_dotenv()
    
    # Obtener URL de la base de datos
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ Error: DATABASE_URL no encontrada en .env")
        return
    
    # Normalizar URL (Railway fix)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    try:
        # Conectar a la base de datos
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        with engine.connect() as conn:
            print("🔗 Conexión a base de datos: ✅ EXITOSA")
            print("=" * 50)
            
            # 1. Verificar si existe la tabla usuarios
            tables = inspector.get_table_names()
            print(f"📋 Tablas existentes: {tables}")
            
            if 'usuarios' in tables:
                print("\n✅ Tabla 'usuarios' EXISTE")
                
                # Verificar cuántos usuarios hay
                result = conn.execute(text("SELECT COUNT(*) FROM usuarios")).fetchone()
                print(f"👥 Número de usuarios: {result[0]}")
                
                if result[0] > 0:
                    # Mostrar usuarios existentes
                    usuarios = conn.execute(text("""
                        SELECT id, username, email, nombre_completo, activo, es_admin 
                        FROM usuarios
                    """)).fetchall()
                    
                    print("\n👤 Usuarios existentes:")
                    for u in usuarios:
                        print(f"  - ID: {u[0]} | Username: {u[1]} | Email: {u[2]} | Activo: {u[4]} | Admin: {u[5]}")
                else:
                    print("⚠️  La tabla usuarios está VACÍA - necesitamos crear el usuario admin")
                    
            else:
                print("❌ Tabla 'usuarios' NO EXISTE")
                print("💡 Necesitas ejecutar la migración: flask db upgrade")
            
            # 2. Verificar estado de expedientes (para el otro error)
            if 'expedientes' in tables:
                print(f"\n📊 Verificando tabla 'expedientes':")
                
                # Contar total y vacíos
                result = conn.execute(text("""
                    SELECT COUNT(*) as total, 
                           SUM(CASE WHEN nro_expediente_cpim IS NULL OR nro_expediente_cpim = '' THEN 1 ELSE 0 END) as vacios
                    FROM expedientes
                """)).fetchone()
                
                print(f"  - Total expedientes: {result[0]}")
                print(f"  - Con nro_expediente_cpim vacío: {result[1]}")
                
                if result[1] > 0:
                    print("⚠️  Hay expedientes con nro_expediente_cpim vacío (esto causa el error de migración)")
            
            # 3. Verificar migraciones aplicadas
            print(f"\n📝 Verificando migraciones:")
            try:
                migrations = conn.execute(text("SELECT version_num FROM alembic_version")).fetchall()
                if migrations:
                    print(f"  - Migración actual: {migrations[0][0]}")
                else:
                    print("  - No hay migraciones aplicadas")
            except:
                print("  - Tabla alembic_version no existe")
                
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")

if __name__ == "__main__":
    print("🔍 Verificación del estado de la base de datos CPIM")
    print("=" * 50)
    verificar_estado_bd()