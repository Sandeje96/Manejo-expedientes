# crear_usuario_admin.py
# Crea el usuario administrador directamente en la base de datos

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

def crear_usuario_admin():
    """Crea el usuario administrador por defecto."""
    
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
            # Verificar si existe la tabla usuarios
            try:
                result = conn.execute(text("SELECT 1 FROM usuarios LIMIT 1"))
                print("✅ Tabla 'usuarios' existe")
            except:
                print("❌ Error: La tabla 'usuarios' no existe")
                print("💡 Primero ejecuta: flask db upgrade")
                return False
            
            # Verificar si ya existe el usuario admin
            existing = conn.execute(text("""
                SELECT id, username FROM usuarios 
                WHERE username = 'admin' OR email = 'admin@cpim.com'
            """)).fetchone()
            
            if existing:
                print(f"⚠️  Ya existe un usuario: ID {existing[0]}, Username: {existing[1]}")
                respuesta = input("❓ ¿Quieres actualizar la contraseña del usuario admin existente? (y/N): ").strip().lower()
                
                if respuesta not in ['y', 'yes', 'sí', 'si']:
                    print("❌ Operación cancelada")
                    return False
                
                # Actualizar contraseña existente
                password_hash = generate_password_hash('admin123')
                conn.execute(text("""
                    UPDATE usuarios 
                    SET password_hash = :password_hash,
                        activo = true,
                        updated_at = :now
                    WHERE username = 'admin'
                """), {
                    'password_hash': password_hash,
                    'now': datetime.utcnow()
                })
                conn.commit()
                print("✅ Contraseña del usuario admin actualizada")
                
            else:
                # Crear nuevo usuario admin
                print("👤 Creando usuario administrador...")
                
                password_hash = generate_password_hash('admin123')
                now = datetime.utcnow()
                
                conn.execute(text("""
                    INSERT INTO usuarios (
                        username, email, password_hash, nombre_completo, 
                        activo, es_admin, created_at, updated_at
                    ) VALUES (
                        :username, :email, :password_hash, :nombre_completo,
                        :activo, :es_admin, :created_at, :updated_at
                    )
                """), {
                    'username': 'admin',
                    'email': 'admin@cpim.com',
                    'password_hash': password_hash,
                    'nombre_completo': 'Administrador CPIM',
                    'activo': True,
                    'es_admin': True,
                    'created_at': now,
                    'updated_at': now
                })
                
                conn.commit()
                print("✅ Usuario administrador creado exitosamente")
            
            # Verificar creación/actualización
            user = conn.execute(text("""
                SELECT username, email, nombre_completo, activo, es_admin 
                FROM usuarios WHERE username = 'admin'
            """)).fetchone()
            
            print(f"\n🎉 Usuario admin configurado:")
            print(f"   👤 Username: {user[0]}")
            print(f"   📧 Email: {user[1]}")
            print(f"   👑 Nombre: {user[2]}")
            print(f"   ✅ Activo: {user[3]}")
            print(f"   🔧 Admin: {user[4]}")
            print(f"\n🔑 Credenciales para login:")
            print(f"   Usuario: admin")
            print(f"   Contraseña: admin123")
            
            return True
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("👤 Creación de usuario administrador CPIM")
    print("=" * 50)
    
    if crear_usuario_admin():
        print("\n🎉 ¡Usuario administrador listo!")
        print("💡 Ahora puedes hacer login en: http://localhost:5000/login")
        print("🔐 Usuario: admin | Contraseña: admin123")
    else:
        print("\n❌ Error creando usuario. Revisa la configuración.")