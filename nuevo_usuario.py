from datetime import datetime, timezone
from werkzeug.security import generate_password_hash
from app import create_app, _db

app = create_app()
with app.app_context():
    Usuario = _db.Model.registry._class_registry.get("Usuario")  # obtiene la clase definida dentro de create_app
    u = Usuario(
        username="Directiva",
        email="balance@cpim.org",
        password_hash=generate_password_hash("haro2745"),
        nombre_completo="Presidencia CPIM",
        activo=True,
        es_admin=False,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    _db.session.add(u)
    _db.session.commit()
    print("✅ Usuario creado vía ORM: Directiva (no admin)")