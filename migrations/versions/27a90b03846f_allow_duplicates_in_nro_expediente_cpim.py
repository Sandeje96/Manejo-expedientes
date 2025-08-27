"""Allow duplicates in nro_expediente_cpim

Revision ID: 27a90b03846f
Revises: 93cf33fea229
Create Date: 2025-08-27 13:11:54.003501

"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    # En Postgres, la restricción UNIQUE creada por SQLAlchemy suele llamarse
    # expedientes_nro_expediente_cpim_key (tal como mostró tu error).
    with op.batch_alter_table('expedientes') as batch_op:
        batch_op.drop_constraint('expedientes_nro_expediente_cpim_key', type_='unique')
        # Crear índice NO único (si no existe)
        batch_op.create_index('ix_expedientes_nro_expediente_cpim', ['nro_expediente_cpim'], unique=False)

def downgrade():
    with op.batch_alter_table('expedientes') as batch_op:
        batch_op.drop_index('ix_expedientes_nro_expediente_cpim')
        batch_op.create_unique_constraint('expedientes_nro_expediente_cpim_key', ['nro_expediente_cpim'])
