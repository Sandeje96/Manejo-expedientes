"""agregar tabla cierres_tasas y campos relacionados

Revision ID: 8d33cccefaff
Revises: 5c561ae1c20c
Create Date: 2025-08-21 18:00:04.173286

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8d33cccefaff'
down_revision = '5c561ae1c20c'
branch_labels = None
depends_on = None


def upgrade():
    # Crear tabla para registrar cierres de tasas
    op.create_table('cierres_tasas',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('nombre_cierre', sa.String(length=200), nullable=False),
        sa.Column('fecha_desde', sa.Date(), nullable=False),
        sa.Column('fecha_hasta', sa.Date(), nullable=False),
        sa.Column('fecha_cierre', sa.DateTime(), nullable=False),
        sa.Column('usuario_cierre', sa.String(length=100), nullable=True),
        sa.Column('total_imlauer', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('total_onetto', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('total_cpim', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('total_general', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('expedientes_incluidos', sa.Text(), nullable=True),
        sa.Column('observaciones', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Agregar campos a expedientes para marcar si están incluidos en algún cierre
    with op.batch_alter_table('expedientes', schema=None) as batch_op:
        batch_op.add_column(sa.Column('incluido_en_cierre_id', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('fecha_inclusion_cierre', sa.DateTime(), nullable=True))

def downgrade():
    # Eliminar campos agregados a expedientes
    with op.batch_alter_table('expedientes', schema=None) as batch_op:
        batch_op.drop_column('fecha_inclusion_cierre')
        batch_op.drop_column('incluido_en_cierre_id')
    
    # Eliminar tabla cierres_tasas