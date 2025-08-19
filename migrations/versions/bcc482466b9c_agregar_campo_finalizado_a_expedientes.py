"""agregar campo finalizado a expedientes

Revision ID: bcc482466b9c
Revises: 2e79b8d3e248
Create Date: [fecha actual]

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'bcc482466b9c'
down_revision = '2e79b8d3e248'
branch_labels = None
depends_on = None

def upgrade():
    # Primero agregamos la columna como nullable
    with op.batch_alter_table('expedientes', schema=None) as batch_op:
        batch_op.add_column(sa.Column('finalizado', sa.Boolean(), nullable=True))
    
    # Luego actualizamos todos los valores NULL a False
    op.execute("UPDATE expedientes SET finalizado = false WHERE finalizado IS NULL")
    
    # Finalmente cambiamos la columna a NOT NULL
    with op.batch_alter_table('expedientes', schema=None) as batch_op:
        batch_op.alter_column('finalizado', nullable=False)

def downgrade():
    with op.batch_alter_table('expedientes', schema=None) as batch_op:
        batch_op.drop_column('finalizado')
