param(
    [switch]$Run
)

# 1) Crear venv si no existe
if (!(Test-Path ".venv")) {
  python -m venv .venv
}

# 2) Activar venv
$activate = ".\.venv\Scripts\Activate.ps1"
. $activate

# 3) Instalar dependencias
pip install -r requirements.txt

# 4) Instalar navegadores de Playwright
python -m playwright install

# 5) Ejecutar el scraper
python .\src\main.py