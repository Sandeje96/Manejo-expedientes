
# GOP Scraper (Posadas)

Automatiza el login y la extracción de la tabla **Mis Bandejas** del sistema GOP de Posadas, y exporta a CSV.
Incluye paginación básica y una opción experimental para intentar descargar PDFs desde el detalle de cada expediente.

## Requisitos
- Python 3.10+
- Google Chrome/Chromium (Playwright descarga uno si hace falta)
- En Windows podés usar **Visual Studio Code** o **Visual Studio** con Python workload.

## Instalación rápida (Windows PowerShell)
```powershell
./run.ps1
```
Este script crea un entorno virtual, instala dependencias y los navegadores de Playwright.

## Instalación manual
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

## Configuración
Crea un archivo `.env` en la raíz (podés copiar el `.env.example`):
```
USER_MUNI=tu_usuario
PASS_MUNI=tu_contrasena
DOWNLOAD_PDFS=false
HEADLESS=true
```

## Uso
```bash
# Windows (PowerShell)
python src/main.py
# o
./run.ps1  # que también ejecuta
```

El CSV se guarda en `data/expedientes_YYYYmmdd-HHMMSS.csv`. Los PDFs (si `DOWNLOAD_PDFS=true`) se guardan en `downloads/<nro_sistema>/`.

## Notas
- La paginación intenta detectar botones de "Siguiente"/paginador típico. Si tu UI difiere, avisá y ajustamos selectores.
- La descarga de PDFs es **best effort**: busca enlaces con texto "PDF" o "Descargar". Si no encuentra, sigue sin fallar.
- Respeta los Términos de Uso del sistema municipal.
