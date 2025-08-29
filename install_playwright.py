#!/usr/bin/env python3
"""
Script para instalar Playwright y sus dependencias en Railway
"""
import subprocess
import sys
import os

def main():
    print("=== Instalando Playwright para Railway ===")
    
    # Establecer variable de entorno
    os.environ['PLAYWRIGHT_BROWSERS_PATH'] = '/app/.cache/ms-playwright'
    
    try:
        # Instalar solo Chromium (más rápido y ligero)
        print("Instalando navegador Chromium...")
        result = subprocess.run([
            sys.executable, "-m", "playwright", "install", "chromium"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            return 1
        
        print("✓ Chromium instalado exitosamente")
        
        # Verificar instalación
        print("Verificando instalación...")
        result = subprocess.run([
            sys.executable, "-m", "playwright", "install", "--help"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✓ Playwright está funcionando correctamente")
        else:
            print("⚠ Advertencia: No se pudo verificar Playwright")
        
        return 0
        
    except Exception as e:
        print(f"Error durante la instalación: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())