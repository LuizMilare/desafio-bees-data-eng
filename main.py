import subprocess
import sys

def run_script(script_path):
    print(f"--- Iniciando: {script_path} ---")
    result = subprocess.run([sys.executable, script_path], capture_output=False)
    if result.returncode != 0:
        print(f"Erro ao executar {script_path}")
        sys.exit(1)
    print(f"--- Finalizado com sucesso: {script_path} ---\n")

if __name__ == "__main__":
    # A ordem de execução do pipeline
    run_script("scripts/ingest_breweries.py")
    run_script("scripts/transform_silver.py")
    run_script("scripts/transform_gold.py")
    
    print("Pipeline de Dados BEES finalizado de ponta a ponta!")