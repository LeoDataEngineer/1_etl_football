from prefect import task, Flow
import subprocess  # Para ejecutar los archivos .py

# Definir tareas para ejecutar cada script ETL
@task
def run_bronze():
    subprocess.run(["python", "bronze.py"], check=True)

@task
def run_silver():
    subprocess.run(["python", "silver.py"], check=True)

@task
def run_gold():
    subprocess.run(["python", "gold.py"], check=True)

@task
def run_load():
    subprocess.run(["python", "load.py"], check=True)

# Crear el flujo de trabajo
with Flow("ETL_Flow") as flow:
    # Definir el orden de ejecución de las tareas
    run_bronze() >> run_silver() >> run_gold() >> run_load()

# Ejecutar el flujo
if __name__ == "__main__":
    flow.run()
