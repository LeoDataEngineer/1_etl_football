from prefect import task, flow
import subprocess

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
@flow(name="ETL_Flow")
def etl_flow():
    run_bronze() >> run_silver() >> run_gold() >> run_load()

# Ejecutar el flujo
if __name__ == "__main__":
    etl_flow()

