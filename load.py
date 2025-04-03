import os
import pandas as pd
from sqlalchemy import create_engine, text
from config import USER, PASSWORD, HOST, PORT, DATABASE



# Crear la conexión con SQLAlchemy
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Definir carpeta de origen
carpeta_origen = "gold"

# Cargar DataFrames
ruta_ligas = os.path.join(carpeta_origen, "tablas_concat.csv")
df_ligas = pd.read_csv(ruta_ligas)


# Convertir la columna CREATED_AT a formato datetime (si no está en ese formato)
df_ligas['CREATED_AT'] = pd.to_datetime(df_ligas['CREATED_AT'])

# Conectar a la base de datos
with engine.connect() as connection:

    # Vaciar las tablas sin eliminar su estructura
    connection.execute(text("TRUNCATE TABLE liga;"))
 
    # Insertar los datos en la tabla liga
    df_ligas.to_sql(name="liga", con=engine, if_exists="append", index=False)


print("Datos enviados correctamente en MySQL")
