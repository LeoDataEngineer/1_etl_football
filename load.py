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

#ruta_equipos = os.path.join(carpeta_origen, "equipos.csv")
#df_equipos = pd.read_csv(ruta_equipos)

# Convertir la columna CREATED_AT a formato datetime (si no está en ese formato)
df_ligas['CREATED_AT'] = pd.to_datetime(df_ligas['CREATED_AT'])

# Conectar a la base de datos
with engine.connect() as connection:
    # 1️⃣ Deshabilitar las restricciones de clave foránea
    # connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

    # 2️⃣ Vaciar las tablas sin eliminar su estructura
    connection.execute(text("TRUNCATE TABLE liga;"))
    # connection.execute(text("TRUNCATE TABLE equipo;"))

    # 3️⃣ Insertar los datos en la tabla liga
    df_ligas.to_sql(name="liga", con=engine, if_exists="append", index=False)

    # 4️⃣ Insertar los datos en la tabla equipo
    #df_equipos.to_sql(name="equipo", con=engine, if_exists="append", index=False)

    # 5️⃣ Volver a habilitar las restricciones de clave foránea
    # connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

print("Datos enviados correctamente en MySQL")
