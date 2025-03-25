import os
import pandas as pd

# Definir carpetas
carpeta_origen = "silver"
carpeta_destino = "gold"

# Crear la carpeta de destino si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)


# Función para guardar un DataFrame como archivo CSV
def save_csv(df, nombre_archivo):
    try:
        # Ruta del archivo destino
        ruta_destino = os.path.join(carpeta_destino, nombre_archivo)

        # Intentar guardar el DataFrame como un archivo CSV
        df.to_csv(ruta_destino, index=False, encoding='utf-8')

        print(f"El archivo {nombre_archivo} se guardó correctamente en {ruta_destino}")

    except FileNotFoundError as e:
        print(f"Error: No se encuentra la carpeta de destino. {e}")
    except PermissionError as e:
        print(f"Error: Permiso denegado para guardar el archivo. {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
    finally:
        # Opcional: código que se ejecutará siempre, independientemente de si ocurrió un error
        print("Intento de guardar el archivo terminado.")

    
fecha_actual = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')    

# Lista de archivos CSV en la carpeta bronze
archivos_csv = [f for f in os.listdir(carpeta_origen) if f.endswith('.csv')]


# Crear lista para almacenar los DataFrames
dfs = []
# Procesar cada archivo
for archivo in archivos_csv:
        # Ruta de origen
        ruta_origen = os.path.join(carpeta_origen, archivo)
        # Leer el CSV
        df = pd.read_csv(ruta_origen)  
        # guardar los dataframes en una lista
        dfs.append(df)
        


# Concatenar todos los DataFrames en uno solo
df_equipos = pd.concat(dfs, ignore_index=True)

# Sobrescribir la columna created_at con la fecha y hora actual
df_equipos['CREATED_AT'] = fecha_actual

# Guardar el resultado en un nuevo CSV si lo necesitas
tablas_concat= "tablas_concat.csv"
save_csv(df_equipos, tablas_concat)


# Crear tabla 'liga' con IDs únicos
df_liga = pd.DataFrame({'ID_LIGA': range(1, df_equipos['LIGA'].nunique() + 1),
                        'LIGA': df_equipos['LIGA'].unique()})
# Guardar el resultado en un nuevo CSV si lo necesitas
nombre_liga= "ligas.csv"
save_csv(df_liga, nombre_liga)


# Merge para reemplazar nombres de liga con id_liga en df_equipos
df_equipos = df_equipos.merge(df_liga, on='LIGA', how='left')

# Eliminar columna 'LIGA' y 'nombre_liga' para solo dejar 'id_liga'
df_equipos.drop(columns=['LIGA'], inplace=True)

nombre_equipo= "equipos.csv"
save_csv(df_equipos, nombre_equipo)


