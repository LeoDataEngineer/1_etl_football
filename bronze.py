import os
import pandas as pd

# Listas de URLs y nombres de ligas
url = ['https://www.espn.com.co/futbol/posiciones/_/liga/esp.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/eng.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/ita.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/ger.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/fra.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/por.1',
       'https://www.espn.com.co/futbol/posiciones/_/liga/ned.1']

ligas = ['ESPAÑA', 'INGLATERRA', 'ITALIA', 'ALEMANIA', 'FRANCIA', 'PORTUGAL', 'HOLANDA']

# Crear un DataFrame con las ligas y URLs
df_ligas = pd.DataFrame({'LIGA': ligas, 'URL': url})

# Definir la carpeta donde se guardarán los archivos
carpeta_destino = "bronze"

# Crear la carpeta si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)

# Obtener la fecha actual
fecha_actual = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')

# Iterar sobre cada liga y su respectiva URL
for i, row in df_ligas.iterrows():
    try:
        # Leer las tablas de la URL
        tables = pd.read_html(row['URL'])
        
        # Concatenar las dos primeras tablas si existen
        if len(tables) >= 2:
            df = pd.concat([tables[0], tables[1]], ignore_index=True, axis=1)
            df= df.rename(columns={0:'EQUIPO',1:'J', 2:'G', 3:'E', 4:'P', 5:'GF', 6:'GC', 7:'DIF', 8:'PTS'})
        else:
            df = tables[0]  # Si solo hay una tabla, usarla directamente
        
        
        # Agregar la columna de fecha y hora
        df['CREATED_AT'] = fecha_actual
        # Crear el nombre del archivo en formato bronze_<liga>.csv
        nombre_csv = f"bronze_{row['LIGA'].lower()}.csv"
        
        # Construir la ruta completa del archivo
        ruta_completa = os.path.join(carpeta_destino, nombre_csv)

        # Guardar el DataFrame en un CSV
        df.to_csv(ruta_completa, index=False, encoding='utf-8')

        print(f"Archivo guardado: {ruta_completa}")
    
    except Exception as e:
        print(f"Error con la liga {row['LIGA']}: {e}")
