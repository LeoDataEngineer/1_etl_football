import os
import pandas as pd

# Definir carpetas
carpeta_origen = "bronze"
carpeta_destino = "silver"

# Crear la carpeta de destino si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)
    
fecha_actual = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')    

# Lista de archivos CSV en la carpeta bronze
archivos_csv = [f for f in os.listdir(carpeta_origen) if f.endswith('.csv')]

# Diccionario de ligas para asignarlas correctamente
ligas = ['ESPAÑA', 'INGLATERRA', 'ITALIA', 'ALEMANIA', 'FRANCIA', 'PORTUGAL', 'HOLANDA']
liga_dict = {f"bronze_{liga.lower()}.csv": liga for liga in ligas}

# Función de transformación
def transformar_dataframe(df, nombre_archivo):
     # Limpiar nombres de equipos
    df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric() else x[4:])

    # Obtener la liga desde el nombre del archivo
    liga = liga_dict.get(nombre_archivo, "DESCONOCIDO")
    df['LIGA'] = liga
    
    
    # Seguda limpieza de nombres de equipos
    if liga == "FRANCIA":
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[1:] if x in ['MStade de Reims', 'ESaint-Etienne'] else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[8:] if x == 'Havre ACLe Havre AC' else x)
    
    elif liga == 'ESPAÑA':
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[4:] if x == 'anésLeganés' else x)
        
    elif liga == 'HOLANDA':   
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[1:] if x == 'XAjax Amsterdam' else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'AZ Alkmaar' if x == 'Z Alkmaar' else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'Go Ahead Eagles' if x == 'AheadGo Ahead Eagles' else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'Fortuna Sittard' if x == 'tunaFortuna Sittard' else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'Sparta Rotterdam' if x == 'RotterdamSparta Rotterdam' else x)
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'Almere City' if x == 'ere CityAlmere City' else x)
    
    elif liga == 'ITALIA':
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: 'Como' if x == 'OComo' else x) 
         
    elif liga == 'PORTUGAL':
        df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[1:] if x in ['GBraga', 'CGil Vicente'] else x)  
    
    # Sobrescribir la columna created_at con la fecha y hora actual
    df['CREATED_AT'] = fecha_actual

    return df

# Procesar cada archivo
for archivo in archivos_csv:
    try:
        # Ruta de origen
        ruta_origen = os.path.join(carpeta_origen, archivo)

        # Leer el CSV
        df = pd.read_csv(ruta_origen)  
        
        # Transformar el DataFrame
        df = transformar_dataframe(df, archivo)

        # Nombre del nuevo archivo en la carpeta silver
        nombre_silver = archivo.replace("bronze", "silver")
        ruta_destino = os.path.join(carpeta_destino, nombre_silver)

        # Guardar el archivo en la carpeta silver
        df.to_csv(ruta_destino, index=False, encoding='utf-8')

        print(f"Archivo procesado y guardado: {ruta_destino}")

    except Exception as e:
        print(f"Error procesando {archivo}: {e}")
