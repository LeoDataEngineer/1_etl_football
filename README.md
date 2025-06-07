# Proyecto: Pipeline de Datos de Ligas de Fútbol Europeas

## Descripción

Este proyecto implementa un pipeline de datos basado en la arquitectura Delta o Medallion para extraer, procesar y visualizar datos sobre ligas de fútbol europeas. La fuente principal de los datos es una página web de deportes, de la cual se obtiene información mediante técnicas de web scraping. El objetivo final es mostrar métricas y estadísticas de las diferentes ligas a través de un dashboard interactivo.

## Arquitectura

El pipeline sigue el enfoque de arquitectura Delta o Medallion, que consiste en tres niveles de procesamiento:

- **Bronze (Raw)**: Almacena los datos sin procesar obtenidos del web scraping.
- **Silver (Cleaned)**: Aplica limpieza y transformación de datos para estructurarlos correctamente.
- **Gold (Refined)**: Genera datos refinados listos para el análisis y la visualización en el dashboard.

Los datos procesados se almacenan en un *data warehouse* y luego se utilizan en un dashboard para la visualización de métricas clave.

![Texto alternativo](/imagen/Arq-Datos-Delta1.jpeg)


## Tecnologías Utilizadas

El proyecto se basa en las siguientes tecnologías:

- **Web Scraping**:  Pandas 
- **Procesamiento de Datos**: Pandas
- **Almacenamiento**: Instancia de Mysql en Aiven
- **Orquestación**: Prefect
- **Visualización**: Looker Studio
- **Gobernanza de Datos**: DataHub
- **Infraestructura**: AWS/EC2

![Texto alternativo](/imagen/Arq-Datos-Delta2.jpeg)
## Flujo del Pipeline

1. **Extracción de Datos**: Se realiza web scraping para obtener información de las ligas de fútbol europeas.
2. **Almacenamiento Inicial**: Los datos en bruto se almacenan en el nivel *Bronze*.
3. **Transformación y Limpieza**: Se aplican procesos de limpieza y transformación en el nivel *Silver*.
4. **Generación de Datos Refinados**: Los datos listos para el análisis se almacenan en el nivel *Gold*.
5. **Carga en Data Warehouse**: Los datos procesados se almacenan en un *data warehouse* para su consulta.
6. **Visualización**: Los datos se muestran en un dashboard interactivo con métricas y estadísticas de las ligas.


## Creación de tabla en la Base de datos.

```bash
CREATE DATABASE football;

USE football;

-- Crear la tabla equipo
CREATE TABLE equipo (
    ID_EQUIPO INT AUTO_INCREMENT PRIMARY KEY,
    EQUIPO VARCHAR(100) NOT NULL,
    J INT NOT NULL,  -- Partidos jugados
    G INT NOT NULL,  -- Partidos ganados
    E INT NOT NULL,  -- Partidos empatados
    P INT NOT NULL,  -- Partidos perdidos
    GF INT NOT NULL, -- Goles a favor
    GC INT NOT NULL, -- Goles en contra
    DIF INT NOT NULL, -- Diferencia de goles
    PTS INT NOT NULL, -- Puntos
    POS INT NOT NULL, -- POSICION
    LIGA VARCHAR(100) NOT NULL, 
    CREATED_AT DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, -- Fecha de registro
    );
```

## Instalación y Ejecución (en local o en algun servidor)

Para ejecutar el pipeline, sigue los siguientes pasos:

### Clonar el repositorio:

```bash
git clone https://github.com/LeoDataEngineer/1_etl_football.git
cd 1_etl_football
```
### Crear un entorno virtual e instalar dependencias:

```bash
python -m venv env
source env/bin/activate  # En Windows: env\Scripts\activate
pip install -r requirements.txt
```
### Configurar credenciales y variables de entorno
#### Crear un archivo config.py y agregar las credenciales y secretos para la conexión a la base de datos

```bash
USER = ""         
PASSWORD = ""  
HOST = ""    
PORT = ""        
DATABASE = ""
```

### Iniciar Prefect (orquestado)
```bash
prefect server start
```
### Ejecutar el archivo con el flujo de trabajo en otra terminal en el ambiente virtual 
```bash
source env/bin/activate
etl_prefect_flow.py
```

## Dashboard y Métricas
### El dashboard presenta visualizaciones clave sobre las ligas de fútbol, tales como:

- Posiciones de los equipos.

- Los equipos de los descenso.

- Los equipos que clasifican a diferentes competencias internaciones.

- Comparaciones de métricas entre ligas.

### Nota: El Dashboard
[Link Dashboard](https://lookerstudio.google.com){:target="_blank"}
<a href="https://lookerstudio.google.com" target="_blank">Link Dashboard</a>
