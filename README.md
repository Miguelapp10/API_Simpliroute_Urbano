# Descripción del Script de Python para Extracción y Procesamiento de Datos de SimpliRoute
Este script está diseñado para extraer datos de la API de SimpliRoute en un rango de fechas específico y procesarlos para su análisis y almacenamiento en Google BigQuery. A continuación, se presenta un resumen de cada sección del script:

1. Importación de Bibliotecas y Configuración Inicial:

Se importan bibliotecas esenciales como requests, json, pandas, datetime, concurrent.futures, geopandas, shapely, y herramientas específicas para Google Colab y Google Cloud.
Se suprimen las advertencias de InsecureRequestWarning para evitar mensajes innecesarios durante las solicitudes HTTPS.
Se definen las URLs base para vehículos, rutas y visitas de SimpliRoute.
Se establecen los encabezados para las solicitudes HTTP, incluyendo el token de autenticación.
2. Generación de un Rango de Fechas:

Se define una función date_range para generar una lista de fechas entre una fecha de inicio (start_date) y la fecha actual (end_date).
3. Función para Obtener Datos en una Fecha Específica:

fetch_data_for_date toma una URL base y una fecha específica para realizar una solicitud GET a la API de SimpliRoute, devolviendo los datos en formato JSON si la respuesta es exitosa.
4. Función para Obtener Datos en Paralelo:

get_data_parallel utiliza ThreadPoolExecutor para hacer solicitudes a la API en paralelo para todas las fechas en el rango definido, recolectando y retornando todos los datos obtenidos.
5. Extracción de Datos de la API:

Se utilizan las funciones anteriores para obtener datos de vehículos, rutas y visitas en el rango de fechas especificado.
6. Transformación de Datos con Pandas:

Se convierten los datos obtenidos en DataFrames de pandas.
Se seleccionan las columnas relevantes y se renombrar algunas columnas para mayor claridad.
Se define una función map_observation_to_new_column para mapear los valores de checkout_observation a descripciones más legibles, aplicándola a los datos de visitas.
7. Unión de DataFrames:

Se realizan uniones entre los DataFrames de rutas, vehículos y visitas en base a campos comunes (vehicle, driver, route, planned_date), consolidando la información en un solo DataFrame.
8. Carga de Datos en BigQuery:

Se configura el proyecto de Google Cloud y se define el dataset y la tabla de destino en BigQuery.
Se utiliza el cliente de BigQuery para cargar el DataFrame consolidado en la tabla especificada, reemplazando cualquier dato existente.
Este script proporciona un flujo completo desde la extracción de datos de una API externa hasta su procesamiento y almacenamiento en BigQuery para análisis posterior, siendo útil para la integración de datos operativos en plataformas de análisis de datos.





