import requests
# Configurar cliente de BigQuery
import gzip
import shutil
from datetime import datetime
from google.colab import auth
from google.colab import files # Esto solo es necesario si estás usando Google Colab
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import os
import gspread as gs
import gspread_dataframe as gd

import concurrent.futures
from google.auth import default

# URL y endpoint para producción
BASE_URL_PROD = "https://api.urbanoexpress.com.pe"
ENDPOINT_PROD = "/api/ws/e-tracking"

# Credenciales de solicitud (reemplazar con tus credenciales reales)
API_KEY = ""

# Suprimir sólo las advertencias de InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

# Autentificación
auth.authenticate_user()
creds, _ = default()
client = bigquery.Client(credentials=creds, project='bi-fcom-drmb-local-pe-sbx')

# Consulta para obtener los números de guía
query = """ select distinct tracking from(
SELECT
  DISTINCT tracking_num as tracking
FROM
  ``
WHERE
  creation_dt >= '2024-04-01'
  AND tracking_num LIKE '%WYB%'
)
"""
numeros_de_guia = client.query(query).to_dataframe()['tracking'].tolist()

# Función para obtener información de seguimiento
def obtener_informacion_tracking(api_key, guia):
    url = f"https://api.urbanoexpress.com.pe/api/ws/e-tracking"
    headers = {
        'x-api-key': api_key,
        'Content-Type': 'application/json'
    }
    data = {
        "guia_ue": guia,
        "tracking_number": ""
    }
    try:
        response = requests.post(url, headers=headers, json=data, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
       # print(f"Error en la solicitud para {guia}: {e}")
        return None

# Función para convertir datos a un DataFrame de pandas
def convertir_a_dataframe(informacion):
    if informacion is None:
        return pd.DataFrame()

    # Crear una lista para almacenar las filas de datos
    rows = []

    # Extraer información principal
    info_principal = {
        #"error_code": informacion.get("error_code", ""),
        #"error_mensaje": informacion.get("error_mensaje", ""),
        #"carrier": informacion.get("carrier", ""),
        #"zone_time": informacion.get("zone_time", ""),
        "guia_ue": informacion.get("guia_ue", ""),
        "tracking_number": informacion.get("tracking_number", ""),
        #"url_tracking": informacion.get("url_tracking", ""),

        "servicio_contrato": informacion.get("servicio", {}).get("contrato", ""),
        "servicio_origen": informacion.get("servicio", {}).get("origen", ""),
        "servicio_admitido": informacion.get("servicio", {}).get("admitido", ""),

        "cliente_codigo": informacion.get("cliente", {}).get("codigo", ""),
        "cliente_nombre": informacion.get("cliente", {}).get("nombre", ""),
        "cliente_telefono": informacion.get("cliente", {}).get("telefono", ""),

        "direccion_entrega": informacion.get("direccion_entrega", {}).get("direccion", ""),
        "direccion_entrega_referencia": informacion.get("direccion_entrega", {}).get("referencia", ""),
        "direccion_entrega_ciudad": informacion.get("direccion_entrega", {}).get("ciudad", ""),
        "direccion_entrega_zona": informacion.get("direccion_entrega", {}).get("zona", ""),
        "direccion_entrega_agencia": informacion.get("direccion_entrega", {}).get("agencia", ""),

        #"estado_actual_codigo": informacion.get("estado_actual", {}).get("codigo", ""),
        "estado_actual_estado": informacion.get("estado_actual", {}).get("estado", ""),
        #"estado_actual_sub_estado": informacion.get("estado_actual", {}).get("sub_estado", ""),
        "estado_actual_Detalle_Estado": informacion.get("estado_actual", {}).get("Detalle_Estado", ""),
        "estado_actual_fecha": informacion.get("estado_actual", {}).get("fecha", "") ,
         "estado_actual_hora":informacion.get("estado_actual", {}).get("hora", "")

        #"datos_envio_contenido": informacion.get("datos_envio", {}).get("contenido", ""),
        #"datos_envio_piezas": informacion.get("datos_envio", {}).get("piezas", ""),
        #"datos_envio_peso": informacion.get("datos_envio", {}).get("peso", ""),
        #"datos_envio_peso_volumen": informacion.get("datos_envio", {}).get("peso_volumen", ""),

        #"remitente_codigo": informacion.get("remitente", {}).get("codigo", ""),
        #"remitente_seller": informacion.get("remitente", {}).get("seller", ""),
        #"remitente_direccion": informacion.get("remitente", {}).get("direccion", ""),
        #"remitente_ciudad": informacion.get("remitente", {}).get("ciudad", ""),

        #"datos_cod_service": informacion.get("datos_cod", {}).get("service", ""),
        #"datos_cod_montos": informacion.get("datos_cod", {}).get("montos", ""),
        #"datos_cod_estado": informacion.get("datos_cod", {}).get("estado", ""),
        #"datos_cod_fecha": informacion.get("datos_cod", {}).get("fecha", ""),

        #"agencia_retiro_agencia": informacion.get("agencia_retiro", {}).get("agencia", ""),
        #"agencia_retiro_horario": informacion.get("agencia_retiro", {}).get("horario", ""),
        #"agencia_retiro_direccion": informacion.get("agencia_retiro", {}).get("direccion", ""),
        #"agencia_retiro_ciudad": informacion.get("agencia_retiro", {}).get("ciudad", ""),
        #"agencia_retiro_latitud": informacion.get("agencia_retiro", {}).get("latitud", ""),
        #"agencia_retiro_longitud": informacion.get("agencia_retiro", {}).get("longitud", "")
    }

    # Extraer información de movimientos
    movimientos = informacion.get("movimiento", [])
    for movimiento in movimientos:
        row = info_principal.copy()  # Copiar información principal
        row.update({
            "movimiento_secuencia": movimiento.get("secuencia", ""),
            "movimiento_codigo": movimiento.get("codigo", ""),
            "movimiento_estado": movimiento.get("estado", ""),
            "movimiento_sub_estado": movimiento.get("sub_estado", ""),
            "movimiento_detalle_estado": movimiento.get("detalle_estado", ""),
            "movimiento_fecha": movimiento.get("fecha", "") + " " + movimiento.get("hora", ""),
            "movimiento_apuntes": movimiento.get("apuntes", ""),
            #"movimiento_agencia": movimiento.get("agencia", ""),
            "movimiento_dir_agencia": movimiento.get("dir_agencia", ""),
            "movimiento_n_visita": movimiento.get("n_visita", "")
        })
        rows.append(row)  # Agregar la fila a la lista

    # Convertir la lista de filas a un DataFrame de pandas
    df = pd.DataFrame(rows)
    # Especificar el formato de las fechas y horas
    date_format = "%Y-%m-%d %H:%M:%S"

    # Convertir la columna de fecha y hora a datetime
    if not df.empty:
        # Especificar el formato de fecha para 'movimiento_fecha'
        df['movimiento_fecha'] = pd.to_datetime(df['movimiento_fecha'], format='%Y-%m-%d', errors='coerce')

        # Combinar 'estado_actual_fecha' y 'estado_actual_hora' y especificar el formato de fecha y hora
        df['estado_actual_fecha_hora'] = pd.to_datetime(df['estado_actual_fecha'] + " " + df['estado_actual_hora'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Eliminar las columnas originales de fecha y hora si ya no son necesarias
        df = df.drop(columns=['estado_actual_fecha', 'estado_actual_hora'])

        # Pivotar los datos para obtener la fecha máxima por movimiento_codigo 'SS' y 'AO'
        #df = df.pivot_table(
        #    index=['guia_ue','tracking_number','servicio_contrato','servicio_origen','servicio_admitido','cliente_codigo',
        #          'cliente_nombre','cliente_telefono','direccion_entrega','direccion_entrega_referencia','direccion_entrega_ciudad',
        #          'direccion_entrega_zona','direccion_entrega_agencia','estado_actual_estado','estado_actual_Detalle_Estado'],
        #    columns='movimiento_codigo',
        #    values='movimiento_fecha',
        #    aggfunc='max'
        #      ).reset_index()

        # Renombrar las columnas pivotadas
        #df.columns.name = None  # Eliminar el nombre de la columna
        #df = df.rename(columns={'SS': 'fecha_SS', 'AO': 'fecha_AO','EN':'fecha_EN'})
    return df
# Procesamiento en paralelo usando ThreadPoolExecutor
def procesar_tracking(api_key, numeros_de_guia, max_workers=100):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_guia = {executor.submit(obtener_informacion_tracking, api_key, guia): guia for guia in numeros_de_guia}
        resultados = []
        for future in concurrent.futures.as_completed(future_to_guia):
            guia = future_to_guia[future]
            try:
                resultado = future.result()
                if resultado:  # Solo agregar resultados válidos
                    resultados.append(resultado)
            except Exception as e:
                print(f"Error al procesar la información de tracking para {guia}: {e}")
    return resultados

resultados = procesar_tracking(API_KEY, numeros_de_guia, max_workers=100)

# Convertir resultados a DataFrame
df_total = pd.concat([convertir_a_dataframe(r) for r in resultados if r], ignore_index=True)

project_id = ''
!gcloud config set project {project_id}
# Configuración del dataset y la tabla en BigQuery
dataset_id = ''
table_id = 'URBANO_IL'

# Cliente de BigQuery
client = bigquery.Client(project=project_id)
# Cargar el DataFrame en BigQuery
df_total.to_gbq(destination_table=f'{dataset_id}.{table_id}', project_id=project_id, if_exists='replace')
