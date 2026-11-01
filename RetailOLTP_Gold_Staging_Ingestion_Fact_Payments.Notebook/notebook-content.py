# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "15094689-301b-4cc4-b32a-9fe54b60893a",
# META       "default_lakehouse_name": "RetailOLTP_Project_Bronze_Silver",
# META       "default_lakehouse_workspace_id": "033013a2-b416-4b09-96e7-2f911dbac52c",
# META       "known_lakehouses": [
# META         {
# META           "id": "15094689-301b-4cc4-b32a-9fe54b60893a"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "8733e6ba-5c24-984e-44b8-11333fb044f5",
# META       "known_warehouses": [
# META         {
# META           "id": "8733e6ba-5c24-984e-44b8-11333fb044f5",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# PARÁMETROS DEL PIPELINE

MODO_CARGA = "RANGO"   # RANGO o FULL
FECHA_INICIO = ""      # Formato YYYY-MM-DD; vacío = automático
FECHA_FIN = ""         # Formato YYYY-MM-DD; vacío = automático

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuración general

import pyspark.sql.functions as F
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4
from com.microsoft.spark.fabric.Constants import Constants

ESQUEMA_SILVER = 'RetailOLTP_Project_Bronze_Silver.Silver_Layer'
ESQUEMA_STAGING = 'RetailOLTP_Project_Bronze_Silver.Staging_Layer'
WAREHOUSE_GOLD = "RetailOLTP_Project_Gold"
ESQUEMA_STAGING_WAREHOUSE = "Staging_Layer"
ESQUEMA_GOLD = "Gold_Layer"

FACT_PAYMENTS_STAGING_GOLD = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.fact_payments_staging"
FACT_PAYMENTS_GOLD = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.fact_payments"

BATCH_ID = str(uuid4())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validar parámetros y determinar el rango temporal

modo = (MODO_CARGA or "RANGO").strip().upper()
inicio_param = (FECHA_INICIO or "").strip()
fin_param = (FECHA_FIN or "").strip()

if modo not in ("RANGO", "FULL"):
    raise ValueError("MODO_CARGA debe ser RANGO o FULL.")

if modo == "RANGO":

    if not inicio_param and not fin_param:
        # Últimos 30 días calendario, incluyendo hoy.
        hoy = datetime.now(
            ZoneInfo("America/Lima")
        ).date()

        fecha_fin = hoy + timedelta(days=1)
        fecha_inicio = fecha_fin - timedelta(days=30)

    elif inicio_param and fin_param:
        try:
            fecha_inicio = datetime.strptime(
                inicio_param, "%Y-%m-%d"
            ).date()

            fecha_fin = datetime.strptime(
                fin_param, "%Y-%m-%d"
            ).date()

        except ValueError as e:
            raise ValueError(
                "Las fechas deben tener formato YYYY-MM-DD."
            ) from e

    else:
        raise ValueError(
            "Indica ambas fechas o deja ambas vacías "
            "para utilizar los últimos 30 días."
        )

    if fecha_inicio >= fecha_fin:
        raise ValueError(
            "FECHA_INICIO debe ser anterior a FECHA_FIN."
        )

    print(f"Modo de carga: {modo}")
    print(f"Inicio inclusivo: {fecha_inicio}")
    print(f"Fin exclusivo: {fecha_fin}")

else:
    fecha_inicio = None
    fecha_fin = None

    print("Modo de carga: FULL")
    print("Se procesarán todas las órdenes de Bronze.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lectura de tablas en la capa silver
df_payments = spark.table(ESQUEMA_SILVER + '.payments_silver')
df_orders = spark.table(ESQUEMA_SILVER + '.orders_silver').select(['OrderID', 'CustomerID', 'StoreID'])
df_customers = spark.table(ESQUEMA_SILVER + '.customers_silver').select(['CustomerID'])
df_stores = spark.table(ESQUEMA_SILVER + '.stores_silver').select(['StoreID'])

df_payments = df_payments.withColumn(
    "PaymentDate",
    F.to_timestamp(F.col("PaymentDate"))
)

if modo == "RANGO":
    df_payments = df_payments.filter(
        (F.col("PaymentDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("PaymentDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

df_orders = df_orders.join(
    df_payments,
    on = "OrderID",
    how = "left_semi"
)

df_customers = df_customers.join(
    df_orders,
    on = "CustomerID",
    how = "left_semi"
)

df_stores = df_stores.join(
    df_orders,
    on = "StoreID",
    how = "left_semi"
)

cantidad_seleccionada_payments = df_payments.count()

print(f"Retornos seleccionados: {cantidad_seleccionada_payments}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_payments.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creación del hecho Payments (fact_payments)

# Lista de columnas a seleccionar
columnas_payments = [
    'PaymentID',
    'CustomerID',
    'StoreID',
    'OrderID',
    'PaymentDate',
    'PaymentMethod',
    'PaymentStatus',
    'Amount'
]

columnas_payments_ordernados = [
    'PaymentID',
    'CustomerID',
    'StoreID',
    'OrderID',
    'PaymentDate',
    'PaymentHour',
    'PaymentMethod',
    'PaymentStatus',
    'Amount'
]

# Broadcast de tablas para optimizar el join
df_stores_b = F.broadcast(df_stores)

# Join entre tablas para obtener CustomerID y StoreID
df_fact_payments_staging = (
    df_payments
    .join(
        df_orders,
        on='OrderID',
        how='left'
    )
    .join(
        df_customers,
        on='CustomerID',
        how='left'
    )
    .join(
        df_stores_b,
        on='StoreID',
        how='left'
    )
    .select(columnas_payments)
)

# Dividir fecha y hora en dos columnas
df_fact_payments_staging = df_fact_payments_staging.withColumns({
    "PaymentHour": F.date_format(F.col("PaymentDate"), "HH:mm:ss"),
    "PaymentDate": F.to_date(F.col('PaymentDate'))
}).select(columnas_payments_ordernados)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agregar columnas de auditoria

fecha_procesamiento = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

df_fact_payments_staging = (
    df_fact_payments_staging 
    .withColumn("CreatedAt", F.lit(fecha_procesamiento).cast("timestamp"))
    .withColumn("UpdatedAt", F.lit(fecha_procesamiento).cast("timestamp"))
    .withColumn("BatchID",   F.lit(BATCH_ID))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escribir tabla en la capa STAGING del Warehouse

(
    df_fact_payments_staging.write
      .mode("overwrite")
      .synapsesql(FACT_PAYMENTS_STAGING_GOLD)
)
print(f'Se escribió la tabla {FACT_PAYMENTS_STAGING_GOLD}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear la capa Gold si no existe

# Validar existencia en Warehouse

nombre_tabla = FACT_PAYMENTS_GOLD.split(".")[-1]

query = f"""
    SELECT CASE
        WHEN EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{ESQUEMA_GOLD}'
              AND TABLE_NAME = '{nombre_tabla}'
              AND TABLE_TYPE = 'BASE TABLE'
        )
        THEN 1
        ELSE 0
    END AS existe
"""

resultado = (
    spark.read
    .option(Constants.DatabaseName, WAREHOUSE_GOLD)
    .synapsesql(query)
    .first()
)

# Crear la tabla si no existe

try:
    if resultado["existe"] == 1:
        print(f"La tabla ya existe. Se omite: {FACT_PAYMENTS_GOLD}")

    else:
        # Obtener el esquema de staging sin registros
        df_vacio = spark.table(FACT_PAYMENTS_STAGING_GOLD).limit(0)

        # Crear la tabla vacía en el Warehouse
        (
            df_vacio.write
            .mode("errorifexists")
            .synapsesql(FACT_PAYMENTS_GOLD)
        )

        print(f"Tabla creada correctamente: {FACT_PAYMENTS_GOLD}")

except Exception as e:
    print(f"Error procesando {FACT_PAYMENTS_GOLD}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
