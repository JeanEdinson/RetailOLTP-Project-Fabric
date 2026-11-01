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
from pyspark.sql.window import Window
from uuid import uuid4
from com.microsoft.spark.fabric.Constants import Constants

ESQUEMA_SILVER = 'RetailOLTP_Project_Bronze_Silver.Silver_Layer'
ESQUEMA_STAGING = 'RetailOLTP_Project_Bronze_Silver.Staging_Layer'
WAREHOUSE_GOLD = "RetailOLTP_Project_Gold"
ESQUEMA_STAGING_WAREHOUSE = "Staging_Layer"
ESQUEMA_GOLD = "Gold_Layer"

FACT_ORDERS_STAGING_GOLD = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.fact_orders_staging"
FACT_ORDERS_GOLD = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.fact_orders"

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

df_orders = spark.table(ESQUEMA_SILVER + '.orders_silver')
df_details = spark.table(ESQUEMA_SILVER + '.ordersdetails_silver')

df_orders = df_orders.withColumn(
    "OrderDate",
    F.to_timestamp(F.col("OrderDate"))
)

if modo == "RANGO":
    df_orders = df_orders.filter(
        (F.col("OrderDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("OrderDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

    df_orders_id_validos = df_orders.select("OrderID").distinct()
    df_details = df_details.join(
        df_orders_id_validos,
        on = "OrderID",
        how = "left_semi"
    )


cantidad_seleccionada_orders = df_orders.count()
cantidad_seleccionada_detalles = df_details.count()

print(f"Órdenes seleccionadas: {cantidad_seleccionada_orders}")
print(f"Detalles de Órdenes seleccionadas: {cantidad_seleccionada_detalles}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creación del hecho Orders (fact_orders)

# Calcular el número de detalles por orden
ventana_order_id = Window.partitionBy(F.col('OrderID'))
df_details = df_details.withColumn('number_of_details', F.count(F.col('OrderDetailID')).over(ventana_order_id))

# Lista de columnas a seleccionar
columnas_orders = [
    'o.OrderID',
    'od.OrderDetailID',
    'o.CustomerID',
    'od.ProductID',
    'o.StoreID',
    'o.EmployeeID',
    'o.OrderDate',
    'o.OrderStatus',
    'od.Quantity',
    'od.UnitPrice',
    'od.DiscountPercent',
    'od.DiscountAmount',
    'od.TaxAmount',
    'o.ShippingAmount',
    'od.number_of_details'
]

columnas_orders_ordenados = [
    'OrderID',
    'OrderDetailID',
    'CustomerID',
    'ProductID',
    'StoreID',
    'EmployeeID',
    'OrderDate',
    'OrderHour',
    'OrderStatus',
    'Quantity',
    'UnitPrice',
    'DiscountPercent',
    'DiscountAmount',
    'TaxAmount',
    'ShippingAmount',
    'LineTotal'
]

# Join entre tabla Orders y OrderDetails
df_fact_orders_staging = (
    df_orders.alias('o')
    .join(
        df_details.alias('od'),
        on='OrderID',
        how='left'
    )
    .select(columnas_orders)
)

# Dividir valor de ShippingAmount entre el número de detalles de cada orden
df_fact_orders_staging = df_fact_orders_staging.withColumn(
    "ShippingAmount",
    (F.col('ShippingAmount')/F.col('number_of_details')).cast('decimal(18,2)')
).drop('number_of_details')

# Dividir fecha y hora en dos columnas, y calcular el monto total por detalle de orden
df_fact_orders_staging = df_fact_orders_staging.withColumns({
    "OrderHour": F.date_format(F.col("OrderDate"), "HH:mm:ss"),
    "OrderDate": F.to_date(F.col('OrderDate')),
    "LineTotal": F.col('Quantity') * F.col('UnitPrice') - F.col('DiscountAmount') + F.col('TaxAmount') + F.col('ShippingAmount')
}).select(columnas_orders_ordenados)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agregar columnas de auditoria

fecha_procesamiento = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

df_fact_orders_staging = (
    df_fact_orders_staging
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
    df_fact_orders_staging.write
      .mode("overwrite")
      .synapsesql(FACT_ORDERS_STAGING_GOLD)
)
print(f'Se escribió la tabla {FACT_ORDERS_STAGING_GOLD}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear la capa Gold si no existe

# Validar existencia en Warehouse

nombre_tabla = FACT_ORDERS_GOLD.split(".")[-1]

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
        print(f"La tabla ya existe. Se omite: {FACT_ORDERS_GOLD}")

    else:
        # Obtener el esquema de staging sin registros
        df_vacio = spark.table(FACT_ORDERS_STAGING_GOLD).limit(0)

        # Crear la tabla vacía en el Warehouse
        (
            df_vacio.write
            .mode("errorifexists")
            .synapsesql(FACT_ORDERS_GOLD)
        )

        print(f"Tabla creada correctamente: {FACT_ORDERS_GOLD}")

except Exception as e:
    print(f"Error procesando {FACT_ORDERS_GOLD}: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
