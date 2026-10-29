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

# CELL ********************

# Configuración general

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from uuid import uuid4
from com.microsoft.spark.fabric.Constants import Constants

ESQUEMA_SILVER = 'RetailOLTP_Project_Bronze_Silver.Silver_Layer'
ESQUEMA_STAGING = 'RetailOLTP_Project_Bronze_Silver.Staging_Layer'
WAREHOUSE_GOLD = "RetailOLTP_Project_Gold"
ESQUEMA_STAGING_WAREHOUSE = "Staging_Layer"
ESQUEMA_GOLD = "Gold_Layer"

DIMENSION_STAGING_CUSTOMERS = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.dim_customers_staging"
DIMENSION_STAGING_PRODUCTS = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.dim_products_staging"
DIMENSION_STAGING_STORES = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.dim_stores_staging"
DIMENSION_STAGING_EMPLOYEES = f"{WAREHOUSE_GOLD}.{ESQUEMA_STAGING_WAREHOUSE}.dim_employees_staging"

DIMENSION_GOLD_CUSTOMERS = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.dim_customers"
DIMENSION_GOLD_PRODUCTS = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.dim_products"
DIMENSION_GOLD_STORES = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.dim_stores"
DIMENSION_GOLD_EMPLOYEES = f"{WAREHOUSE_GOLD}.{ESQUEMA_GOLD}.dim_employees"

BATCH_ID = str(uuid4())

DIMENSIONES = {
    "dim_customers": "CustomerID",
    "dim_employees": "EmployeeID",
    "dim_products": "ProductID",
    "dim_stores": "StoreID"
}

COLUMNAS_AUDITORIA = {
    "createdat",
    "updatedat",
    "batchid"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lectura de tablas en la capa silver

df_customers_s = spark.table(ESQUEMA_SILVER + '.customers_silver')
df_customer_addresses_s = spark.table(ESQUEMA_SILVER + '.customeraddresses_silver')
df_customer_segments_s = spark.table(ESQUEMA_SILVER + '.customersegments_silver')

df_products_s = spark.table(ESQUEMA_SILVER + '.products_silver')
df_suppliers_s = spark.table(ESQUEMA_SILVER + '.suppliers_silver')
df_categories_s = spark.table(ESQUEMA_SILVER + '.categories_silver')

df_employees_s = spark.table(ESQUEMA_SILVER + '.employees_silver')
df_stores_s = spark.table(ESQUEMA_SILVER + '.stores_silver')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creacion de la dimension Customers (dim_customers)

# Broadcast de la dimension de segmentos para optimizar el join
df_customer_segments_b = F.broadcast(df_customer_segments_s)

# Elegir la ultima direccion registrada por cliente
ventana_direccion = Window.partitionBy('CustomerID').orderBy(F.col('CustomerAddressesID').desc())
df_last_customer_addresses = df_customer_addresses_s.withColumn(
    'maximo', F.row_number().over(ventana_direccion)
).filter(F.col('maximo') == 1).drop('maximo')


# Lista de columnas a seleccionar
columnas_customers = [
    'CustomerID',
    'FirstName',
    'LastName',
    'Email',
    'Phone',
    'BirthDate',
    'Gender',
    'SegmentName',
    'AddressLine1',
    'AddressLine2',
    'City',
    'State',
    'PostalCode',
    'Country',
    'RegistrationDate',
    'Status'
]

# Join de customers con addresses y segments y selección de columnas finales
df_customers_staging = (
    df_customers_s
        .join(df_last_customer_addresses, on=['CustomerID'], how='left')
        .join(df_customer_segments_b, on=['CustomerSegmentID'], how='left')
        .select(columnas_customers)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creacion de la dimension Products (dim_products)

# Broadcast de la dimension de categorias para optimizar el join
df_categories_b = F.broadcast(df_categories_s)

# Lista de columnas a seleccionar
columnas_products = [
    'ProductID',
    'SKU',
    'ProductName',
    'CostPrice',
    'UnitsPerPackage',
    'Status',
    'CategoryName',
    'SupplierName',
    'ContactName',
    'Email',
    'Phone',
    'City',
    'Country'
]

# Join de products con categories y suppliers y selección de columnas finales
df_products_staging = (
    df_products_s
        .join(df_suppliers_s, on=['SupplierID'], how='left')
        .join(df_categories_b, on=['CategoryID'], how='left')
        .select(columnas_products)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creacion de la dimension Stores (dim_stores)

# Lista de columnas a seleccionar
columnas_stores = [
    'StoreID',
    'StoreCode',
    'StoreName',
    'Address',
    'City',
    'State',
    'Country',
    'OpeningDate',
    'StoreType',
    'Status'
]

df_stores_staging = df_stores_s.select(columnas_stores)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creacion de la dimension Employees (dim_employees)

# Lista de columnas a seleccionar
columnas_employees = [
    'EmployeeID',
    'FirstName',
    'LastName',
    'Email',
    'JobTitle',
    'HireDate',
    'Status'
]

df_employees_staging = df_employees_s.select(columnas_employees)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agregar columnas de auditoria

def with_audit_columns(df):
    """Devuelve una copia del DataFrame con columnas de auditoría."""
    fecha_procesamiento = spark.sql("SELECT current_timestamp() AS ts").first()["ts"]

    return (
        df
        .withColumn("CreatedAt", F.lit(fecha_procesamiento).cast("timestamp"))
        .withColumn("UpdatedAt", F.lit(fecha_procesamiento).cast("timestamp"))
        .withColumn("BatchID",   F.lit(BATCH_ID))
    )

# Aplicar función a cada DataFrame de staging

df_customers_staging  = with_audit_columns(df_customers_staging)
df_products_staging   = with_audit_columns(df_products_staging)
df_stores_staging     = with_audit_columns(df_stores_staging)
df_employees_staging  = with_audit_columns(df_employees_staging)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escribir tablas en la capa STAGING del Warehouse

(
    df_customers_staging.write
      .mode("overwrite")
      .synapsesql(DIMENSION_STAGING_CUSTOMERS)
)
print(f'Se escribió la tabla {DIMENSION_STAGING_CUSTOMERS}')

(
    df_products_staging.write
      .mode("overwrite")
      .synapsesql(DIMENSION_STAGING_PRODUCTS)
)
print(f'Se escribió la tabla {DIMENSION_STAGING_PRODUCTS}')

(
    df_stores_staging.write
      .mode("overwrite")
      .synapsesql(DIMENSION_STAGING_STORES)
)
print(f'Se escribió la tabla {DIMENSION_STAGING_STORES}')

(
    df_employees_staging.write
      .mode("overwrite")
      .synapsesql(DIMENSION_STAGING_EMPLOYEES)
)
print(f'Se escribió la tabla {DIMENSION_STAGING_EMPLOYEES}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear la capa Gold si no existe

dimensiones = {
    DIMENSION_GOLD_CUSTOMERS:
        DIMENSION_STAGING_CUSTOMERS,

    DIMENSION_GOLD_PRODUCTS:
        DIMENSION_STAGING_PRODUCTS,

    DIMENSION_GOLD_STORES:
        DIMENSION_STAGING_STORES,

    DIMENSION_GOLD_EMPLOYEES:
        DIMENSION_STAGING_EMPLOYEES
}

# Validar existencia en Warehouse

def tabla_existe_warehouse(nombre_tabla):

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

    return resultado["existe"] == 1

# Crear las dimensiones que no existen

for tabla_destino, tabla_staging in dimensiones.items():

    nombre_tabla = tabla_destino.split(".")[-1]

    try:
        if tabla_existe_warehouse(nombre_tabla):

            print(f"La tabla ya existe. Se omite: {tabla_destino}")
            continue

        # Obtener el esquema de staging sin registros
        df_vacio = spark.table(tabla_staging).limit(0)

        # Crear la tabla vacía en el Warehouse
        (
            df_vacio.write
            .mode("errorifexists")
            .synapsesql(tabla_destino)
        )

        print(f"Tabla creada correctamente: {tabla_destino}")

    except Exception as e:
        print(f"Error procesando {tabla_destino}: {str(e)}")
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
