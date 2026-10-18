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
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# CELDA 1 — PARÁMETROS DEL PIPELINE

MODO_CARGA = "RANGO"   # RANGO o FULL
FECHA_INICIO = ""      # Formato YYYY-MM-DD; vacío = automático
FECHA_FIN = ""         # Formato YYYY-MM-DD; vacío = automático


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CONGIGURACIÓN GENERAL

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4

from pyspark.sql import functions as F
from delta.tables import DeltaTable

TABLA_ORIGEN_ORDERS = "Bronze_Layer.Orders"
TABLA_STAGING_ORDERS = "Staging_Layer.Orders_Staging"
TABLA_DESTINO_ORDERS = "Silver_Layer.Orders_Silver"

TABLA_ORIGEN_ORDERS_DETAILS = "Bronze_Layer.OrderDetails"
TABLA_STAGING_ORDERS_DETAILS = "Staging_Layer.OrdersDetails_Staging"
TABLA_DESTINO_ORDERS_DETAILS = "Silver_Layer.OrdersDetails_Silver"

TABLA_CUSTOMERS_MAPPING = "Mapping_Layer.Customers_Mapping"
TABLA_EMPLOYEES_MAPPING = "Mapping_Layer.Employees_Mapping"
TABLA_STORES_MAPPING = "Mapping_Layer.Stores_Mapping"

SOURCE_SYSTEM = "RetailOLTP"
BATCH_ID = str(uuid4())

spark.conf.set("spark.sql.session.timeZone", "America/Lima")

MAP_CUSTOMER_SOURCE = "OriginalCustomerID"
MAP_CUSTOMER_TARGET = "NewCustomerID"

MAP_EMPLOYEE_SOURCE = "OriginalEmployeeID"
MAP_EMPLOYEE_TARGET = "NewEmployeeID"

MAP_STORE_SOURCE = "OriginalStoreID"
MAP_STORE_TARGET = "NewStoreID"


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

# LEER BRONZE Y FILTRAR POR OrderDate

df = spark.table(TABLA_ORIGEN_ORDERS)
df_details = spark.table(TABLA_ORIGEN_ORDERS_DETAILS)

df = df.withColumn(
    "OrderDate",
    F.to_timestamp(F.col("OrderDate"))
)

if modo == "RANGO":
    df = df.filter(
        (F.col("OrderDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("OrderDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

    df_orders_id_validos = df.select("OrderID").distinct()
    df_details = df_details.join(
        df_orders_id_validos,
        on = "OrderID",
        how = "left_semi"
    )


cantidad_seleccionada_orders = df.count()
cantidad_seleccionada_detalles = df_details.count()

print(f"Órdenes seleccionadas: {cantidad_seleccionada_orders}")
print(f"Detalles de Órdenes seleccionadas: {cantidad_seleccionada_detalles}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpieza de datos

# Eliminar duplicados
df = df.drop_duplicates(["OrderDate", "OrderStatus", "Subtotal", "DiscountAmount", "TaxAmount", "ShippingAmount", "TotalAmount", "CustomerID", "EmployeeID", "StoreID"])
df_details = df_details.drop_duplicates()

# NORMALIZAR OrderStatus en ORDERS

if "OrderStatus" in df.columns:

    estado = F.lower(F.trim(F.col("OrderStatus")))

    estados_invalidos = [
        "", "unknown", "n/a", "na", "null", "none"
    ]

    equivalencias_estado = {
        "pending": "Pending",
        "pendiente": "Pending",
        "processing": "Processing",
        "shipped": "Shipped",
        "delivered": "Delivered",
        "cancelled": "Cancelled",
        "canceled": "Cancelled",
        "cancelado": "Cancelled",
        "completed": "Completed",
        "completado": "Completed"
    }

    expresion_estado = F.when(
        estado.isin(estados_invalidos),
        F.lit("No registered status").cast("string")
    )

    for valor, canonico in equivalencias_estado.items():
        expresion_estado = expresion_estado.when(
            estado == valor,
            F.lit(canonico)
        )

    df = df.withColumn(
        "OrderStatus",
        expresion_estado.otherwise(F.col("OrderStatus"))
    )

# Convertir valores de cantidades y porcentajes de descuento en positivos en ORDERDETAILS
# Por regla de negocio, las cantidades absolutas son correctas y los porcentajes de descuento mayores a 100% son el descuento total del monto de línea

df_details = df_details.withColumn(
    "Quantity",
    F.when(
        F.col("Quantity") < 0,
        F.col("Quantity") * -1
    ).otherwise(F.col("Quantity"))
)

df_details = df_details.withColumn(
    "DiscountPercent",
    F.when(
        F.col("DiscountPercent") < 0,
        F.col("DiscountPercent") * -1
    ).when(
        F.col("DiscountPercent") > 100,
        100
    ).otherwise(F.col("DiscountPercent"))
)

# Recalcular importes de ORDERSDETAILS y ORDENES

df_details = df_details.withColumn(
    "DiscountAmount",
    (F.col("Quantity") * F.col("UnitPrice") * (F.col("DiscountPercent")/100)).cast("decimal(18,2)")
)

df_details = df_details.withColumn(
    "LineTotal",
    (F.col("Quantity") * F.col("UnitPrice") - F.col("DiscountAmount") + F.col("TaxAmount")).cast("decimal(18,2)")
)

df_detail_agrupado = df_details.groupBy(
    "OrderID"
).agg(
    F.sum(F.col("Quantity") * F.col("UnitPrice")).alias("Subtotal").cast("decimal(18,2)"),
    F.sum("DiscountAmount").alias("DiscountAmount").cast("decimal(18,2)"),
    F.sum("TaxAmount").alias("TaxAmount").cast("decimal(18,2)")
)

df = df.select("OrderID", "CustomerID", "StoreID", "EmployeeID", "OrderDate", "OrderStatus", "ShippingAmount", "TotalAmount").join(
    df_detail_agrupado,
    on = "OrderID",
    how = "left"
).filter(F.col("Subtotal").isNotNull())

# Tratar valores nulos de ShippingAmount

df = df.withColumn(
    "ShippingAmount",
    F.when(
        F.col("ShippingAmount").isNull(),
        0
    ).otherwise(F.col("ShippingAmount"))
)

# Recalcular importe total de ORDERS

df = df.withColumn(
    "TotalAmount",
    F.col("Subtotal") + F.col("TaxAmount") + F.col("ShippingAmount") - F.col("DiscountAmount")
)

df = df.select('OrderID','CustomerID','StoreID','EmployeeID','OrderDate','OrderStatus','Subtotal','DiscountAmount','TaxAmount', 'ShippingAmount','TotalAmount')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mapear ID's estandarizados

# FUNCIÓN PARA APLICAR LOS MAPPINGS


def aplicar_mapping(
    dataframe,
    tabla_mapping,
    columna_fk,
    columna_mapping_origen,
    columna_mapping_destino
):
    """
    Sustituye una FK por su ID estandarizado.

    - Verifica la existencia de las columnas del mapping.
    - Detecta asociaciones conflictivas.
    - Evita multiplicar registros por mappings repetidos.
    - Detiene la carga si un ID informado no tiene mapping.
    """

    if columna_fk not in dataframe.columns:
        raise ValueError(
            f"No existe la columna FK {columna_fk}."
        )

    df_mapping_original = spark.table(tabla_mapping)

    columnas_mapping_requeridas = [
        columna_mapping_origen,
        columna_mapping_destino
    ]

    faltantes_mapping = [
        c for c in columnas_mapping_requeridas
        if c not in df_mapping_original.columns
    ]

    if faltantes_mapping:
        raise ValueError(
            f"Faltan columnas en {tabla_mapping}: "
            f"{faltantes_mapping}. "
            "Corrige la configuración del notebook."
        )

    mapping = (
        df_mapping_original
        .select(
            F.col(columna_mapping_origen).alias("_ID_Origen"),
            F.col(columna_mapping_destino).alias("_ID_Estandar")
        )
    )

    # Un ID de origen no puede tener múltiples destinos.
    conflictos = (
        mapping
        .groupBy("_ID_Origen")
        .agg(
            F.countDistinct("_ID_Estandar").alias("_cantidad")
        )
        .filter(F.col("_cantidad") > 1)
        .limit(1)
        .count()
    )

    if conflictos:
        raise ValueError(
            f"Mapping conflictivo en {tabla_mapping}: "
            "un ID de origen tiene varios IDs estandarizados."
        )

    # Eliminar duplicados idénticos del mapping.
    mapping = mapping.dropDuplicates(
        ["_ID_Origen", "_ID_Estandar"]
    )

    # Comprobar IDs de origen sin mapping.
    ids_origen = dataframe.select(
        F.col(columna_fk).alias("_ID_Origen")
    ).filter(
        F.col("_ID_Origen").isNotNull()
    ).distinct()

    ids_mapping = mapping.select("_ID_Origen").distinct()

    sin_correspondencia = (
        ids_origen
        .join(ids_mapping, on="_ID_Origen", how="left_anti")
        .limit(1)
        .count()
    )

    if sin_correspondencia:
        raise ValueError(
            f"Existen valores de {columna_fk} sin mapping "
            f"en {tabla_mapping}. La carga se detuvo."
        )

    # Validar IDs estandarizados nulos.
    mapping_nulo = (
        mapping
        .filter(
            F.col("_ID_Origen").isNotNull()
            & F.col("_ID_Estandar").isNull()
        )
        .limit(1)
        .count()
    )

    if mapping_nulo:
        raise ValueError(
            f"{tabla_mapping} contiene IDs estandarizados nulos "
            "para claves de origen informadas."
        )

    # Reemplazar la FK de origen por la FK estandarizada.
    temporal = dataframe.alias("d").join(
        mapping.alias("m"),
        F.col(f"d.`{columna_fk}`") == F.col("m._ID_Origen"),
        "left"
    )

    resultado = temporal.select(
        *[
            F.col(f"d.`{c}`").alias(c)
            for c in dataframe.columns
            if c != columna_fk
        ],
        F.col("m._ID_Estandar").alias(columna_fk)
    )

    return resultado

# APLICAR LOS TRES MAPPINGS

df = aplicar_mapping(
    df,
    TABLA_CUSTOMERS_MAPPING,
    "CustomerID",
    MAP_CUSTOMER_SOURCE,
    MAP_CUSTOMER_TARGET
)

df = aplicar_mapping(
    df,
    TABLA_EMPLOYEES_MAPPING,
    "EmployeeID",
    MAP_EMPLOYEE_SOURCE,
    MAP_EMPLOYEE_TARGET
)

df = aplicar_mapping(
    df,
    TABLA_STORES_MAPPING,
    "StoreID",
    MAP_STORE_SOURCE,
    MAP_STORE_TARGET
)

print("Mappings aplicados correctamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validación del OrderID

if df.filter(
    F.col("OrderID").isNull()
).limit(1).count():
    raise ValueError(
        "Se encontraron órdenes sin OrderID."
    )

duplicados = (
    df.groupBy("OrderID")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicados:
    raise ValueError(
        "Hay OrderID duplicados en la carga. "
        "No se continuará con el MERGE."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validación del OrderID y OrderDetailID

if df_details.filter(
    (F.col("OrderID").isNull()) |
    (F.col("OrderDetailID").isNull())
).limit(1).count():
    raise ValueError(
        "Se encontraron detalles de órdenes sin OrderID u OrderDetailID."
    )

duplicados = (
    df_details.groupBy(["OrderID", "OrderDetailID"])
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicados:
    raise ValueError(
        "Hay combiaciones de OrderID y OrderDetailID duplicados en la carga. "
        "No se continuará con el MERGE."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agregar columnas de auditoria

fecha_procesamiento = spark.sql(
    "SELECT current_timestamp() AS ts"
).first()["ts"]

df = (
    df
    .withColumn(
        "CreatedAt",
        F.lit(fecha_procesamiento).cast("timestamp")
    )
    .withColumn(
        "UpdatedAt",
        F.lit(fecha_procesamiento).cast("timestamp")
    )
    .withColumn(
        "SourceSystem",
        F.lit(SOURCE_SYSTEM)
    )
    .withColumn(
        "BatchID",
        F.lit(BATCH_ID)
    )
)

df_details = (
    df_details
    .withColumn(
        "CreatedAt",
        F.lit(fecha_procesamiento).cast("timestamp")
    )
    .withColumn(
        "UpdatedAt",
        F.lit(fecha_procesamiento).cast("timestamp")
    )
    .withColumn(
        "SourceSystem",
        F.lit(SOURCE_SYSTEM)
    )
    .withColumn(
        "BatchID",
        F.lit(BATCH_ID)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Escribir tablas en la capa STAGING

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLA_STAGING_ORDERS)
)

(
    df_details.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLA_STAGING_ORDERS_DETAILS)
)

print(f"Staging cargado: {TABLA_STAGING_ORDERS}")
print(f"Staging cargado: {TABLA_STAGING_ORDERS_DETAILS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear la capa Silver si no existe

if not spark.catalog.tableExists(TABLA_DESTINO_ORDERS):

    # Crear una tabla Delta vacía con el mismo esquema de staging.
    (
        spark.table(TABLA_STAGING_ORDERS)
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABLA_DESTINO_ORDERS)
    )

    print(f"Tabla creada: {TABLA_DESTINO_ORDERS}")

if not spark.catalog.tableExists(TABLA_DESTINO_ORDERS_DETAILS):

    # Crear una tabla Delta vacía con el mismo esquema de staging.
    (
        spark.table(TABLA_STAGING_ORDERS_DETAILS)
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABLA_DESTINO_ORDERS_DETAILS)
    )

    print(f"Tabla creada: {TABLA_DESTINO_ORDERS_DETAILS}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validar compatibilidad de esquemas

df_staging_orders = spark.table(TABLA_STAGING_ORDERS)
df_destino_orders = spark.table(TABLA_DESTINO_ORDERS)
df_staging_details = spark.table(TABLA_STAGING_ORDERS_DETAILS)
df_destino_details = spark.table(TABLA_DESTINO_ORDERS_DETAILS)

columnas_staging_orders = set(df_staging_orders.columns)
columnas_destino_orders = set(df_destino_orders.columns)
columnas_staging_details = set(df_staging_details.columns)
columnas_destino_details = set(df_destino_details.columns)

if columnas_staging_orders != columnas_destino_orders:
    faltan_destino_orders = sorted(
        columnas_staging_orders - columnas_destino_orders
    )
    sobran_destino_orders = sorted(
        columnas_destino_orders - columnas_staging_orders
    )

    raise ValueError(
        "Tabla ORDERS: "
        "El esquema de Staging no coincide con Silver. "
        f"Columnas que faltan en Silver: {faltan_destino_orders}. "
        f"Columnas adicionales en Silver: {sobran_destino_orders}."
    )

if columnas_staging_details != columnas_destino_details:
    faltan_destino_details = sorted(
        columnas_staging_details - columnas_destino_details
    )
    sobran_destino_details = sorted(
        columnas_destino_details - columnas_staging_details
    )

    raise ValueError(
        "Tabla ORDERS: "
        "El esquema de Staging no coincide con Silver. "
        f"Columnas que faltan en Silver: {faltan_destino_details}. "
        f"Columnas adicionales en Silver: {sobran_destino_details}."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UPSERT de la tabla ORDERS

if df_staging_orders.limit(1).count() == 0:

    print(
        "Staging no contiene registros para este rango. "
        "No se modificó Silver."
    )

else:

    destino = DeltaTable.forName(
        spark,
        TABLA_DESTINO_ORDERS
    )

    columnas_auditoria = [
        "CreatedAt",
        "UpdatedAt",
        "SourceSystem",
        "BatchID"
    ]

    columnas_negocio = [
        c for c in df_staging_orders.columns
        if c not in columnas_auditoria
    ]

    # Comparación null-safe para detectar cambios reales.
    condicion_cambio = " OR ".join(
        [
            f"NOT (t.`{c}` <=> s.`{c}`)"
            for c in columnas_negocio
        ]
    )

    # UPDATE:
    # - Conservar CreatedAt.
    # - Actualizar UpdatedAt, BatchID y demás campos
    #   únicamente cuando cambien datos de negocio.
    asignaciones_update = {
        c: f"s.`{c}`"
        for c in df_staging_orders.columns
        if c not in ["OrderID", "CreatedAt", "UpdatedAt"]
    }

    asignaciones_update["UpdatedAt"] = "s.`UpdatedAt`"

    # INSERT: incluir todas las columnas.
    asignaciones_insert = {
        c: f"s.`{c}`"
        for c in df_staging_orders.columns
    }

    (
        destino.alias("t")
        .merge(
            df_staging_orders.alias("s"),
            "t.OrderID = s.OrderID"
        )
        .whenMatchedUpdate(
            condition=condicion_cambio,
            set=asignaciones_update
        )
        .whenNotMatchedInsert(
            values=asignaciones_insert
        )
        .execute()
    )

    print("UPSERT de la tabla ORDERS completado correctamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UPSERT de la tabla ORDERSDETAILS

if df_staging_details.limit(1).count() == 0:

    print(
        "Staging no contiene registros para este rango. "
        "No se modificó Silver."
    )

else:

    destino = DeltaTable.forName(
        spark,
        TABLA_DESTINO_ORDERS_DETAILS
    )

    columnas_auditoria = [
        "CreatedAt",
        "UpdatedAt",
        "SourceSystem",
        "BatchID"
    ]

    columnas_negocio = [
        c for c in df_staging_details.columns
        if c not in columnas_auditoria
    ]

    # Comparación null-safe para detectar cambios reales.
    condicion_cambio = " OR ".join(
        [
            f"NOT (t.`{c}` <=> s.`{c}`)"
            for c in columnas_negocio
        ]
    )

    # UPDATE:
    # - Conservar CreatedAt.
    # - Actualizar UpdatedAt, BatchID y demás campos
    #   únicamente cuando cambien datos de negocio.
    asignaciones_update = {
        c: f"s.`{c}`"
        for c in df_staging_details.columns
        if c not in ["OrderID", "OrderDetailID", "CreatedAt", "UpdatedAt"]
    }

    asignaciones_update["UpdatedAt"] = "s.`UpdatedAt`"

    # INSERT: incluir todas las columnas.
    asignaciones_insert = {
        c: f"s.`{c}`"
        for c in df_staging_details.columns
    }

    (
        destino.alias("t")
        .merge(
            df_staging_details.alias("s"),
            """
            t.OrderID = s.OrderID
            AND t.OrderDetailID = s.OrderDetailID
            """
        )
        .whenMatchedUpdate(
            condition=condicion_cambio,
            set=asignaciones_update
        )
        .whenNotMatchedInsert(
            values=asignaciones_insert
        )
        .execute()
    )

    print("UPSERT de la tabla ORDERDETAILS completado correctamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RESUMEN DE EJECUCIÓN

print("========================================")
print("PROCESO ORDERS Y ORDERDETAILS FINALIZADO")
print(f"Modo de carga: {modo}")
print(f"Órdenes seleccionadas: {cantidad_seleccionada_orders}")
print(f"Detalles de Órdenes seleccionadas: {cantidad_seleccionada_detalles}")
print(f"Tablas origen: {TABLA_ORIGEN_ORDERS} y {TABLA_ORIGEN_ORDERS_DETAILS}")
print(f"Tablas staging: {TABLA_STAGING_ORDERS} y {TABLA_STAGING_ORDERS_DETAILS}")
print(f"Tablas destino: {TABLA_DESTINO_ORDERS} y {TABLA_DESTINO_ORDERS_DETAILS}")
print(f"BatchID: {BATCH_ID}")
print("========================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
