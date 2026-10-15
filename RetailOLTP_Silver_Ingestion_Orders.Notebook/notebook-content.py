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


# ============================================================
# CELDA 1 — PARÁMETROS DEL PIPELINE
# Marcar esta celda como "Parameter cell" en Microsoft Fabric
# ============================================================

MODO_CARGA = "RANGO"   # RANGO o FULL
FECHA_INICIO = "2026-08-01"      # Formato YYYY-MM-DD; vacío = automático
FECHA_FIN = "2026-08-31"         # Formato YYYY-MM-DD; vacío = automático


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================================
# CELDA 2 — NOTEBOOK ORDERS
# Bronze -> Limpieza -> Mappings -> Staging -> MERGE -> Silver
# ============================================================

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURACIÓN GENERAL
# ============================================================

TABLA_ORIGEN = "Bronze_Layer.Orders"
TABLA_STAGING = "Staging_Layer.Orders_Staging"
TABLA_DESTINO = "Silver_Layer.Orders"

TABLA_CUSTOMERS_MAPPING = "Mapping_Layer.Customers_Mapping"
TABLA_EMPLOYEES_MAPPING = "Mapping_Layer.Employees_Mapping"
TABLA_STORES_MAPPING = "Mapping_Layer.Stores_Mapping"

SOURCE_SYSTEM = "RetailOLTP"
BATCH_ID = str(uuid4())

# Zona horaria de negocio para interpretar los rangos.
spark.conf.set("spark.sql.session.timeZone", "America/Lima")


# ============================================================
# 2. CONFIGURACIÓN DE LAS COLUMNAS DE MAPPING
# IMPORTANTE: ajustar los nombres a las tablas reales.
# ============================================================

MAP_CUSTOMER_SOURCE = "OriginalCustomerID"
MAP_CUSTOMER_TARGET = "NewCustomerID"

MAP_EMPLOYEE_SOURCE = "EmployeeID"
MAP_EMPLOYEE_TARGET = "NewEmployeeID"

MAP_STORE_SOURCE = "OriginalStoreID"
MAP_STORE_TARGET = "NewStoreID"


# ============================================================
# 3. VALIDAR PARÁMETROS Y DETERMINAR EL RANGO TEMPORAL
# ============================================================

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


# ============================================================
# 4. LEER BRONZE Y FILTRAR POR OrderDate
# ============================================================

df = spark.table(TABLA_ORIGEN)

columnas_requeridas = [
    "OrderID",
    "CustomerID",
    "EmployeeID",
    "StoreID",
    "OrderDate"
]

columnas_faltantes = [
    c for c in columnas_requeridas
    if c not in df.columns
]

if columnas_faltantes:
    raise ValueError(
        f"Faltan columnas en {TABLA_ORIGEN}: "
        f"{columnas_faltantes}"
    )

df = df.withColumn(
    "OrderDate",
    F.to_timestamp(F.col("OrderDate"))
)

if modo == "RANGO":
    df = df.filter(
        (F.col("OrderDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("OrderDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

cantidad_seleccionada = df.count()

print(f"Órdenes seleccionadas: {cantidad_seleccionada}")


# ============================================================
# 5. LIMPIEZA DE TEXTO
# ============================================================

columnas_texto = [
    campo.name
    for campo in df.schema.fields
    if campo.dataType.simpleString() == "string"
]

for columna in columnas_texto:
    df = df.withColumn(
        columna,
        F.when(
            F.col(columna).isNull(),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col(columna)))
    )


# ============================================================
# 6. NORMALIZAR OrderStatus
# ============================================================

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
        "completed": "Completed"
    }

    expresion_estado = F.when(
        estado.isin(estados_invalidos),
        F.lit(None).cast("string")
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


# ============================================================
# 7. NORMALIZAR TIPOS DE DATOS DE LOS IMPORTES
# ============================================================

columnas_importes = [
    "Subtotal",
    "DiscountAmount",
    "ShippingAmount",
    "TotalAmount"
]

for columna in columnas_importes:
    if columna in df.columns:
        df = df.withColumn(
            columna,
            F.col(columna).cast("decimal(18,2)")
        )

# Los importes no convertibles quedan como NULL.
# No se corrigen automáticamente importes alterados,
# ya que se necesita una regla de negocio para recalcularlos.


# ============================================================
# 8. FUNCIÓN PARA APLICAR LOS MAPPINGS
# ============================================================

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


# ============================================================
# 9. APLICAR LOS TRES MAPPINGS
# ============================================================

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


# ============================================================
# 10. VALIDAR OrderID
# ============================================================

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


# ============================================================
# 11. AGREGAR COLUMNAS DE AUDITORÍA
# ============================================================

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

print(f"BatchID: {BATCH_ID}")


# ============================================================
# 12. ESCRIBIR STAGING
# ============================================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLA_STAGING)
)

print(f"Staging cargado: {TABLA_STAGING}")


# ============================================================
# 13. CREAR SILVER SI NO EXISTE
# ============================================================

if not spark.catalog.tableExists(TABLA_DESTINO):

    # Crear una tabla Delta vacía con el mismo esquema de staging.
    (
        spark.table(TABLA_STAGING)
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABLA_DESTINO)
    )

    print(f"Tabla creada: {TABLA_DESTINO}")


# ============================================================
# 14. VALIDAR COMPATIBILIDAD DE ESQUEMAS
# ============================================================

df_staging = spark.table(TABLA_STAGING)
df_destino = spark.table(TABLA_DESTINO)

columnas_staging = set(df_staging.columns)
columnas_destino = set(df_destino.columns)

if columnas_staging != columnas_destino:
    faltan_destino = sorted(
        columnas_staging - columnas_destino
    )
    sobran_destino = sorted(
        columnas_destino - columnas_staging
    )

    raise ValueError(
        "El esquema de Staging no coincide con Silver. "
        f"Columnas que faltan en Silver: {faltan_destino}. "
        f"Columnas adicionales en Silver: {sobran_destino}."
    )


# ============================================================
# 15. UPSERT DE STAGING HACIA SILVER
# ============================================================

if df_staging.limit(1).count() == 0:

    print(
        "Staging no contiene registros para este rango. "
        "No se modificó Silver."
    )

else:

    destino = DeltaTable.forName(
        spark,
        TABLA_DESTINO
    )

    columnas_auditoria = [
        "CreatedAt",
        "UpdatedAt",
        "SourceSystem",
        "BatchID"
    ]

    columnas_negocio = [
        c for c in df_staging.columns
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
    # - No modificar OrderID.
    # - Conservar CreatedAt.
    # - Actualizar UpdatedAt, BatchID y demás campos
    #   únicamente cuando cambien datos de negocio.
    asignaciones_update = {
        c: f"s.`{c}`"
        for c in df_staging.columns
        if c not in ["OrderID", "CreatedAt", "UpdatedAt"]
    }

    asignaciones_update["UpdatedAt"] = "s.`UpdatedAt`"

    # INSERT: incluir todas las columnas.
    asignaciones_insert = {
        c: f"s.`{c}`"
        for c in df_staging.columns
    }

    (
        destino.alias("t")
        .merge(
            df_staging.alias("s"),
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

    print("UPSERT completado correctamente.")


# ============================================================
# 16. RESUMEN DE EJECUCIÓN
# ============================================================

print("========================================")
print("PROCESO ORDERS FINALIZADO")
print(f"Modo de carga: {modo}")
print(f"Registros seleccionados: {cantidad_seleccionada}")
print(f"Tabla origen: {TABLA_ORIGEN}")
print(f"Tabla staging: {TABLA_STAGING}")
print(f"Tabla destino: {TABLA_DESTINO}")
print(f"BatchID: {BATCH_ID}")
print("========================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
