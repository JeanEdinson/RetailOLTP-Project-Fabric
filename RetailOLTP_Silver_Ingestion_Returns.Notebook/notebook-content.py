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

TABLA_ORIGEN_RETURNS = "Bronze_Layer.Returns"
TABLA_STAGING_RETURNS = "Staging_Layer.Returns_Staging"
TABLA_DESTINO_RETURNS = "Silver_Layer.Returns_Silver"

SOURCE_SYSTEM = "RetailOLTP"
BATCH_ID = str(uuid4())

spark.conf.set("spark.sql.session.timeZone", "America/Lima")

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

# LEER BRONZE Y FILTRAR POR ReturnDate

df = spark.table(TABLA_ORIGEN_RETURNS)

df = df.withColumn(
    "ReturnDate",
    F.to_timestamp(F.col("ReturnDate"))
)

if modo == "RANGO":
    df = df.filter(
        (F.col("ReturnDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("ReturnDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

cantidad_seleccionada = df.count()

print(f"Retornos seleccionados: {cantidad_seleccionada}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Limpieza de datos

# Eliminar duplicados
df.drop_duplicates(["ReturnDate", "Quantity", "ReturnReason", "RefundAmount", "ReturnStatus"])

# Estandarizar ReturnStatus

columna_estado = F.lower(F.trim(F.col("ReturnStatus")))

estados_invalidos = ["unknown", "n/a", ""]

equivalencias_ReturnStatus = {
    "rejected": "Rejected",
    "approved": "Approved",
    "rechazado": "Rejected",
    "pendiente": "Pending",
    "aprobado": "Approved",
    "pending": "Pending"
}

expresion_condicional = F.when(
    columna_estado.isNull() | columna_estado.isin(estados_invalidos), F.lit("No Registered Status").cast("string")
)

for valor, canonico in equivalencias_ReturnStatus.items():
    expresion_condicional = expresion_condicional.when(
        columna_estado == valor,
        F.lit(canonico)
    )

df = df.withColumn(
    "ReturnStatus",
    expresion_condicional.otherwise(F.initcap(F.trim(F.col("ReturnStatus"))))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validación del ReturnID

if df.filter(
    F.col("ReturnID").isNull()
).limit(1).count():
    raise ValueError(
        "Se encontraron retornos sin ReturnID."
    )

duplicados = (
    df.groupBy("ReturnID")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicados:
    raise ValueError(
        "Hay ReturnID duplicados en la carga. "
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
    .saveAsTable(TABLA_STAGING_RETURNS)
)

print(f"Staging cargado: {TABLA_STAGING_RETURNS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Crear la capa Silver si no existe

if not spark.catalog.tableExists(TABLA_DESTINO_RETURNS):

    # Crear una tabla Delta vacía con el mismo esquema de staging.
    (
        spark.table(TABLA_STAGING_RETURNS)
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABLA_DESTINO_RETURNS)
    )

    print(f"Tabla creada: {TABLA_DESTINO_RETURNS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validar compatibilidad de esquemas

df_staging = spark.table(TABLA_STAGING_RETURNS)
df_destino = spark.table(TABLA_DESTINO_RETURNS)

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UPSERT de la tabla RETURNS

if df_staging.limit(1).count() == 0:

    print(
        "Staging no contiene registros para este rango. "
        "No se modificó Silver."
    )

else:

    destino = DeltaTable.forName(
        spark,
        TABLA_DESTINO_RETURNS
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
    # - Conservar CreatedAt.
    # - Actualizar UpdatedAt, BatchID y demás campos
    #   únicamente cuando cambien datos de negocio.
    asignaciones_update = {
        c: f"s.`{c}`"
        for c in df_staging.columns
        if c not in ["ReturnID", "CreatedAt", "UpdatedAt"]
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
            "t.ReturnID = s.ReturnID"
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

    print("UPSERT de la tabla RETURNS completado correctamente.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RESUMEN DE EJECUCIÓN

print("========================================")
print("PROCESO RETURNS FINALIZADO")
print(f"Modo de carga: {modo}")
print(f"Retornos seleccionados: {cantidad_seleccionada}")
print(f"Tabla origen: {TABLA_ORIGEN_RETURNS}")
print(f"Tabla staging: {TABLA_STAGING_RETURNS}")
print(f"Tabla destino: {TABLA_DESTINO_RETURNS}")
print(f"BatchID: {BATCH_ID}")
print("========================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
