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

# CELL ********************

# CELDA 1 — PARÁMETROS DEL PIPELINE

MODO_CARGA = "RANGO"   # RANGO o FULL
FECHA_INICIO = "2026-07-01"      # Formato YYYY-MM-DD; vacío = automático
FECHA_FIN = "2026-08-31"         # Formato YYYY-MM-DD; vacío = automático

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

TABLA_ORIGEN_PAYMENTS = "Bronze_Layer.Payments"
TABLA_STAGING_PAYMENTS = "Staging_Layer.Payments_Staging"
TABLA_DESTINO_PAYMENTS = "Silver_Layer.Payments_Silver"

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

# LEER BRONZE Y FILTRAR POR PaymentDate

df = spark.table(TABLA_ORIGEN_PAYMENTS)

df = df.withColumn(
    "PaymentDate",
    F.to_timestamp(F.col("PaymentDate"))
)

if modo == "RANGO":
    df = df.filter(
        (F.col("PaymentDate") >= F.lit(str(fecha_inicio)).cast("timestamp"))
        & (F.col("PaymentDate") < F.lit(str(fecha_fin)).cast("timestamp"))
    )

cantidad_seleccionada_orders = df.count()

print(f"Órdenes seleccionadas: {cantidad_seleccionada_orders}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(
    F.col("PaymentMethod")
).distinct().show()

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
