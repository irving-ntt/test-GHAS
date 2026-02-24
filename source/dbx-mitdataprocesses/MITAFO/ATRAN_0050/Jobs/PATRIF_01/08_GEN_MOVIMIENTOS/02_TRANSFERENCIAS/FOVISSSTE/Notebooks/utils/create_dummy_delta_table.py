# Databricks notebook source
# MAGIC %run "../startup"

# COMMAND ----------

import decimal
import random
from datetime import datetime, timedelta

from pyspark.sql.functions import col, rand, randn
from pyspark.sql.types import *

params = WidgetParams(
    {
        # Parámetros específicos para datos dummy
        "sr_folio": str,
        "num_records": int,  # Número de registros a generar
        "tipo_subcta15": int,  # Tipo subcuenta 15
        "tipo_subcta16": int,  # Tipo subcuenta 16
        "output_table_name": str,  # Nombre de la tabla de salida
    }
)
params.validate()
conf = ConfManager()
db = DBXConnectionManager()


def generate_dummy_data_for_datastage(
    spark, num_records=1000, tipo_subcta15=15, tipo_subcta16=16
):
    """
    Generar datos dummy que simulan la entrada del job DataStage JP_PATRIF_0800_GMO_TRANS_FOV_01.
    Esquema basado en el dataset DS_700_SALDOS_PROC del job DataStage.
    """

    logger.info(
        f"Generando {num_records} registros de datos dummy según esquema DataStage..."
    )

    # Esquema exacto del dataset de entrada del job DataStage
    schema = StructType(
        [
            StructField("FTN_NUM_CTA_INVDUAL", DecimalType(10, 0), True),
            StructField("FTC_FOLIO_BITACORA", StringType(), True),
            StructField("FCN_ID_SUBP", DecimalType(10, 0), False),  # not nullable
            StructField("FTN_VALOR_AIVS", DecimalType(18, 14), True),
            StructField("FTN_IND_SDO_DISP_VIV97", DecimalType(1, 0), True),
            StructField("FTN_SDO_AIVS", DecimalType(18, 6), True),
            StructField("FTN_MONTO_PESOS", DecimalType(18, 2), True),
            StructField("FCN_ID_TIPO_SUBCTA_97", DecimalType(4, 0), True),
            StructField("FTN_NUM_APLI_INTE_VIVI97_SOL", DecimalType(18, 6), True),
            StructField("FTN_SALDO_TOT_VIVI08", DecimalType(18, 2), True),
            StructField("FTN_IND_SDO_DISP_92", DecimalType(1, 0), True),
            StructField("FTN_SDO_AIVS_92", DecimalType(18, 6), True),
            StructField("FTN_MONTO_PESOS_92", DecimalType(18, 2), True),
            StructField("FCN_ID_TIPO_SUBCTA_92", DecimalType(10, 0), True),
            StructField("FTN_NUM_APLI_INTE_VIVI92_SOL", DecimalType(18, 6), True),
            StructField("FTN_SALDO_TOT_VIVI92", DecimalType(18, 2), True),
            StructField("FCN_ESTATUS", DecimalType(10, 0), True),
            StructField("FCN_ID_REGIMEN", DecimalType(10, 0), True),
            StructField("FCN_ID_VALOR_ACCION", DecimalType(10, 0), True),
            StructField("FTN_CONTA_SERV", DecimalType(10, 0), True),
            StructField("FTN_MOTI_RECH", DecimalType(10, 0), True),
            StructField("FTD_FEH_CRE", TimestampType(), True),
            StructField("FTC_USU_CRE", StringType(), True),
        ]
    )

    data = []
    base_date = datetime.now() - timedelta(days=30)

    for i in range(num_records):
        # Generar datos según el esquema exacto del DataStage
        cuenta = decimal.Decimal(str(1000000000 + i))
        folio = f"{params.sr_folio}"
        subp = decimal.Decimal(str(random.choice([10, 20, 30, 40])))
        valor_aivs = decimal.Decimal(f"{random.uniform(0.5, 2.5):.14f}")
        ind_saldo_viv97 = decimal.Decimal(str(random.choice([0, 1])))
        saldo_aivs = decimal.Decimal(f"{random.uniform(1000.0, 50000.0):.6f}")
        monto_pesos = decimal.Decimal(f"{random.uniform(50000.0, 200000.0):.2f}")
        tipo_subcta_97 = decimal.Decimal(
            str(random.choice([tipo_subcta15, tipo_subcta16, 20, 25]))
        )
        apli_inte_vivi97 = decimal.Decimal(f"{random.uniform(0.0, 1000.0):.6f}")
        saldo_tot_vivi08 = decimal.Decimal(f"{random.uniform(0.0, 100000.0):.2f}")
        ind_saldo_92 = decimal.Decimal(str(random.choice([0, 1])))
        saldo_aivs_92 = decimal.Decimal(f"{random.uniform(1000.0, 50000.0):.6f}")
        monto_pesos_92 = decimal.Decimal(f"{random.uniform(50000.0, 200000.0):.2f}")
        tipo_subcta_92 = decimal.Decimal(
            str(random.choice([tipo_subcta15, tipo_subcta16, 20, 25]))
        )
        apli_inte_vivi92 = decimal.Decimal(f"{random.uniform(0.0, 1000.0):.6f}")
        saldo_tot_vivi92 = decimal.Decimal(f"{random.uniform(0.0, 100000.0):.2f}")
        estatus = decimal.Decimal(str(random.choice([1, 2, 3, 4])))
        regimen = decimal.Decimal(str(random.choice([1, 2, 3])))
        valor_accion = decimal.Decimal(str(random.choice([1, 2, 3, 4])))
        conta_serv = decimal.Decimal(str(random.randint(0, 10)))
        moti_rech = decimal.Decimal(str(random.choice([0, 1, 2, 3])))
        fecha_cre = base_date + timedelta(days=random.randint(0, 30))
        usuarios = ["SISTEMA", "ADMIN", "OPERADOR", "SUPERVISOR"]
        usuario = random.choice(usuarios)

        data.append(
            (
                cuenta,
                folio,
                subp,
                valor_aivs,
                ind_saldo_viv97,
                saldo_aivs,
                monto_pesos,
                tipo_subcta_97,
                apli_inte_vivi97,
                saldo_tot_vivi08,
                ind_saldo_92,
                saldo_aivs_92,
                monto_pesos_92,
                tipo_subcta_92,
                apli_inte_vivi92,
                saldo_tot_vivi92,
                estatus,
                regimen,
                valor_accion,
                conta_serv,
                moti_rech,
                fecha_cre,
                usuario,
            )
        )

    df = spark.createDataFrame(data, schema)
    logger.info(
        f"Datos dummy generados exitosamente según esquema DataStage: {df.count()} registros"
    )
    return df


# COMMAND ----------

num_records = getattr(params, "num_records", 1000)
tipo_subcta15 = getattr(params, "tipo_subcta15", 15)
tipo_subcta16 = getattr(params, "tipo_subcta16", 16)
output_table_name = getattr(
    params, "output_table_name", f"datastage_dummy_input_{params.sr_folio}"
)
output_table_name = f"{output_table_name}_{params.sr_folio}"
logger.info("Configuración de generación:")
logger.info(f"- Número de registros: {num_records}")
logger.info(f"- Tipo subcuenta 15: {tipo_subcta15}")
logger.info(f"- Tipo subcuenta 16: {tipo_subcta16}")
logger.info(f"- Tabla de salida: {output_table_name}")

# Generar datos dummy según esquema DataStage
df_dummy = generate_dummy_data_for_datastage(
    spark=spark,
    num_records=num_records,
    tipo_subcta15=tipo_subcta15,
    tipo_subcta16=tipo_subcta16,
)

logger.info(f"Guardando datos dummy en tabla Delta: {output_table_name}")
db.write_delta(output_table_name, df_dummy, "overwrite")

df_verification = db.read_delta(output_table_name)
logger.info(f"Verificación: {df_verification.count()} registros cargados")
display(df_verification.limit(5))

logger.info("=== Análisis de distribución de datos ===")
logger.info("--- Distribución por tipo de subcuenta 97 ---")
df_verification.groupBy("FCN_ID_TIPO_SUBCTA_97").count().orderBy(
    "FCN_ID_TIPO_SUBCTA_97"
).show()

logger.info("--- Distribución por tipo de subcuenta 92 ---")
df_verification.groupBy("FCN_ID_TIPO_SUBCTA_92").count().orderBy(
    "FCN_ID_TIPO_SUBCTA_92"
).show()

logger.info("--- Distribución de indicadores de saldo disponible ---")
df_verification.groupBy("FTN_IND_SDO_DISP_VIV97").count().orderBy(
    "FTN_IND_SDO_DISP_VIV97"
).show()
df_verification.groupBy("FTN_IND_SDO_DISP_92").count().orderBy(
    "FTN_IND_SDO_DISP_92"
).show()

logger.info("--- Distribución por estatus ---")
df_verification.groupBy("FCN_ESTATUS").count().orderBy("FCN_ESTATUS").show()

logger.info("=== Esquema de datos generados ===")
df_verification.printSchema()

logger.info("=== Estadísticas básicas ===")
logger.info(f"Total de registros: {df_verification.count()}")
logger.info(f"Columnas: {len(df_verification.columns)}")

# Mostrar estadísticas de campos numéricos
logger.info("--- Estadísticas de campos numéricos ---")
df_verification.describe().show()

# Ejemplo de consulta usando la vista
logger.info("--- Ejemplo de consulta SQL ---")
spark.sql(
    f"""
    SELECT 
        FCN_ID_TIPO_SUBCTA_97,
        FCN_ID_TIPO_SUBCTA_92,
        COUNT(*) as cantidad
    FROM {SETTINGS.GENERAL.CATALOG}.{SETTINGS.GENERAL.SCHEMA}.{output_table_name}
    GROUP BY FCN_ID_TIPO_SUBCTA_97, FCN_ID_TIPO_SUBCTA_92
    ORDER BY FCN_ID_TIPO_SUBCTA_97, FCN_ID_TIPO_SUBCTA_92
"""
).show()

logger.info("=== Proceso completado exitosamente ===")
logger.info(f"Tabla Delta creada: {output_table_name}")
logger.info(f"Registros generados: {df_verification.count()}")
