import concurrent.futures
import json
import logging
import os
import random
import threading
import time
import uuid
from typing import Optional

import requests
from databricks.sdk.runtime import dbutils
from logger import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import MapType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException
from settings import SETTINGS


class DBXConnectionManager:
    """
    ⚡ OPTIMIZADO PARA CIENTOS DE NOTEBOOKS CONCURRENTES

    Gestor de conexiones con Databricks optimizado para:
    - Escritura/lectura ultra-rápida en Delta Lake
    - Concurrencia masiva (100+ notebooks)
    - Configuraciones runtime inteligentes
    - Patrón Singleton para evitar re-inicializaciones
    """

    # Singleton para evitar múltiples inicializaciones
    _instance: Optional["DBXConnectionManager"] = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls, *args, **kwargs):
        """
        Implementa patrón Singleton thread-safe para cientos de notebooks
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info(
                        "🏗️ Creando PRIMERA instancia DBXConnectionManager (Singleton)"
                    )
                    cls._instance = super(DBXConnectionManager, cls).__new__(cls)
                else:
                    logger.info(
                        "🔄 Reutilizando instancia existente DBXConnectionManager"
                    )
        else:
            logger.info("♻️ Instancia DBXConnectionManager ya existe - reutilizando")

        return cls._instance

    def __init__(
        self,
        catalog=SETTINGS.GENERAL.CATALOG,
        schema=SETTINGS.GENERAL.SCHEMA,
        table="default_connection",
    ):
        """
        Constructor optimizado para cientos de notebooks concurrentes.
        Solo se ejecuta UNA VEZ gracias al patrón Singleton.
        """

        # Evitar re-inicialización si ya está inicializado
        if self._initialized:
            logger.info(
                "✅ DBXConnectionManager ya inicializado - saltando configuración"
            )
            return

        with self._lock:
            if self._initialized:
                return

            logger.info("🚀 INICIALIZANDO DBXConnectionManager...")
            start_time = time.time()

            # 🚀 Configuración ESTABLE y PROBADA para Azure Databricks
            # ✅ Optimizada específicamente para escritura Delta rápida y concurrencia
            self.spark = (
                SparkSession.builder
                # 🔹 CONFIGURACIONES BÁSICAS DATABRICKS (OBLIGATORIAS)
                .config("spark.databricks.service.server.enabled", "true")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config(
                    "spark.databricks.connect.enabled", "false"
                )  # CRÍTICO: Deshabilitar Spark Connect
                # 🔹 ADAPTIVE QUERY EXECUTION (CRÍTICO PARA CIENTOS DE NOTEBOOKS)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .config(
                    "spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB"
                )  # Mínimo por partición
                .config(
                    "spark.sql.adaptive.coalescePartitions.initialPartitionNum", "auto"
                )  # Databricks decide inicial
                # 🔹 CONFIGURACIONES DE PARTICIONES (DATABRICKS DECIDE AUTOMÁTICAMENTE)
                .config(
                    "spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB"
                )  # Tamaño objetivo por partición
                .config(
                    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "256MB"
                )  # Target size post-shuffle
                .config(
                    "spark.sql.files.maxPartitionBytes", "256MB"
                )  # Balance perfecto
                .config("spark.sql.files.openCostInBytes", "4MB")  # Estándar probado
                .config(
                    "spark.sql.autoBroadcastJoinThreshold", "128MB"
                )  # Conservador y estable
                # 🔹 DELTA LAKE CONFIGURACIONES ESENCIALES (PROBADAS)
                .config(
                    "spark.databricks.delta.optimizeWrite.enabled", "true"
                )  # Escritura optimizada
                .config(
                    "spark.databricks.delta.autoCompact.enabled", "true"
                )  # Compactación automática
                .config(
                    "spark.databricks.delta.stats.skipping", "true"
                )  # Data skipping
                .config(
                    "spark.databricks.delta.schema.autoMerge.enabled", "false"
                )  # Evita problemas de esquema
                .config(
                    "spark.sql.sources.partitionOverwriteMode", "dynamic"
                )  # Overwrite dinámico
                # 🔹 CONFIGURACIONES CRÍTICAS PARA CIENTOS DE NOTEBOOKS
                .config(
                    "spark.databricks.delta.write.txnAppId.enabled", "true"
                )  # CRÍTICO: IDs únicos de transacción
                .config("spark.scheduler.mode", "FAIR")  # CRÍTICO: Scheduler justo
                .config(
                    "spark.databricks.delta.concurrentTransactionRetries", "5"
                )  # Reintentos para conflictos
                .config(
                    "spark.databricks.delta.concurrentTransactionBackoff", "1000"
                )  # Backoff de 1s
                # 🔹 COMPRESIÓN Y FORMATO (ESTABLES)
                .config(
                    "spark.sql.parquet.compression.codec", "snappy"
                )  # Snappy es más estable que zstd
                .config(
                    "spark.sql.parquet.filterPushdown", "true"
                )  # Push-down de filtros
                .config(
                    "spark.sql.parquet.vectorizedReader.enabled", "true"
                )  # Lectura vectorizada
                # 🔹 PHOTON (SI ESTÁ DISPONIBLE EN TU CLUSTER)
                .config("spark.databricks.photon.enabled", "true")  # Photon básico
                # 🔹 CACHE I/O CONSERVADOR PARA MÚLTIPLES NOTEBOOKS
                .config("spark.databricks.io.cache.enabled", "true")  # Cache I/O básico
                .config(
                    "spark.databricks.io.cache.maxDiskUsage", "15GB"
                )  # REDUCIDO para cientos de notebooks
                # 🔹 TIMEOUTS ESTABLES PARA ALTA CONCURRENCIA
                .config("spark.sql.broadcastTimeout", "900")  # 15 minutos (más tiempo)
                .config("spark.network.timeout", "900s")  # 15 minutos (más tiempo)
                .config("spark.rpc.retry.wait", "3s")  # Espera entre reintentos
                .config("spark.rpc.numRetries", "3")  # Reintentos de red
                # 🔹 SERIALIZACIÓN ESTABLE
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .config(
                    "spark.sql.execution.arrow.pyspark.enabled", "true"
                )  # Arrow para Python
                .getOrCreate()
            )

            # ⚡ APLICAR CONFIGURACIONES RUNTIME CRÍTICAS (¡PROBADO QUE FUNCIONA!)
            logger.info("🔧 Aplicando configuraciones runtime optimizadas...")

            # Configuraciones críticas que SÍ funcionan en Spark Connect
            critical_runtime_configs = {
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
                "spark.sql.files.maxPartitionBytes": "256MB",
                "spark.sql.files.openCostInBytes": "4MB",
                "spark.sql.autoBroadcastJoinThreshold": "128MB",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
            }

            applied_count = 0
            for config_key, config_value in critical_runtime_configs.items():
                try:
                    self.spark.conf.set(config_key, config_value)
                    # Verificar que se aplicó
                    actual_value = self.spark.conf.get(config_key)
                    if actual_value == config_value:
                        applied_count += 1
                        logger.info(f"✅ {config_key}: {config_value}")
                    else:
                        logger.warning(
                            f"⚠️ {config_key}: esperado={config_value}, real={actual_value}"
                        )
                except Exception as e:
                    logger.warning(f"❌ {config_key}: {e}")

            success_rate = (applied_count / len(critical_runtime_configs)) * 100
            logger.info(
                f"🎯 CONFIGURACIONES RUNTIME: {applied_count}/{len(critical_runtime_configs)} ({success_rate:.1f}%)"
            )

            if success_rate > 75:
                logger.info(
                    "✅ EXCELENTE: Configuraciones runtime aplicadas correctamente"
                )
            else:
                logger.warning("⚠️ Algunas configuraciones runtime no se aplicaron")

            self.catalog = catalog
            self.schema = schema
            self.table = table
            self.spark.sql(f"USE CATALOG {catalog}")
            self.spark.sql(f"USE {schema}")

            self.BASE_URL = self._ensure_dict(SETTINGS.CONN.CONN_OPTIONS)
            self.CONN_ADITIONAL_OPTIONS = self._ensure_dict(
                SETTINGS.CONN.CONN_ADITIONAL_OPTIONS
            )

            # self._ensure_table_exists()  # Comentado para evitar consultas a default_connection

            # Marcar como inicializado al final
            self._initialized = True
            init_time = time.time() - start_time
            logger.info(
                f"✅ DBXConnectionManager inicializado en {init_time:.2f}s - LISTO para multiples notebooks"
            )

    @classmethod
    def get_instance(cls, **kwargs):
        """
        Método alternativo para obtener la instancia singleton
        Recomendado para uso en cientos de notebooks
        """
        if cls._instance is None:
            logger.info("🏗️ Creando instancia DBXConnectionManager via get_instance()")
            return cls(**kwargs)
        else:
            logger.info("♻️ Reutilizando instancia DBXConnectionManager existente")
            return cls._instance

    @classmethod
    def reset_singleton(cls):
        """
        Método para resetear el singleton (solo para testing/debugging)
        """
        with cls._lock:
            logger.warning("🔄 RESETEANDO Singleton DBXConnectionManager")
            cls._instance = None
            cls._initialized = False

    def validate_cluster_configuration(self):
        """
        Valida que la configuración del cluster sea compatible y estable.
        Detecta posibles problemas antes de que causen errores.

        Returns:
            dict: Resultado de la validación con warnings y recomendaciones.
        """
        validation_result = {
            "status": "success",
            "warnings": [],
            "recommendations": [],
            "cluster_info": {},
        }

        try:
            # 🔹 Información básica del cluster
            sc = self.spark.sparkContext
            spark_conf = self.spark.conf

            validation_result["cluster_info"] = {
                "spark_version": sc.version,
                "app_id": sc.applicationId,
                "default_parallelism": sc.defaultParallelism,
                "total_executor_cores": sc._jsc.sc().getExecutorMemoryStatus().size(),
            }

            # 🔹 Validar configuraciones críticas
            # Photon
            photon_enabled = spark_conf.get("spark.databricks.photon.enabled", "false")
            if photon_enabled.lower() == "false":
                validation_result["warnings"].append(
                    "Photon no está habilitado - el rendimiento podría ser mejor"
                )
                validation_result["recommendations"].append(
                    "Considera habilitar Photon en la configuración del cluster"
                )

            # Delta Lake extensions
            extensions = spark_conf.get("spark.sql.extensions", "")
            if "DeltaSparkSessionExtension" not in extensions:
                validation_result["warnings"].append(
                    "Delta Lake extensions no están configuradas correctamente"
                )
                validation_result["status"] = "warning"

            # Adaptive Query Execution
            aqe_enabled = spark_conf.get("spark.sql.adaptive.enabled", "false")
            if aqe_enabled.lower() == "false":
                validation_result["warnings"].append(
                    "Adaptive Query Execution (AQE) no está habilitado"
                )
                validation_result["recommendations"].append(
                    "AQE es crítico para el rendimiento en escrituras Delta"
                )

            # Particiones
            shuffle_partitions = int(
                spark_conf.get("spark.sql.shuffle.partitions", "200")
            )
            cores = sc.defaultParallelism
            if shuffle_partitions < cores * 2:
                validation_result["warnings"].append(
                    f"Pocas particiones de shuffle ({shuffle_partitions}) para {cores} cores"
                )
                validation_result["recommendations"].append(
                    f"Considera usar al menos {cores * 4} particiones de shuffle"
                )
            elif shuffle_partitions > cores * 20:
                validation_result["warnings"].append(
                    f"Demasiadas particiones de shuffle ({shuffle_partitions}) para {cores} cores"
                )
                validation_result["recommendations"].append(
                    f"Considera reducir a máximo {cores * 10} particiones de shuffle"
                )

            # Scheduler mode
            scheduler_mode = spark_conf.get("spark.scheduler.mode", "FIFO")
            if (
                scheduler_mode != "FAIR"
                and validation_result["cluster_info"]["total_executor_cores"] > 1
            ):
                validation_result["recommendations"].append(
                    "Para múltiples notebooks, considera usar scheduler FAIR"
                )

            # Cache I/O
            io_cache = spark_conf.get("spark.databricks.io.cache.enabled", "false")
            if io_cache.lower() == "false":
                validation_result["recommendations"].append(
                    "Cache I/O deshabilitado - considera habilitarlo para mejor rendimiento de lectura"
                )

            # 🔹 Validaciones específicas de Delta Lake
            optimize_write = spark_conf.get(
                "spark.databricks.delta.optimizeWrite.enabled", "false"
            )
            if optimize_write.lower() == "false":
                validation_result["warnings"].append(
                    "Delta optimizeWrite no está habilitado - escrituras serán más lentas"
                )
                validation_result["status"] = "warning"

            # 🔹 Resumen final
            if len(validation_result["warnings"]) == 0:
                validation_result["message"] = (
                    "✅ Configuración del cluster es óptima para escritura Delta"
                )
            elif validation_result["status"] == "warning":
                validation_result["message"] = (
                    f"⚠️ Configuración funcional pero con {len(validation_result['warnings'])} warnings"
                )
            else:
                validation_result["message"] = (
                    f"ℹ️ Configuración estable con {len(validation_result['recommendations'])} recomendaciones"
                )

            logger.info(f"📋 Validación completada: {validation_result['message']}")
            return validation_result

        except Exception as e:
            validation_result["status"] = "error"
            validation_result["message"] = f"❌ Error en validación: {str(e)}"
            logger.error(f"Error validando configuración del cluster: {e}")
            return validation_result

    def check_adaptive_partitioning_status(self):
        """
        Verifica que las configuraciones de particionado automático estén funcionando correctamente
        para cientos de notebooks ejecutándose simultáneamente.

        Returns:
            dict: Estado de las configuraciones de particionado automático
        """
        try:
            spark_conf = self.spark.conf

            # Función helper para obtener configuraciones de manera segura
            def safe_get_config(key, default="not_configured"):
                try:
                    value = spark_conf.get(key)
                    return value if value is not None else default
                except Exception:
                    return default

            # Obtener configuraciones de manera segura
            status = {
                "adaptive_enabled": safe_get_config(
                    "spark.sql.adaptive.enabled", "false"
                ).lower()
                == "true",
                "auto_coalesce": safe_get_config(
                    "spark.sql.adaptive.coalescePartitions.enabled", "false"
                ).lower()
                == "true",
                "scheduler_mode": safe_get_config("spark.scheduler.mode", "FIFO"),
                "shuffle_partitions": safe_get_config(
                    "spark.sql.shuffle.partitions", "default"
                ),
            }

            # Verificar configuraciones específicas de bytes de manera segura
            byte_configs = {
                "advisory_partition_size": "spark.sql.adaptive.advisoryPartitionSizeInBytes",
                "target_post_shuffle_size": "spark.sql.adaptive.shuffle.targetPostShuffleInputSize",
                "min_partition_size": "spark.sql.adaptive.coalescePartitions.minPartitionSize",
            }

            for config_name, config_key in byte_configs.items():
                try:
                    # Intentar obtener sin valor por defecto para evitar parsing
                    value = spark_conf.get(config_key)
                    status[config_name] = (
                        value if value is not None else "not_configured"
                    )
                except Exception:
                    status[config_name] = "not_configured"

            # Configuración especial para initialPartitionNum
            try:
                initial_num = spark_conf.get(
                    "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
                )
                status["initial_partition_num"] = (
                    initial_num if initial_num is not None else "not_configured"
                )
            except Exception:
                status["initial_partition_num"] = "not_configured"

            # Evaluar si está optimizado para cientos de notebooks
            is_adaptive_enabled = status["adaptive_enabled"] and status["auto_coalesce"]
            has_byte_configs = status.get(
                "advisory_partition_size", "not_configured"
            ) not in ["not_configured", None] or status.get(
                "target_post_shuffle_size", "not_configured"
            ) not in [
                "not_configured",
                None,
            ]
            is_fair_scheduler = status["scheduler_mode"] == "FAIR"

            if is_adaptive_enabled and is_fair_scheduler:
                if has_byte_configs:
                    if (
                        status["shuffle_partitions"] == "default"
                        or "auto"
                        in str(status.get("initial_partition_num", "")).lower()
                    ):
                        message = "✅ ÓPTIMO: Databricks decidirá particiones automáticamente para cada notebook"
                        recommendation = (
                            "Configuración ideal para cientos de notebooks concurrentes"
                        )
                        status_level = "optimal"
                    else:
                        message = "⚠️ BUENO: AQE habilitado con configuraciones de bytes, pero particiones pueden estar fijas"
                        recommendation = (
                            "Configuración buena, Databricks optimizará dinámicamente"
                        )
                        status_level = "good"
                else:
                    message = "⚠️ BUENO: AQE habilitado pero sin configuraciones específicas de tamaño"
                    recommendation = (
                        "Funciona bien, AQE manejará las particiones automáticamente"
                    )
                    status_level = "good"
            elif is_adaptive_enabled:
                message = "⚠️ MEJORABLE: AQE habilitado pero scheduler no es FAIR"
                recommendation = (
                    "Para cientos de notebooks, considera usar scheduler FAIR"
                )
                status_level = "needs_improvement"
            else:
                message = "❌ NO ÓPTIMO: AQE no está habilitado completamente"
                recommendation = (
                    "Habilita AQE y configuraciones automáticas para mejor rendimiento"
                )
                status_level = "needs_improvement"

            return {
                "status": status_level,
                "message": message,
                "recommendation": recommendation,
                "details": status,
            }

        except Exception as e:
            return {
                "status": "error",
                "message": f"❌ Error verificando configuraciones: {str(e)}",
                "recommendation": "Revisa la configuración del cluster",
                "details": {},
            }

    def execute_oci_dml(self, statement, async_mode=False):
        """
        Ejecuta un notebook en Databricks para operaciones DML en OCI (MERGE, INSERT, DELETE)
        y guarda logs en una tabla Delta. Puede ejecutarse en segundo plano si se especifica.

        Args:
            statement (str): Consulta SQL a ejecutar.
            async_mode (bool): Si es True, crea un job en Databricks en lugar de ejecutar en thread.

        Returns:
            dict | dict: Resultado JSON o información del job si es asíncrono.

        Raises:
            RuntimeError: Si el notebook devuelve un error.
        """
        import uuid
        from datetime import datetime

        def log_result_to_delta(statement_text, result_data):
            log_data = [
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "statement": statement_text,
                    "status": result_data.get("status", "unknown"),
                    "message": result_data.get("message", ""),
                    "raw_result": json.dumps(result_data),
                }
            ]

            schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("timestamp", StringType(), False),
                    StructField("statement", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("message", StringType(), True),
                    StructField("raw_result", StringType(), True),
                ]
            )

            df_log = self.spark.createDataFrame(log_data, schema)
            full_table = f"{self.catalog}.{self.schema}.oci_dml_logs"

            try:
                self.spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {full_table} (id STRING, timestamp STRING, statement STRING, status STRING, message STRING, raw_result STRING) USING DELTA"
                )
            except Exception as e:
                logger.warning(
                    f"No se pudo crear la tabla de logs (puede que ya exista): {e}"
                )

            # df_log.write.format("delta").mode("append").saveAsTable(full_table)
            # logger.info(f"Log DML registrado en Delta: {full_table}")
            pass  # Log DML deshabilitado para mejor rendimiento

        try:
            conn_user = str(SETTINGS.GENERAL.CONN_USER)
            conn_key = str(SETTINGS.GENERAL.CONN_KEY)
            conn_scope = str(SETTINGS.GENERAL.SCOPE)

            conn_url = (
                SETTINGS.CONN.CONN_OPTIONS.get("url", "")
                if isinstance(SETTINGS.CONN.CONN_OPTIONS, dict)
                else str(SETTINGS.CONN.CONN_OPTIONS)
            )

            current_dir = os.path.dirname(os.path.abspath(__file__))
            notebook_path = (
                current_dir.replace("/Workspace", "").replace(os.sep, "/")
                + "/OracleExecutor"
            )

            logger.info(f"Ejecutando notebook en Databricks: {notebook_path}")

            def run_notebook():
                try:
                    result_str = dbutils.notebook.run(
                        notebook_path,
                        timeout_seconds=7200,
                        arguments={
                            "conn_user": conn_user,
                            "conn_key": conn_key,
                            "conn_url": conn_url,
                            "conn_scope": conn_scope,
                            "statement": str(statement),
                        },
                    )
                    result = json.loads(result_str)

                    if result.get("status") == "error":
                        raise RuntimeError(
                            f"Error en la base de datos: {result.get('message', 'Error desconocido')}"
                        )

                    logger.info(f"Ejecución exitosa: {result}")
                    return result

                except Exception as e:
                    logger.error(f"Error ejecutando el notebook: {e}")
                    raise

            if async_mode:
                # 🔹 MODO ASÍNCRONO: Crear o usar job existente en Databricks
                try:
                    # Obtener token del contexto del notebook
                    token = (
                        dbutils.notebook.entry_point.getDbutils()
                        .notebook()
                        .getContext()
                        .apiToken()
                        .getOrElse(None)
                    )

                    if not token:
                        raise RuntimeError(
                            "No se pudo obtener el token de autenticación"
                        )

                    headers = {
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    }

                    # 🔹 Configuración del job desde SETTINGS
                    job_name = SETTINGS.GENERAL.OCI_DML_JOB_NAME
                    databricks_instance = SETTINGS.GENERAL.DATABRICKS_INSTANCE

                    # 🔹 1) Verificar si el job ya existe
                    list_jobs_url = f"{databricks_instance}/api/2.1/jobs/list"
                    list_response = requests.get(list_jobs_url, headers=headers)

                    existing_job_id = None
                    if list_response.status_code == 200:
                        jobs = list_response.json().get("jobs", [])
                        for job in jobs:
                            if job.get("settings", {}).get("name") == job_name:
                                existing_job_id = job["job_id"]
                                logger.info(
                                    f"✅ Job existente encontrado: {job_name} (ID: {existing_job_id})"
                                )
                                break

                                # 🔹 2) Crear el job si no existe
                    if not existing_job_id:
                        create_url = f"{databricks_instance}/api/2.1/jobs/create"

                        job_settings = {
                            "name": job_name,
                            "max_concurrent_runs": SETTINGS.GENERAL.OCI_DML_MAX_CONCURRENT_RUNS,  # 🔹 Concurrencia configurable
                            "tasks": [
                                {
                                    "task_key": "oci_dml_execution",
                                    "notebook_task": {
                                        "notebook_path": notebook_path,
                                        "base_parameters": {
                                            "conn_user": conn_user,
                                            "conn_key": conn_key,
                                            "conn_url": conn_url,
                                            "conn_scope": conn_scope,
                                            # 🔹 NO incluir statement aquí - se pasa en cada ejecución
                                        },
                                    },
                                    "existing_cluster_id": SETTINGS.GENERAL.DATABRICKS_CLUSTER_ID,  # 🔹 Cluster desde configuración
                                    "timeout_seconds": SETTINGS.GENERAL.OCI_DML_TIMEOUT_SECONDS,
                                    "max_retries": SETTINGS.GENERAL.OCI_DML_MAX_RETRIES,
                                    "min_retry_interval_millis": SETTINGS.GENERAL.OCI_DML_RETRY_INTERVAL_MS,
                                }
                            ],
                            "email_notifications": {"on_success": [], "on_failure": []},
                            "access_control_list": [  # 🔹 Permisos desde configuración
                                {
                                    "group_name": SETTINGS.GENERAL.OCI_DML_JOB_GROUP_NAME,
                                    "permission_level": SETTINGS.GENERAL.OCI_DML_JOB_PERMISSION_LEVEL,
                                }
                            ],
                        }

                        create_response = requests.post(
                            create_url, headers=headers, data=json.dumps(job_settings)
                        )
                        if create_response.status_code != 200:
                            raise RuntimeError(
                                f"Error al crear el job ({create_response.status_code}): {create_response.text}"
                            )

                        existing_job_id = create_response.json()["job_id"]
                        logger.info(
                            f"✅ Job creado exitosamente: {job_name} (ID: {existing_job_id})"
                        )

                    # 🔹 3) Ejecutar el job inmediatamente
                    run_now_url = f"{databricks_instance}/api/2.1/jobs/run-now"
                    run_body = {
                        "job_id": existing_job_id,
                        "notebook_params": {
                            "conn_user": conn_user,
                            "conn_key": conn_key,
                            "conn_url": conn_url,
                            "conn_scope": conn_scope,
                            "statement": str(statement),
                        },
                    }

                    run_response = requests.post(
                        run_now_url, headers=headers, data=json.dumps(run_body)
                    )
                    if run_response.status_code != 200:
                        raise RuntimeError(
                            f"Error al lanzar el job ({run_response.status_code}): {run_response.text}"
                        )

                    run_id = run_response.json()["run_id"]
                    logger.info(f"🚀 Job lanzado exitosamente: run_id={run_id}")

                    # 🔹 4) Retornar información del job ejecutándose
                    job_info = {
                        "status": "job_launched",
                        "message": f"Job '{job_name}' ejecutándose en segundo plano",
                        "job_id": existing_job_id,
                        "run_id": run_id,
                        "job_name": job_name,
                        "databricks_url": f"{databricks_instance}/#job/{existing_job_id}/run/{run_id}",
                        "timestamp": datetime.utcnow().isoformat(),
                        "statement_preview": (
                            str(statement)[:100] + "..."
                            if len(str(statement)) > 100
                            else str(statement)
                        ),
                    }

                    logger.info(f"✅ Job asíncrono lanzado: {job_info}")
                    return job_info

                except Exception as e:
                    logger.error(f"❌ Error al crear/ejecutar job asíncrono: {e}")
                    # Fallback: ejecutar de manera síncrona si falla la creación del job
                    logger.warning(
                        "⚠️ Fallback a ejecución síncrona debido a error en job"
                    )
                    return run_notebook()
            else:
                # 🔹 MODO SÍNCRONO: Ejecución normal
                result = run_notebook()
                # log_result_to_delta(statement, result)  # Deshabilitado para mejor rendimiento
                return result

        except Exception as e:
            logger.error(f"Error en execute_oci_dml: {e}")
            raise

    def check_job_execution_status(self, execution):
        """
        Verifica el status de una ejecución de job usando el parámetro execution completo.

        Args:
            execution (dict): Diccionario retornado por execute_oci_dml en modo asíncrono

        Returns:
            dict: Estado detallado de la ejecución del job
        """
        try:
            # Extraer job_id y run_id del parámetro execution
            job_id = execution.get("job_id")
            run_id = execution.get("run_id")

            if not job_id or not run_id:
                return {
                    "status": "error",
                    "message": "Parámetro execution no contiene job_id o run_id válidos",
                    "execution_data": execution,
                }

            # Obtener token del contexto del notebook
            token = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .apiToken()
                .getOrElse(None)
            )

            if not token:
                return {
                    "status": "error",
                    "message": "No se pudo obtener el token de autenticación",
                }

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            # Consultar estado del job
            url = f"{SETTINGS.GENERAL.DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get"
            params = {"run_id": run_id}

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                run_info = response.json()
                state = run_info.get("state", {})

                # Extraer información del estado
                life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
                result_state = state.get("result_state")
                start_time = run_info.get("start_time")
                end_time = run_info.get("end_time")

                # Determinar si finalizó
                is_finished = life_cycle_state in [
                    "TERMINATED",
                    "SKIPPED",
                    "INTERNAL_ERROR",
                ]

                # Construir respuesta
                status_info = {
                    "status": "success",
                    "is_finished": is_finished,
                    "life_cycle_state": life_cycle_state,
                    "result_state": result_state,
                    "start_time": start_time,
                    "end_time": end_time,
                    "job_id": job_id,
                    "run_id": run_id,
                    "execution_url": execution.get("databricks_url", ""),
                    "execution_data": execution,
                }

                # 🔹 Si el job terminó exitosamente, obtener el resultado del notebook
                if is_finished and result_state == "SUCCESS":
                    try:
                        # 🔹 Esperar un poco más para asegurar que el resultado esté disponible
                        import time

                        time.sleep(2)

                        # 🔹 Consultar el resultado de la ejecución usando get-output-by-task
                        # Primero obtener información del run para identificar el task
                        run_info_url = f"{SETTINGS.GENERAL.DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get"
                        run_info_response = requests.get(
                            run_info_url, headers=headers, params={"run_id": run_id}
                        )

                        if run_info_response.status_code == 200:
                            run_info = run_info_response.json()
                            tasks = run_info.get("tasks", [])

                            if tasks:
                                # Usar el primer task (nuestro notebook)
                                task_run_id = tasks[0].get("run_id")
                                task_key = tasks[0].get("task_key", "oci_dml_execution")

                                logger.info(
                                    f"🔍 Task encontrado: {task_key} con run_id: {task_run_id}"
                                )

                                # 🔹 Intentar obtener resultado usando el run_id del task individual
                                if task_run_id:
                                    result_url = f"{SETTINGS.GENERAL.DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get-output"
                                    result_params = {"run_id": task_run_id}

                                    logger.info(
                                        f"🔍 Consultando resultado usando run_id del task: {task_run_id}"
                                    )
                                    result_response = requests.get(
                                        result_url,
                                        headers=headers,
                                        params=result_params,
                                    )

                                    logger.info(
                                        f"📡 Respuesta API resultado del task: {result_response.status_code}"
                                    )

                                    if result_response.status_code == 200:
                                        result_data = result_response.json()
                                        notebook_output = result_data.get(
                                            "notebook_output", {}
                                        )

                                        if notebook_output:
                                            # Extraer el resultado del notebook
                                            result_text = notebook_output.get(
                                                "result", ""
                                            )
                                            status_info["notebook_result"] = result_text

                                            # Intentar parsear el JSON del resultado
                                            try:
                                                import json

                                                parsed_result = json.loads(result_text)
                                                status_info["parsed_result"] = (
                                                    parsed_result
                                                )
                                                # Solo mostrar statement en caso de error
                                                status_info["affected_rows"] = (
                                                    parsed_result.get("affected_rows")
                                                )
                                                status_info["execution_time_ms"] = (
                                                    parsed_result.get(
                                                        "execution_time_ms"
                                                    )
                                                )
                                                status_info["notebook_status"] = (
                                                    parsed_result.get("status")
                                                )
                                                status_info["notebook_message"] = (
                                                    parsed_result.get("message")
                                                )

                                                # Solo mostrar resultado en caso de error
                                            except json.JSONDecodeError as json_err:
                                                # Si no es JSON válido, guardar como texto
                                                logger.warning(
                                                    f"⚠️ Error parseando JSON: {json_err}"
                                                )
                                                status_info["parsed_result"] = None
                                                status_info["notebook_message"] = (
                                                    result_text
                                                )
                                        else:
                                            logger.warning(
                                                f"⚠️ No se encontró notebook_output en la respuesta del task"
                                            )
                                            status_info["notebook_result"] = (
                                                "No se encontró resultado del notebook en el task"
                                            )
                                            status_info["parsed_result"] = None
                                    else:
                                        error_detail = f"Error {result_response.status_code}: {result_response.text}"
                                        logger.error(f"❌ {error_detail}")
                                        status_info["notebook_result"] = error_detail
                                        status_info["parsed_result"] = None

                                        # 🔹 Intentar obtener más información del error
                                        try:
                                            error_data = result_response.json()
                                            status_info["error_details"] = error_data
                                        except:
                                            status_info["error_details"] = (
                                                result_response.text
                                            )
                                else:
                                    logger.warning(f"⚠️ No se encontró run_id del task")
                                    status_info["notebook_result"] = (
                                        "No se encontró run_id del task"
                                    )
                                    status_info["parsed_result"] = None
                            else:
                                logger.warning(f"⚠️ No se encontraron tasks en el run")
                                status_info["notebook_result"] = (
                                    "No se encontraron tasks en el run"
                                )
                                status_info["parsed_result"] = None
                        else:
                            error_detail = f"Error obteniendo información del run: {run_info_response.status_code}"
                            logger.error(f"❌ {error_detail}")
                            status_info["notebook_result"] = error_detail
                            status_info["parsed_result"] = None

                    except Exception as e:
                        error_msg = f"Error obteniendo resultado del notebook: {str(e)}"
                        logger.error(f"❌ {error_msg}")
                        status_info["notebook_result"] = error_msg
                        status_info["parsed_result"] = None
                else:
                    # Si no terminó o falló, no hay resultado del notebook
                    status_info["notebook_result"] = None
                    status_info["parsed_result"] = None

                # Verificar si el notebook reportó un error (incluso si result_state es SUCCESS)
                notebook_status = status_info.get("notebook_status")
                notebook_message = status_info.get("notebook_message", "")

                # Si el notebook reportó un error, cambiar el result_state
                if (
                    is_finished
                    and result_state == "SUCCESS"
                    and notebook_status == "error"
                ):
                    result_state = "FAILED"
                    status_info["result_state"] = "FAILED"
                    logger.warning(
                        f"⚠️ Job marcado como FALLIDO debido a error en notebook: {notebook_message}"
                    )

                # Agregar mensaje descriptivo
                if is_finished:
                    if result_state == "SUCCESS":
                        status_info["message"] = "✅ Job ejecutado exitosamente"
                    elif result_state == "FAILED":
                        if notebook_status == "error":
                            status_info["message"] = (
                                f"❌ Job falló en la ejecución: {notebook_message}"
                            )
                        else:
                            status_info["message"] = "❌ Job falló en la ejecución"
                    elif result_state == "CANCELLED":
                        status_info["message"] = "⏹️ Job cancelado"
                    else:
                        status_info["message"] = (
                            f"⚠️ Job terminado con estado: {result_state}"
                        )
                else:
                    status_info["message"] = f"🔄 Job en ejecución: {life_cycle_state}"

                # Solo mostrar logs detallados en caso de error
                if result_state == "FAILED":
                    logger.error(
                        f"📊 Estado del job {run_id}: {status_info['message']}"
                    )
                return status_info

            else:
                error_msg = f"Error al consultar estado del job: {response.status_code}"
                logger.error(f"{error_msg} - {response.text}")
                return {
                    "status": "error",
                    "message": error_msg,
                    "response_text": response.text,
                }

        except Exception as e:
            error_msg = f"Error al verificar estado del job: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "execution_data": execution,
            }

    def execute_multiple_statements_parallel(
        self, statements, check_interval=30, max_wait_time=7200
    ):
        """
        Ejecuta múltiples statements SQL en paralelo y espera a que todos terminen.

        Args:
            statements (list): Lista de statements SQL a ejecutar
            check_interval (int): Intervalo en segundos para verificar el estado (default: 30)
            max_wait_time (int): Tiempo máximo de espera en segundos (default: 7200 = 2 horas)

        Returns:
            dict: Resumen de la ejecución con estadísticas y resultados
        """
        import time

        logger.info(
            f"🚀 Iniciando ejecución paralela de {len(statements)} statements..."
        )

        # Ejecutar todos los jobs en paralelo
        executions = []
        for i, statement in enumerate(statements):
            logger.info(f"🚀 Lanzando job {i+1}/{len(statements)}...")
            execution = self.execute_oci_dml(statement=statement, async_mode=True)
            executions.append(
                {
                    "index": i + 1,
                    "statement": statement,
                    "execution": execution,
                    "completed": False,
                }
            )

        logger.info(
            f"🎯 Todos los jobs lanzados. Monitoreando {len(executions)} ejecuciones..."
        )

        # Monitorear todos los jobs hasta que terminen
        start_time = time.time()
        successful_jobs = 0
        failed_jobs = 0

        while True:
            completed_jobs = 0

            for job_info in executions:
                if job_info["completed"]:
                    completed_jobs += 1
                else:
                    status = self.check_job_execution_status(job_info["execution"])

                    if status["is_finished"]:
                        if status["result_state"] == "SUCCESS":
                            logger.info(
                                f"✅ Job {job_info['index']} completado exitosamente"
                            )
                            job_info["completed"] = True
                            job_info["status"] = "SUCCESS"
                            job_info["result"] = status
                            successful_jobs += 1
                            completed_jobs += 1
                        else:
                            logger.error(
                                f"❌ Job {job_info['index']} FALLÓ: {status['result_state']}"
                            )
                            logger.error(
                                f"   - Statement: {job_info['statement'][:100]}..."
                            )
                            if status.get("notebook_message"):
                                logger.error(
                                    f"   - Error: {status['notebook_message']}"
                                )
                            job_info["completed"] = True
                            job_info["status"] = "FAILED"
                            job_info["result"] = status
                            failed_jobs += 1
                            completed_jobs += 1
                    else:
                        # Solo mostrar progreso cada cierto tiempo para reducir verbosidad
                        if completed_jobs == 0:  # Solo mostrar en la primera iteración
                            logger.info(f"🔄 Jobs en ejecución...")

            # Verificar si todos terminaron
            if completed_jobs >= len(executions):
                break

            # Verificar timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > max_wait_time:
                timeout_msg = f"⏰ Timeout alcanzado - No todos los jobs terminaron en el tiempo límite de {max_wait_time} segundos"
                logger.error(timeout_msg)

                # Lanzar excepción por timeout
                incomplete_jobs = [job for job in executions if not job["completed"]]
                error_msg = f"❌ ERROR: {timeout_msg}\n"
                error_msg += f"📋 Jobs incompletos ({len(incomplete_jobs)}):\n"
                for job in incomplete_jobs:
                    error_msg += (
                        f"   - Job {job['index']}: {job['statement'][:100]}...\n"
                    )

                logger.error(error_msg)
                raise ValueError(error_msg)

            # Mostrar progreso solo si no hay jobs completados aún
            if completed_jobs == 0:
                logger.info(f"⏰ Esperando jobs... ({elapsed_time:.0f}s)")
            time.sleep(check_interval)

        # Resumen final
        total_time = time.time() - start_time
        logger.info(
            f"🎉 Ejecución completada: {successful_jobs} exitosos, {failed_jobs} fallidos ({total_time:.0f}s)"
        )

        # Verificar si hubo jobs fallidos y lanzar excepción si es necesario
        if failed_jobs > 0:
            failed_statements = []
            for job_info in executions:
                if job_info.get("status") == "FAILED":
                    failed_statements.append(
                        {
                            "index": job_info["index"],
                            "statement": (
                                job_info["statement"][:100] + "..."
                                if len(job_info["statement"]) > 100
                                else job_info["statement"]
                            ),
                            "result": job_info.get("result", {}),
                        }
                    )

            error_msg = f"❌ ERROR: {failed_jobs} de {len(executions)} statements fallaron en la ejecución paralela.\n"
            error_msg += f"📋 Statements fallidos:\n"
            for failed in failed_statements:
                error_msg += f"   - Job {failed['index']}: {failed['statement']}\n"
                if failed["result"].get("message"):
                    error_msg += f"     Error: {failed['result']['message']}\n"

            logger.error(error_msg)
            raise ValueError(error_msg)

        return {
            "status": "completed",
            "total_jobs": len(executions),
            "successful_jobs": successful_jobs,
            "failed_jobs": failed_jobs,
            "total_time_seconds": total_time,
            "executions": executions,
        }

    def _ensure_dict(self, value):
        """Verifica si `value` ya es un diccionario, si no lo convierte."""
        if isinstance(value, dict):
            return value
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            logger.error(f"Error al decodificar JSON: {e}")
            raise ValueError(
                f"❌ ERROR: La variable {value} no tiene formato JSON válido."
            )

    def _ensure_table_exists(self):
        """
        Verifica si la tabla existe. Si no, la crea con la configuración por defecto.
        """
        schema_definition = StructType(
            [
                StructField("conn_name", StringType(), True),
                StructField("conn_options", MapType(StringType(), StringType()), True),
                StructField(
                    "conn_aditional_options", MapType(StringType(), StringType()), True
                ),
                StructField("scope", StringType(), True),
                StructField("conn_user", StringType(), True),
                StructField("conn_key", StringType(), True),
                StructField("conn_driver", StringType(), True),
                StructField("conn_url", StringType(), True),
            ]
        )

        default_data = [
            {
                "conn_name": "default",
                "conn_options": self.BASE_URL,
                "conn_aditional_options": self.CONN_ADITIONAL_OPTIONS,
                "scope": SETTINGS.GENERAL.SCOPE,
                "conn_user": SETTINGS.GENERAL.CONN_USER,
                "conn_key": SETTINGS.GENERAL.CONN_KEY,
                "conn_driver": "oracle.jdbc.driver.OracleDriver",
                "conn_url": self.BASE_URL["url"],
            }
        ]

        tables = (
            self.spark.sql(f"SHOW TABLES IN {self.schema}")
            .filter(f"tableName = '{self.table}'")
            .collect()
        )

        if not tables:
            logger.info(f"La tabla {self.table} no existe. Creándola...")
            default_df = self.spark.createDataFrame(
                default_data, schema=schema_definition
            )
            default_df.write.format("delta").saveAsTable(f"{self.schema}.{self.table}")
            logger.info(f"Tabla {self.table} creada.")

    def create_or_update_config(
        self, conn_name, fetch_size="20000", batch_size="20000", num_partitions="100"
    ):
        """
        Crea o actualiza una configuración en la tabla Delta con manejo de concurrencia.

        Args:
            conn_name (str): Nombre de la conexión.
            fetch_size (str): Tamaño de la extracción JDBC.
            batch_size (str): Tamaño de las inserciones por lote.
            num_partitions (str): Número de particiones para leer datos.
        """
        max_retries = 5  # 🔹 Número implícito de reintentos
        retries = 0

        while retries < max_retries:
            try:
                # 🔹 Generar un nombre único para la vista temporal
                temp_view_name = f"new_data_temp_{conn_name}_{uuid.uuid4().hex[:8]}"

                conn_options = {
                    "url": self.BASE_URL["url"],
                    "driver": self.BASE_URL.get(
                        "driver", "oracle.jdbc.driver.OracleDriver"
                    ),
                    "fetchSize": str(fetch_size),
                    "numPartitions": str(num_partitions),
                    "queryTimeout": str(self.BASE_URL.get("queryTimeout", "0")),
                    "connectRetryCount": str(
                        self.BASE_URL.get("connectRetryCount", "10")
                    ),
                    "connectRetryInterval": str(
                        self.BASE_URL.get("connectRetryInterval", "5")
                    ),
                }

                conn_aditional_options = {
                    "batchsize": str(batch_size),
                    "sessionInitStatement": str(
                        self.CONN_ADITIONAL_OPTIONS.get(
                            "sessionInitStatement",
                            "BEGIN execute immediate 'ALTER SESSION ENABLE PARALLEL DML'; END;",
                        )
                    ),
                }

                new_data = [
                    {
                        "conn_name": conn_name,
                        "conn_options": conn_options,
                        "conn_aditional_options": conn_aditional_options,
                        "scope": SETTINGS.GENERAL.SCOPE,
                        "conn_user": SETTINGS.GENERAL.CONN_USER,
                        "conn_key": SETTINGS.GENERAL.CONN_KEY,
                        "conn_driver": conn_options["driver"],
                        "conn_url": conn_options["url"],
                    }
                ]

                new_df = self.spark.createDataFrame(new_data)

                # 🔹 Registrar la vista temporal con un nombre único
                new_df.createOrReplaceTempView(temp_view_name)

                # 🔹 Configurar el nivel de aislamiento para permitir concurrencia
                self.spark.sql(
                    "SET spark.databricks.delta.isolationLevel = WRITE_COMMITTED"
                )

                # 🔹 Ejecutar `MERGE INTO` con la vista temporal única
                self.spark.sql(
                    f"""
                    MERGE INTO {self.schema}.{self.table} AS target
                    USING {temp_view_name} AS source
                    ON target.conn_name = source.conn_name
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.conn_options = source.conn_options,
                            target.conn_aditional_options = source.conn_aditional_options,
                            target.scope = source.scope,
                            target.conn_user = source.conn_user,
                            target.conn_key = source.conn_key,
                            target.conn_driver = source.conn_driver,
                            target.conn_url = source.conn_url
                    WHEN NOT MATCHED THEN
                        INSERT *
                """
                )

                logger.info(
                    f"✅ Configuración '{conn_name}' creada o actualizada correctamente en intento {retries + 1}."
                )

                # 🔹 Eliminar la vista temporal después de la operación
                self.spark.catalog.dropTempView(temp_view_name)

                break  # ✅ Si todo funciona, salir del loop

            except Exception as e:
                error_msg = str(e)

                # 🔹 Log para ver el tipo de excepción
                logger.error(
                    f"⚠️ Error detectado en intento {retries + 1}: {type(e).__name__} - {error_msg}"
                )

                # if (
                #     "Concurrent modification" in error_msg
                #     or "ConcurrentAppendException" in error_msg
                # ):
                retries += 1
                if retries < max_retries:
                    logger.warning(
                        f"⚠️ Concurrencia detectada en '{conn_name}', reintentando intento {retries}/{max_retries} en 10 segundos..."
                    )
                    time.sleep(10)  # Esperar antes de reintentar
                else:
                    logger.error(
                        f"❌ No se pudo actualizar '{conn_name}' después de {max_retries} intentos."
                    )
                    raise ValueError(
                        f"❌ No se pudo actualizar la configuración '{conn_name}' después de {max_retries} intentos."
                    )
                # else:
                #     raise e  # Si el error no es de concurrencia, lanzar excepción inmediatamente

    def delete_config(self, conn_name):
        """
        Elimina una configuración existente.

        Args:
            conn_name (str): Nombre de la configuración a eliminar.
        """
        self.spark.sql(
            f"DELETE FROM {self.schema}.{self.table} WHERE conn_name = "
            f"'{conn_name}'"
        )
        logger.info(f"Configuración '{conn_name}' eliminada.")

    def list_configs(self):
        """
        Lista todas las configuraciones disponibles.
        """
        configs = self.spark.sql(f"SELECT * FROM {self.schema}.{self.table}")
        return configs

    def get_config(self, conn_name):
        """
        Obtiene una configuración específica.

        Args:
            conn_name (str): Nombre de la configuración a obtener.

        Returns:
            dict: Configuración en formato de diccionario.
        """
        # Comentado para evitar consultas a default_connection
        # configs = self.spark.sql(
        #     f"SELECT * FROM {self.schema}.{self.table} WHERE conn_name = "
        #     f"'{conn_name}'"
        # ).collect()
        # if not configs:
        #     raise ValueError(f"No se encontró la configuración '{conn_name}'.")
        # return configs[0].asDict()

        # Retorna configuración directamente desde SETTINGS
        return {
            "conn_name": conn_name,
            "conn_options": self.BASE_URL,
            "conn_aditional_options": self.CONN_ADITIONAL_OPTIONS,
            "scope": SETTINGS.GENERAL.SCOPE,
            "conn_user": SETTINGS.GENERAL.CONN_USER,
            "conn_key": SETTINGS.GENERAL.CONN_KEY,
            "conn_driver": self.BASE_URL.get(
                "driver", "oracle.jdbc.driver.OracleDriver"
            ),
            "conn_url": self.BASE_URL["url"],
        }

    def _get_credentials_from_scope(self, scope, user_key, pass_key):
        """
        Obtiene las credenciales almacenadas en un Scope de Databricks.

        Args:
            scope (str): Nombre del scope.
            user_key (str): Clave del usuario en el scope.
            pass_key (str): Clave de la contraseña en el scope.

        Returns:
            tuple: Usuario y contraseña.
        """
        try:
            user = dbutils.secrets.get(scope=scope, key=user_key)
            password = dbutils.secrets.get(scope=scope, key=pass_key)
            return user, password
        except Exception as e:
            logger.error(f"Error al obtener credenciales del scope '{scope}': {e}")
            raise

    def read_data(self, conn_name="default", sql_query=None):
        """
        Lee datos de una base de datos utilizando la configuración JDBC.

        Args:
            conn_name (str): Nombre de la conexión a utilizar.
            sql_query (str): Sentencia SQL personalizada (opcional).

        Returns:
            DataFrame: DataFrame resultante de la lectura.
        """
        try:
            connection = self.get_config(conn_name)
            if not connection:
                raise ValueError(
                    f"No existe configuración para la conexión '{conn_name}'."
                )

            scope = connection["scope"]
            user, password = self._get_credentials_from_scope(
                scope, connection["conn_user"], connection["conn_key"]
            )

            jdbc_url = connection["conn_url"]
            options = {
                "url": jdbc_url,
                "user": user,
                "password": password,
                "driver": connection["conn_driver"],
                **connection["conn_options"],
                "conn_aditional_options": connection["conn_aditional_options"],
            }

            query = f"({sql_query})"

            logger.info(f"Leyendo datos usando la " f"conexión '{conn_name}'.")

            df = (
                self.spark.read.format("jdbc")
                .options(**options)
                .option("dbtable", query)
                .load()
            )

            return df
        except Exception as e:
            logger.error(f"Error al leer datos de la tabla: {e}")
            raise

    def write_data(self, df, table_name, conn_name="default", mode="overwrite"):
        """
        Escribe datos en una base de datos utilizando la configuración JDBC.

        Args:
            df (DataFrame): DataFrame a escribir.
            table_name (str): Nombre de la tabla destino.
            conn_name (str): Nombre de la conexión a utilizar.
            mode (str): Modo de escritura ('overwrite', 'append', etc.).
        """
        import gc

        try:
            connection = self.get_config(conn_name)
            if not connection:
                raise ValueError(
                    f"No existe configuración para la conexión '{conn_name}'."
                )

            scope = connection["scope"]
            user, password = self._get_credentials_from_scope(
                scope, connection["conn_user"], connection["conn_key"]
            )

            jdbc_url = connection["conn_url"]
            options = {
                "url": jdbc_url,
                "user": user,
                "password": password,
                "driver": connection["conn_driver"],
                **connection["conn_options"],
                "conn_aditional_options": connection["conn_aditional_options"],
            }

            logger.info(
                f"Escribiendo datos en la tabla '{table_name}' usando la conexión '{conn_name}'."
            )

            df.write.format("jdbc").options(**options).option(
                "dbtable", table_name
            ).mode(mode).save()
            logger.info(f"✅ Escritura completada para tabla '{table_name}'.")

        except Exception as e:
            logger.error(f"❌ Error al escribir datos en la tabla '{table_name}': {e}")
            raise

        # 🔹 Limpieza del DataFrame
        try:
            if df.is_cached:
                logger.info(
                    "Liberando cache del DataFrame con unpersist(blocking=False)..."
                )
                df.unpersist(blocking=False)

                try:
                    logger.info("Materializando parcialmente con take(1)...")
                    df.take(1)
                except Exception as e:
                    logger.warning(f"No se pudo materializar con take(1): {e}")

            df = None
            gc.collect()
            logger.info(f"🧹 Memoria liberada tras escribir la tabla '{table_name}'")
        except Exception as cleanup_err:
            logger.warning(f"⚠️ No se pudo liberar el DataFrame: {cleanup_err}")

    def write_delta(
        self,
        delta_name,
        dataframe,
        method="overwrite",
        allow_delete=True,
        delete_until=None,
        optimize_partitions=True,
        target_partition_size_mb=512,
        z_order_columns=None,
        enable_liquid_clustering=False,
        liquid_clustering_columns=None,
        fast_write_mode=True,
        post_write_optimize=False,
        post_write_analyze=False,
    ):
        """
        Escribe datos en una tabla Delta optimizada para MÁXIMA VELOCIDAD DE ESCRITURA.

        Args:
            delta_name (str): Nombre de la tabla Delta.
            dataframe (DataFrame): DataFrame de PySpark a escribir.
            method (str): Método de escritura ('overwrite' o 'append').
            allow_delete (bool): Si la tabla puede ser borrada automáticamente (por defecto True).
            delete_until (int, opcional): Subetapa hasta la cual se puede borrar la tabla (requerido si allow_delete=False).
            optimize_partitions (bool): Si optimizar automáticamente las particiones antes de escribir.
            target_partition_size_mb (int): Tamaño objetivo de partición en MB (por defecto 512MB).
            z_order_columns (list): Columnas para Z-ORDER BY optimization (solo si post_write_optimize=True).
            enable_liquid_clustering (bool): Habilitar Liquid Clustering (requiere DBR 13.3+).
            liquid_clustering_columns (list): Columnas para Liquid Clustering.
            fast_write_mode (bool): MODO ESCRITURA RÁPIDA - omite optimizaciones que ralentizan (por defecto True).
            post_write_optimize (bool): Ejecutar OPTIMIZE después de escribir (ralentiza escritura, por defecto False).
            post_write_analyze (bool): Ejecutar ANALYZE TABLE después de escribir (ralentiza escritura, por defecto False).
        """
        import gc
        import math

        from pyspark.sql.functions import col, spark_partition_id
        from pyspark.sql.types import StringType

        # 🔹 Validar delete_until cuando allow_delete=False
        if not allow_delete and delete_until is None:
            raise ValueError(
                "❌ El parámetro 'delete_until' es obligatorio cuando allow_delete=False"
            )

        # 🔹 Convertir columnas VOID a StringType (optimizado y rápido)
        void_columns = [
            field.name
            for field in dataframe.schema.fields
            if field.dataType.simpleString() == "void"
        ]

        if void_columns:
            logger.info(
                f"🔧 Convirtiendo {len(void_columns)} columnas VOID a StringType"
            )
            for col_name in void_columns:
                dataframe = dataframe.withColumn(
                    col_name, col(col_name).cast(StringType())
                )

        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}".lower()

        # 🔹 Preparación si overwrite (rápido)
        if method == "overwrite":
            self._drop_delta_force(delta_name)

            # 🔹 DATABRICKS DECIDE PARTICIONES AUTOMÁTICAMENTE
        # No intervenimos - dejamos que AQE, optimizeWrite y las configuraciones automáticas
        # decidan el número óptimo de particiones para cada DataFrame individualmente
        if not fast_write_mode and optimize_partitions:
            logger.info(
                "✅ Databricks decidirá particiones óptimas automáticamente con AQE"
            )
        elif fast_write_mode:
            # En modo rápido, 100% automático sin logs innecesarios
            pass

        # 🔹 ESCRITURA DELTA ULTRA-RÁPIDA
        try:
            logger.info(
                f"⚡ ESCRITURA RÁPIDA: {full_table_name} (particiones automáticas)"
            )

            # Configurar writer con opciones de velocidad máxima
            writer = dataframe.write.format("delta").mode(method)

            # Aplicar Liquid Clustering solo si está explícitamente habilitado
            if enable_liquid_clustering and liquid_clustering_columns:
                logger.info(f"🧬 Liquid Clustering: {liquid_clustering_columns}")
                writer = writer.option("delta.enableLiquidClustering", "true")
                writer = writer.option(
                    "delta.liquidClustering.columns",
                    ",".join(liquid_clustering_columns),
                )

            # ⚡ ESCRITURA INMEDIATA SIN BLOQUEOS
            writer.saveAsTable(full_table_name)
            logger.info(f"✅ ESCRITURA COMPLETADA: {full_table_name}")

            # 🔹 POST-ESCRITURA: SOLO SI NO ES FAST_WRITE_MODE
            if not fast_write_mode and (post_write_optimize or post_write_analyze):
                logger.info("🔧 Ejecutando optimizaciones post-escritura...")

                if post_write_optimize:
                    if z_order_columns and len(z_order_columns) > 0:
                        z_order_cols = ", ".join(z_order_columns)
                        logger.info(f"🎯 Z-ORDER: {z_order_cols}")
                        self.spark.sql(
                            f"OPTIMIZE {full_table_name} ZORDER BY ({z_order_cols})"
                        )
                    else:
                        logger.info(f"🔧 OPTIMIZE básico")
                        self.spark.sql(f"OPTIMIZE {full_table_name}")

                if post_write_analyze:
                    logger.info(f"📈 ANALYZE TABLE")
                    self.spark.sql(
                        f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
                    )
            else:
                pass  # Modo rápido: sin optimizaciones post-escritura

            # 🔹 Registro en control DESHABILITADO para máxima velocidad
            # notebook_name = self._get_notebook_name()
            # sr_subetapa = self._get_sr_subetapa()
            # self._register_delta_table(
            #     delta_name, notebook_name, allow_delete, sr_subetapa, delete_until
            # )

        except Exception as e:
            logger.error(f"❌ Error en escritura rápida: {e}")
            raise

        # 🔹 LIMPIEZA RÁPIDA DE MEMORIA (no bloqueante)
        try:
            if dataframe.is_cached:
                dataframe.unpersist(blocking=False)  # No bloqueante

            # Limpieza mínima y rápida
            dataframe = None
            if not fast_write_mode:  # Solo gc completo si no es modo ultra-rápido
                gc.collect()

            logger.info(f"⚡ Escritura y limpieza completadas: {full_table_name}")

        except Exception as e:
            logger.warning(f"⚠️ Limpieza rápida: {e}")

    def write_delta_fast(
        self, delta_name, dataframe, method="overwrite", max_retries=3
    ):
        """
        ⚡ ESCRITURA DELTA ULTRA-RÁPIDA - Optimizada para concurrencia multi-notebook.

        Esta función está diseñada para MÁXIMA VELOCIDAD DE ESCRITURA en entornos
        con múltiples notebooks ejecutándose simultáneamente.

        Args:
            delta_name (str): Nombre de la tabla Delta.
            dataframe (DataFrame): DataFrame de PySpark a escribir.
            method (str): Método de escritura ('overwrite' o 'append').
            max_retries (int): Número máximo de reintentos en caso de conflictos de concurrencia.
        """
        import random
        import time

        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType
        from pyspark.sql.utils import AnalysisException

        # Solo conversión esencial de columnas VOID (ultra-rápido)
        void_columns = [
            field.name
            for field in dataframe.schema.fields
            if field.dataType.simpleString() == "void"
        ]

        if void_columns:
            for col_name in void_columns:
                dataframe = dataframe.withColumn(
                    col_name, col(col_name).cast(StringType())
                )

        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}".lower()

        # 🔹 ESCRITURA CON MANEJO DE CONCURRENCIA
        for attempt in range(max_retries + 1):
            try:
                # Drop rápido si es overwrite (solo en primer intento)
                if method == "overwrite" and attempt == 0:
                    # self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    self._drop_delta_force(delta_name)

                # ⚡ ESCRITURA INMEDIATA CON CONFIGURACIONES DE CONCURRENCIA
                logger.info(
                    f"⚡ ESCRITURA ULTRA-RÁPIDA: {full_table_name} (intento {attempt + 1})"
                )

                # Configurar writer con opciones específicas para concurrencia
                writer = (
                    dataframe.write.format("delta")
                    .mode(method)
                    .option(
                        "delta.autoOptimize.optimizeWrite", "true"
                    )  # Optimización automática
                    .option(
                        "delta.autoOptimize.autoCompact", "false"
                    )  # Deshabilitado para concurrencia
                    .option("delta.isolationLevel", "WriteSerializable")
                )  # Máximo aislamiento

                writer.saveAsTable(full_table_name)
                logger.info(f"✅ COMPLETADO: {full_table_name}")
                break  # Éxito - salir del loop de reintentos

            except Exception as e:
                error_msg = str(e).lower()

                # Detectar errores de concurrencia comunes
                is_concurrency_error = any(
                    keyword in error_msg
                    for keyword in [
                        "concurrent",
                        "conflict",
                        "version",
                        "transaction",
                        "commitinfo",
                        "metadata",
                        "protocol",
                        "retry",
                    ]
                )

                if is_concurrency_error and attempt < max_retries:
                    # Backoff exponencial con jitter para evitar thundering herd
                    wait_time = (2**attempt) + random.uniform(0.1, 1.0)
                    logger.warning(
                        f"⚠️ Conflicto de concurrencia detectado. Reintentando en {wait_time:.1f}s... (intento {attempt + 1}/{max_retries + 1})"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    # Error no relacionado con concurrencia o se agotaron los reintentos
                    if attempt == max_retries:
                        logger.error(
                            f"❌ Falló después de {max_retries + 1} intentos: {e}"
                        )
                    else:
                        logger.error(f"❌ Error no relacionado con concurrencia: {e}")
                    raise

        # Limpieza mínima no bloqueante
        try:
            if dataframe.is_cached:
                dataframe.unpersist(blocking=False)
            dataframe = None
        except:
            pass  # Ignorar errores de limpieza en modo ultra-rápido

    def get_cluster_concurrency_status(self):
        """
        Obtiene información sobre el estado de concurrencia del cluster y recursos disponibles.
        Útil para monitorear el rendimiento con múltiples notebooks ejecutándose.

        Returns:
            dict: Información detallada sobre el estado del cluster.
        """
        try:
            status = {}

            # 🔹 Información de Spark Context
            sc = self.spark.sparkContext
            status["spark_app_id"] = sc.applicationId
            status["spark_app_name"] = sc.appName
            status["default_parallelism"] = sc.defaultParallelism
            status["default_min_partitions"] = sc.defaultMinPartitions

            # 🔹 Configuraciones críticas para concurrencia
            spark_conf = self.spark.conf
            status["scheduler_mode"] = spark_conf.get("spark.scheduler.mode", "FIFO")
            status["isolation_level"] = spark_conf.get(
                "spark.databricks.delta.isolationLevel", "Serializable"
            )
            status["max_concurrent_retries"] = spark_conf.get(
                "spark.databricks.delta.concurrentTransactionRetries", "3"
            )
            status["concurrent_backoff_ms"] = spark_conf.get(
                "spark.databricks.delta.concurrentTransactionBackoff", "1000"
            )

            # 🔹 Información de memoria y cache
            storage_level = self.spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
            status["executor_memory_info"] = str(storage_level)

            # 🔹 Configuraciones de particiones
            status["shuffle_partitions"] = spark_conf.get(
                "spark.sql.shuffle.partitions", "200"
            )
            status["adaptive_enabled"] = spark_conf.get(
                "spark.sql.adaptive.enabled", "false"
            )
            status["max_partition_bytes"] = spark_conf.get(
                "spark.sql.files.maxPartitionBytes", "128MB"
            )

            # 🔹 Cache I/O status
            status["io_cache_enabled"] = spark_conf.get(
                "spark.databricks.io.cache.enabled", "false"
            )
            status["io_cache_max_disk"] = spark_conf.get(
                "spark.databricks.io.cache.maxDiskUsage", "0"
            )

            # 🔹 Photon status
            status["photon_enabled"] = spark_conf.get(
                "spark.databricks.photon.enabled", "false"
            )

            logger.info(f"📊 Estado del cluster obtenido: {len(status)} métricas")
            return status

        except Exception as e:
            logger.error(f"❌ Error obteniendo estado del cluster: {e}")
            return {"error": str(e)}

    def monitor_concurrent_operations(
        self, operation_name="operacion", duration_seconds=30
    ):
        """
        Monitorea las operaciones concurrentes durante un período específico.
        Útil para diagnosticar problemas de rendimiento con múltiples notebooks.

        Args:
            operation_name (str): Nombre descriptivo de la operación a monitorear.
            duration_seconds (int): Duración del monitoreo en segundos.
        """
        import threading
        import time

        try:
            logger.info(
                f"🔍 Iniciando monitoreo de '{operation_name}' por {duration_seconds}s..."
            )

            start_time = time.time()
            metrics = []

            def collect_metrics():
                while time.time() - start_time < duration_seconds:
                    try:
                        current_time = time.time() - start_time

                        # Obtener métricas básicas de Spark
                        sc = self.spark.sparkContext
                        active_jobs = len(sc.statusTracker().getActiveJobIds())
                        active_stages = len(sc.statusTracker().getActiveStageIds())

                        # Información de ejecutores
                        executor_infos = sc.statusTracker().getExecutorInfos()
                        total_cores = sum(
                            executor.totalCores for executor in executor_infos
                        )
                        active_tasks = sum(
                            executor.activeTasks for executor in executor_infos
                        )

                        metric = {
                            "timestamp": current_time,
                            "active_jobs": active_jobs,
                            "active_stages": active_stages,
                            "total_cores": total_cores,
                            "active_tasks": active_tasks,
                            "max_tasks": sum(
                                executor.maxTasks for executor in executor_infos
                            ),
                        }

                        metrics.append(metric)
                        time.sleep(2)  # Recopilar métricas cada 2 segundos

                    except Exception as e:
                        logger.warning(f"⚠️ Error recopilando métricas: {e}")
                        time.sleep(2)

            # Ejecutar recopilación en thread separado
            monitor_thread = threading.Thread(target=collect_metrics)
            monitor_thread.daemon = True
            monitor_thread.start()
            monitor_thread.join(duration_seconds + 5)  # Timeout de seguridad

            # Analizar métricas recopiladas
            if metrics:
                avg_active_jobs = sum(m["active_jobs"] for m in metrics) / len(metrics)
                max_active_jobs = max(m["active_jobs"] for m in metrics)
                avg_active_tasks = sum(m["active_tasks"] for m in metrics) / len(
                    metrics
                )
                max_active_tasks = max(m["active_tasks"] for m in metrics)
                avg_total_cores = sum(m["total_cores"] for m in metrics) / len(metrics)

                logger.info(f"📊 Resumen de monitoreo '{operation_name}':")
                logger.info(
                    f"   🔹 Jobs activos promedio: {avg_active_jobs:.1f} (máximo: {max_active_jobs})"
                )
                logger.info(
                    f"   🔹 Tasks activos promedio: {avg_active_tasks:.1f} (máximo: {max_active_tasks})"
                )
                logger.info(f"   🔹 Métricas recopiladas: {len(metrics)} muestras")

                # Detectar posible contención
                if max_active_jobs > 10:
                    logger.warning(
                        "⚠️ Alto número de jobs concurrentes detectado - posible contención"
                    )
                if avg_total_cores > 0 and avg_active_tasks / avg_total_cores > 0.8:
                    logger.warning(
                        "⚠️ Alta utilización de cores - posible saturación del cluster"
                    )
            else:
                logger.warning("⚠️ No se pudieron recopilar métricas de monitoreo")

        except Exception as e:
            logger.error(f"❌ Error en monitoreo de operaciones concurrentes: {e}")

    def _get_notebook_name(self):
        """
        Obtiene el nombre del notebook actual en Databricks.

        Returns:
            str: Nombre del notebook o "Notebook_Desconocido" si no se puede obtener.
        """
        try:
            notebook_name = (
                dbutils.notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .notebookPath()
                .get()
            )
            # Extraer solo la última parte del notebook después de "Notebooks/"
            notebook_key = (
                notebook_name.split("Notebooks/")[-1]
                if "Notebooks/" in notebook_name
                else notebook_name
            )
            return notebook_key
        except Exception as e:
            logger.warning(f"No se pudo obtener el nombre del notebook: {e}")
            return "Notebook_Desconocido"

    def _get_notebook_params(self):
        """
        Obtiene los parámetros del notebook actual usando el approach de handler.py.

        Returns:
            dict: Diccionario con los parámetros del notebook o diccionario vacío si no se pueden obtener.
        """
        import sys

        try:
            # Obtener el nombre completo del notebook
            notebook_name = self._get_notebook_name()

            # Clave única para evitar colisiones
            params_key = f"params_{notebook_name}"

            if notebook_name == "Notebook_Desconocido":
                return {}

            # Intentar obtener desde sys.modules
            if params_key in sys.modules:
                params_instance = sys.modules[params_key]

                # Convertir a diccionario
                if hasattr(params_instance, "to_dict"):
                    return params_instance.to_dict()
                else:
                    return vars(params_instance)

            # Si no está en sys.modules, intentar desde dbutils.widgets
            try:
                params = {key: value for key, value in dbutils.widgets.getAll().items()}
                return params

            except Exception as e:
                logger.warning(f"⚠️ No se pudieron obtener los parámetros: {e}")
                return {}

        except Exception as e:
            logger.error(f"❌ Error al obtener parámetros del notebook: {e}")
            return {}

    def _get_sr_subetapa(self):
        """
        Obtiene el parámetro sr_subetapa del notebook actual.

        Returns:
            int or None: Valor de sr_subetapa convertido a int o None si no se encuentra.
        """
        params = self._get_notebook_params()
        sr_subetapa = params.get("sr_subetapa")

        if sr_subetapa is None:
            logger.warning("⚠️ No se pudo obtener el parámetro sr_subetapa")
            return None

        # Convertir a int si es posible
        try:
            sr_subetapa_int = int(sr_subetapa)
            return sr_subetapa_int
        except (ValueError, TypeError) as e:
            logger.error(
                f"❌ Error al convertir sr_subetapa '{sr_subetapa}' a int: {e}"
            )
            return None

    def _get_debug_param(self):
        """
        Obtiene el parámetro debug del objeto ConfManager en el notebook actual.

        Busca dinámicamente el objeto ConfManager (típicamente llamado 'conf') en el notebook
        y extrae el valor de debug de ese objeto.

        Returns:
            bool: Valor de debug del objeto ConfManager. Retorna False si no se encuentra.
        """
        try:
            # Obtener el nombre del notebook actual
            notebook_name = self._get_notebook_name()
            logger.info(f"🔍 Buscando objeto ConfManager en notebook: {notebook_name}")

            # Buscar el objeto ConfManager en el namespace global del notebook
            conf_object = self._find_conf_manager_in_notebook()

            if conf_object is None:
                logger.info(
                    "ℹ️ No se encontró objeto ConfManager en el notebook, usando debug=False"
                )
                return False

            # Intentar obtener el valor de debug del objeto ConfManager
            try:
                debug_value = conf_object.debug
                logger.info(
                    f"✅ Valor de debug encontrado en ConfManager: {debug_value}"
                )

                # Asegurar que es un booleano
                if isinstance(debug_value, bool):
                    return debug_value
                else:
                    # Convertir a bool si no lo es
                    debug_bool = bool(debug_value)
                    logger.info(
                        f"ℹ️ Convertido debug '{debug_value}' a bool: {debug_bool}"
                    )
                    return debug_bool

            except AttributeError:
                logger.warning(
                    "⚠️ El objeto ConfManager no tiene atributo 'debug', usando debug=False"
                )
                return False
            except Exception as e:
                logger.error(f"❌ Error al acceder a conf.debug: {e}")
                return False

        except Exception as e:
            logger.error(f"❌ Error al buscar ConfManager en notebook: {e}")
            return False

    def _find_conf_manager_in_notebook(self):
        """
        Busca el objeto ConfManager en el notebook actual.

        Returns:
            ConfManager object or None: El objeto ConfManager encontrado o None si no se encuentra.
        """
        import inspect
        import sys

        try:
            # Buscar en el namespace global del notebook
            # En Databricks, el namespace del notebook está en el frame actual
            current_frame = inspect.currentframe()

            # Subir en la pila de frames para encontrar el namespace del notebook
            frame = current_frame
            conf_object = None

            # Buscar en múltiples frames hacia arriba
            for i in range(10):  # Máximo 10 frames hacia arriba
                if frame is None:
                    break

                # Obtener las variables locales del frame
                local_vars = frame.f_locals

                # Buscar objeto con nombre 'conf' que sea instancia de ConfManager
                for var_name, var_value in local_vars.items():
                    if var_name == "conf" and hasattr(var_value, "env_variables"):
                        # Verificar que tenga los métodos característicos de ConfManager
                        if hasattr(var_value, "get") and hasattr(
                            var_value, "__getattr__"
                        ):
                            conf_object = var_value
                            logger.info(
                                f"✅ Objeto ConfManager encontrado como variable '{var_name}' en frame {i}"
                            )
                            break

                if conf_object is not None:
                    break

                # Buscar también en variables globales del frame
                if hasattr(frame, "f_globals"):
                    global_vars = frame.f_globals
                    for var_name, var_value in global_vars.items():
                        if var_name == "conf" and hasattr(var_value, "env_variables"):
                            if hasattr(var_value, "get") and hasattr(
                                var_value, "__getattr__"
                            ):
                                conf_object = var_value
                                logger.info(
                                    f"✅ Objeto ConfManager encontrado como variable global '{var_name}' en frame {i}"
                                )
                                break

                if conf_object is not None:
                    break

                frame = frame.f_back

            return conf_object

        except Exception as e:
            logger.error(f"❌ Error al buscar ConfManager en frames: {e}")
            return None
        finally:
            # Limpiar referencia del frame para evitar memory leaks
            if "current_frame" in locals():
                del current_frame

    def _extract_base_table_name(self, delta_name):
        """
        Extrae el nombre base de la tabla Delta (sin folio).

        Args:
            delta_name (str): Nombre completo de la tabla Delta.

        Returns:
            str: Nombre base de la tabla sin folio.
        """
        # Buscar patrones comunes de folios al final del nombre
        import re

        # Patrones para detectar folios al final: _123, _ABC123, _20231201, etc.
        patterns = [
            r"_\d+$",  # _123, _456, etc.
            r"_[A-Z0-9]+$",  # _ABC123, _FOLIO001, etc.
            r"_\d{8}$",  # _20231201, _20240115, etc.
            r"_\d{6}$",  # _231201, _240115, etc.
        ]

        base_name = delta_name
        for pattern in patterns:
            match = re.search(pattern, delta_name)
            if match:
                base_name = delta_name[: match.start()]
                # logger.info(
                #     f"🔍 Extraído nombre base '{base_name}' de '{delta_name}' (patrón: {pattern})"
                # )
                break

        return base_name.lower()

    def _register_delta_table(
        self,
        delta_name,
        notebook_name,
        allow_delete=True,
        sr_subetapa=None,
        delete_until=None,
    ):
        """
        Registra una tabla Delta en la tabla de control.

        Args:
            delta_name (str): Nombre de la tabla Delta.
            notebook_name (str): Nombre del notebook donde se creó.
            allow_delete (bool): Si la tabla puede ser borrada automáticamente.
            sr_subetapa (int, opcional): Subetapa donde se creó la tabla.
            delete_until (int, opcional): Subetapa hasta la cual se puede borrar la tabla.
        """
        try:
            from datetime import datetime

            from pyspark.sql.types import (
                BooleanType,
                IntegerType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            # Crear tabla de control si no existe
            control_table = f"{self.catalog}.{self.schema}.delta_tables_control"

            try:
                self.spark.sql(
                    f"""CREATE TABLE IF NOT EXISTS {control_table} (
                        id STRING,
                        delta_name STRING,
                        notebook_name STRING,
                        allow_delete BOOLEAN,
                        created_at TIMESTAMP,
                        workflow_name STRING,
                        sr_subetapa INT,
                        delete_until INT
                    ) USING DELTA"""
                )
            except Exception as e:
                logger.warning(
                    f"No se pudo crear la tabla de control (puede que ya exista): {e}"
                )

            # Extraer nombre base de la tabla (sin folio)
            base_table_name = self._extract_base_table_name(delta_name)

            # Preparar datos para registro
            import uuid

            from pyspark.sql.functions import current_timestamp

            log_data = [
                {
                    "id": str(uuid.uuid4()),
                    "delta_name": base_table_name,  # Usar nombre base en lugar del nombre completo
                    "notebook_name": notebook_name,
                    "allow_delete": allow_delete,
                    "created_at": datetime.utcnow(),
                    "workflow_name": None,  # Inicialmente vacío
                    "sr_subetapa": sr_subetapa,
                    "delete_until": delete_until,
                }
            ]

            schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("delta_name", StringType(), False),
                    StructField("notebook_name", StringType(), True),
                    StructField("allow_delete", BooleanType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("workflow_name", StringType(), True),
                    StructField("sr_subetapa", IntegerType(), True),
                    StructField("delete_until", IntegerType(), True),
                ]
            )

            df_log = self.spark.createDataFrame(log_data, schema)

            # Usar MERGE para evitar duplicados
            temp_view_name = f"delta_control_temp_{uuid.uuid4().hex[:8]}"
            df_log.createOrReplaceTempView(temp_view_name)

            self.spark.sql(
                f"""
                MERGE INTO {control_table} AS target
                USING {temp_view_name} AS source
                ON target.delta_name = source.delta_name
                WHEN MATCHED THEN
                    UPDATE SET
                        target.notebook_name = source.notebook_name,
                        target.allow_delete = source.allow_delete,
                        target.created_at = source.created_at,
                        target.workflow_name = source.workflow_name,
                        target.sr_subetapa = source.sr_subetapa,
                        target.delete_until = source.delete_until
                WHEN NOT MATCHED THEN
                    INSERT *
                """
            )

            self.spark.catalog.dropTempView(temp_view_name)
            logger.info(
                f"✅ Tabla Delta '{delta_name}' registrada en control (allow_delete={allow_delete}, sr_subetapa={sr_subetapa}, delete_until={delete_until})"
            )

        except Exception as e:
            logger.error(f"❌ Error al registrar tabla Delta en control: {e}")

    def cleanup_delta_tables_by_notebooks(self, notebook_names=None, table_suffix=None):
        """
        Borra todas las tablas Delta que coincidan con la subetapa detectada automáticamente
        y el table_suffix proporcionado, basándose en las reglas de borrado.

        El método identifica automáticamente la sr_subetapa actual y busca tablas de TODOS los notebooks
        que coincidan con esa subetapa y el table_suffix.

        Args:
            notebook_names (list, opcional): Lista de nombres de notebooks para filtrar.
                                           Si es None, busca en TODOS los notebooks.
            table_suffix (str, opcional): Sufijo para completar el nombre de la tabla.
                                        Ej: si table_suffix="123", buscará "demo_delta_001_123"
        """
        try:
            control_table = f"{self.catalog}.{self.schema}.delta_tables_control"

            # Verificar si la tabla de control existe
            tables = (
                self.spark.sql(f"SHOW TABLES IN {self.schema}")
                .filter(f"tableName = 'delta_tables_control'")
                .collect()
            )

            if not tables:
                logger.warning(
                    "⚠️ No existe tabla de control de tablas Delta. No se pueden borrar tablas."
                )
                return

            # Obtener la subetapa actual del notebook
            current_sr_subetapa = self._get_sr_subetapa()
            if current_sr_subetapa is None:
                logger.warning(
                    "⚠️ No se pudo obtener la subetapa actual. No se pueden borrar tablas."
                )
                return

            # current_sr_subetapa ya es un int desde _get_sr_subetapa()
            logger.info(f"🔍 Subetapa actual detectada: {current_sr_subetapa}")

            # Obtener tablas que se pueden borrar
            if table_suffix:
                # Construir la consulta base
                base_query = f"""
                    SELECT delta_name, notebook_name, allow_delete, sr_subetapa, delete_until
                    FROM {control_table}
                    WHERE (
                        (allow_delete = true AND CAST(sr_subetapa AS INT) = {current_sr_subetapa})
                        OR 
                        (allow_delete = false AND delete_until = {current_sr_subetapa})
                    )
                """

                # Agregar filtro de notebooks si se especifica
                if notebook_names:
                    notebook_list = [f"'{name}'" for name in notebook_names]
                    notebook_filter = (
                        f"AND notebook_name IN ({','.join(notebook_list)})"
                    )
                    base_query += f" {notebook_filter}"
                    logger.info(f"🔍 Filtrando por notebooks: {notebook_names}")
                else:
                    logger.info(f"🔍 Buscando en TODOS los notebooks")

                # Ejecutar la consulta
                deletable_tables_raw = self.spark.sql(base_query).collect()

                # Construir nombres completos con sufijo
                deletable_tables = []
                for table_info in deletable_tables_raw:
                    deletable_tables.append(
                        {
                            "delta_name": table_info.delta_name,
                            "notebook_name": table_info.notebook_name,
                            "allow_delete": table_info.allow_delete,
                            "sr_subetapa": table_info.sr_subetapa,
                            "delete_until": table_info.delete_until,
                            "full_table_name": f"{self.catalog}.{self.schema}.{table_info.delta_name}_{table_suffix}".lower(),
                        }
                    )
            else:
                logger.warning(
                    "⚠️ No se especificó table_suffix. Las tablas no se podrán borrar porque no existen físicamente."
                )
                return

            if not deletable_tables:
                logger.info(
                    f"ℹ️ No se encontraron tablas borrables para la subetapa {current_sr_subetapa} con table_suffix '{table_suffix}'"
                )
                return

            logger.info(
                f"🗑️ Borrando {len(deletable_tables)} tablas Delta para subetapa {current_sr_subetapa} con table_suffix '{table_suffix}'..."
            )

            deleted_count = 0
            for table_info in deletable_tables:
                try:
                    full_table_name = table_info["full_table_name"]
                    delta_name = table_info["delta_name"]
                    notebook_name = table_info["notebook_name"]
                    allow_delete = table_info["allow_delete"]
                    sr_subetapa = table_info["sr_subetapa"]
                    delete_until = table_info["delete_until"]

                    # Determinar el motivo del borrado
                    if allow_delete:
                        motivo = f"allow_delete=True y sr_subetapa={sr_subetapa}"
                    else:
                        motivo = f"allow_delete=False y delete_until={delete_until}"

                    logger.info(
                        f"🗑️ Borrando tabla '{delta_name}' creada en notebook '{notebook_name}' ({motivo})..."
                    )

                    # Usar drop_delta para borrar la tabla física del catálogo
                    table_name_with_suffix = f"{delta_name}_{table_suffix}"
                    self.drop_delta(table_name_with_suffix)

                    # Mantener el registro en la tabla de control para historial
                    logger.info(
                        f"✅ Tabla '{table_name_with_suffix}' borrada del catálogo. Registro mantenido en control para historial."
                    )

                    deleted_count += 1
                    logger.info(
                        f"✅ Tabla '{table_name_with_suffix}' borrada correctamente del catálogo (registro mantenido en control)."
                    )

                except Exception as e:
                    logger.error(
                        f"❌ Error al borrar tabla '{table_info['delta_name']}': {e}"
                    )

            logger.info(
                f"✅ Proceso completado. {deleted_count} tablas borradas de {len(deletable_tables)} encontradas para subetapa {current_sr_subetapa}."
            )

        except Exception as e:
            logger.error(f"❌ Error en cleanup_delta_tables_by_notebooks: {e}")
            raise

    def list_delta_tables_by_notebook(self, notebook_name=None):
        """
        Lista las tablas Delta registradas, opcionalmente filtradas por notebook.
        Si no se especifica notebook_name, detecta automáticamente el notebook actual.
        Acepta un solo notebook (str) o una lista de notebooks.

        Args:
            notebook_name (str or list, opcional): Nombre del notebook o lista de notebooks para filtrar.
                                                 Si es None, usa el notebook actual.

        Returns:
            DataFrame: DataFrame con las tablas Delta registradas.
        """
        try:
            control_table = f"{self.catalog}.{self.schema}.delta_tables_control"

            # Verificar si la tabla de control existe
            tables = (
                self.spark.sql(f"SHOW TABLES IN {self.schema}")
                .filter(f"tableName = 'delta_tables_control'")
                .collect()
            )

            if not tables:
                logger.warning("⚠️ No existe tabla de control de tablas Delta.")
                return self.spark.createDataFrame([], self.spark.sql("SELECT 1").schema)

            # Si no se especifica notebook_name, detectar automáticamente
            if notebook_name is None:
                notebook_name = self._get_notebook_name()
                logger.info(f"🔍 Detectando automáticamente notebook: {notebook_name}")

            # Manejar diferentes tipos de entrada
            if isinstance(notebook_name, str):
                # Un solo notebook
                query = f"SELECT * FROM {control_table} WHERE notebook_name = '{notebook_name}'"
                logger.info(f"📋 Listando tablas Delta para notebook: {notebook_name}")
            elif isinstance(notebook_name, list):
                # Lista de notebooks
                notebook_list = [f"'{name}'" for name in notebook_name]
                query = f"SELECT * FROM {control_table} WHERE notebook_name IN ({','.join(notebook_list)})"
                logger.info(
                    f"📋 Listando tablas Delta para notebooks: {', '.join(notebook_name)}"
                )
            else:
                raise ValueError(
                    f"❌ Tipo de dato no soportado para notebook_name: {type(notebook_name)}"
                )

            return self.spark.sql(query)

        except Exception as e:
            logger.error(f"❌ Error al listar tablas Delta: {e}")
            raise

    def read_delta(
        self,
        delta_name,
        query=None,
        enable_caching=False,
        cache_level="MEMORY_AND_DISK",
    ):
        """
        Lee datos de una tabla Delta con optimizaciones de rendimiento.

        Args:
            delta_name (str): Nombre de la tabla Delta.
            query (str, opcional): Consulta SQL personalizada. Si no se proporciona, devuelve toda la tabla.
            enable_caching (bool): Si cachear el DataFrame resultante para lecturas futuras.
            cache_level (str): Nivel de cache ("MEMORY_ONLY", "MEMORY_AND_DISK", "DISK_ONLY").

        Returns:
            DataFrame: DataFrame resultante optimizado.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}"

        # 🔹 Construir query optimizada
        if query:
            # Si hay filtros específicos, aplicarlos directamente en la query
            final_query = f"SELECT * FROM {full_table_name} WHERE {query}"
        else:
            final_query = f"SELECT * FROM {full_table_name}"

        logger.info(f"📖 Leyendo tabla Delta {full_table_name} con query optimizada")

        # 🔹 Ejecutar query con optimizaciones
        df = self.spark.sql(final_query)

        # 🔹 Aplicar cache inteligente si está habilitado
        if enable_caching:
            try:
                # Mapear niveles de cache
                cache_levels = {
                    "MEMORY_ONLY": "MEMORY_ONLY",
                    "MEMORY_AND_DISK": "MEMORY_AND_DISK",
                    "DISK_ONLY": "DISK_ONLY",
                    "MEMORY_ONLY_SER": "MEMORY_ONLY_SER",
                    "MEMORY_AND_DISK_SER": "MEMORY_AND_DISK_SER",
                }

                if cache_level in cache_levels:
                    from pyspark import StorageLevel

                    storage_level = getattr(StorageLevel, cache_levels[cache_level])
                    df = df.persist(storage_level)
                    logger.info(f"💾 DataFrame cacheado con nivel: {cache_level}")
                else:
                    df = df.cache()  # Cache por defecto
                    logger.info("💾 DataFrame cacheado con nivel por defecto")

            except Exception as e:
                logger.warning(f"⚠️ No se pudo aplicar cache: {e}")

        return df

    def drop_delta(self, delta_name):
        """
        Elimina una tabla Delta si existe, respetando el parámetro debug del ConfManager.

        Si debug=True en el ConfManager del notebook, la tabla NO será eliminada.
        Si debug=False o no se encuentra el ConfManager, la tabla SÍ será eliminada.

        Args:
            delta_name (str): Nombre de la tabla Delta a eliminar.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}".lower()

        # Obtener el valor de debug del ConfManager
        debug_mode = self._get_debug_param()

        if debug_mode:
            logger.info(
                f"🔍 [DEBUG MODE] No se eliminará la tabla Delta {full_table_name} porque debug=True"
            )
            return

        # Si debug=False o no se encontró ConfManager, proceder con la eliminación
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name} PURGE")
            logger.info(f"✅ Tabla Delta {full_table_name} eliminada correctamente.")
        except Exception as e:
            logger.error(f"❌ Error al eliminar tabla Delta {full_table_name}: {e}")
            raise

    def _drop_delta_force(self, delta_name):
        """
        Elimina una tabla Delta si existe, SIN respetar el parámetro debug.

        Esta función SIEMPRE elimina la tabla Delta, independientemente del valor de debug.
        Se usa internamente en write_delta cuando method="overwrite" para garantizar
        que la tabla se borre antes de escribir nuevos datos.

        Args:
            delta_name (str): Nombre de la tabla Delta a eliminar.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}".lower()

        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name} PURGE")
            logger.info(
                f"🗑️ [FORCE DROP] Tabla Delta {full_table_name} eliminada forzadamente (ignorando debug)."
            )
        except Exception as e:
            logger.error(
                f"❌ Error al eliminar tabla Delta {full_table_name} (force): {e}"
            )
            raise

    def sql_delta(self, query):
        """
        Ejecuta una consulta SQL sobre Delta usando Spark.

        Args:
            query (str): Consulta SQL a ejecutar.

        Returns:
            DataFrame: DataFrame resultante de la consulta.
        """
        if not query:
            raise ValueError("El parámetro 'query' no puede estar vacío.")

        logger.info(f"Ejecutando consulta SQL: {query}")
        return self.spark.sql(query)

    def optimize_delta_table(
        self, delta_name, z_order_columns=None, vacuum_hours=168, analyze_stats=True
    ):
        """
        Optimiza una tabla Delta existente para máximo rendimiento de lectura.

        Args:
            delta_name (str): Nombre de la tabla Delta a optimizar.
            z_order_columns (list, opcional): Columnas para Z-ORDER BY optimization.
            vacuum_hours (int): Horas de retención para VACUUM (por defecto 7 días = 168 horas).
            analyze_stats (bool): Si actualizar estadísticas de tabla.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{delta_name}".lower()

        try:
            logger.info(f"🔧 Iniciando optimización de tabla Delta: {full_table_name}")

            # 1. OPTIMIZE con Z-ORDER si se especificaron columnas
            if z_order_columns and len(z_order_columns) > 0:
                z_order_cols = ", ".join(z_order_columns)
                logger.info(
                    f"🎯 Aplicando OPTIMIZE con Z-ORDER en columnas: {z_order_cols}"
                )
                self.spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({z_order_cols})")
            else:
                # OPTIMIZE básico para compactar archivos pequeños
                logger.info(f"🔧 Aplicando OPTIMIZE básico para compactar archivos")
                self.spark.sql(f"OPTIMIZE {full_table_name}")

            # 2. VACUUM para limpiar archivos antiguos (mejora rendimiento de scan)
            logger.info(f"🧹 Aplicando VACUUM con retención de {vacuum_hours} horas")
            self.spark.sql(f"VACUUM {full_table_name} RETAIN {vacuum_hours} HOURS")

            # 3. ANALYZE TABLE para estadísticas actualizadas
            if analyze_stats:
                logger.info(f"📈 Actualizando estadísticas de tabla")
                self.spark.sql(
                    f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
                )

            # 4. Mostrar información de la tabla optimizada
            try:
                table_info = self.spark.sql(
                    f"DESCRIBE DETAIL {full_table_name}"
                ).collect()[0]
                num_files = table_info["numFiles"]
                size_mb = round(table_info["sizeInBytes"] / (1024 * 1024), 2)
                logger.info(
                    f"✅ Optimización completada. Archivos: {num_files}, Tamaño: {size_mb} MB"
                )
            except Exception as e:
                logger.warning(
                    f"⚠️ No se pudo obtener información detallada de la tabla: {e}"
                )

        except Exception as e:
            logger.error(f"❌ Error al optimizar tabla Delta {full_table_name}: {e}")
            raise

    def batch_optimize_delta_tables(
        self,
        table_patterns=None,
        z_order_configs=None,
        vacuum_hours=168,
        max_concurrent=3,
    ):
        """
        Optimiza múltiples tablas Delta en paralelo para máximo rendimiento.

        Args:
            table_patterns (list): Patrones de nombres de tabla (ej: ['demo_*', 'prod_*']).
            z_order_configs (dict): Configuración de Z-ORDER por tabla {'tabla': ['col1', 'col2']}.
            vacuum_hours (int): Horas de retención para VACUUM.
            max_concurrent (int): Máximo número de optimizaciones concurrentes.
        """
        import concurrent.futures
        import re

        try:
            # Obtener lista de todas las tablas Delta
            all_tables = self.spark.sql(f"SHOW TABLES IN {self.schema}").collect()
            delta_tables = []

            for table in all_tables:
                table_name = table["tableName"]

                # Filtrar por patrones si se especificaron
                if table_patterns:
                    for pattern in table_patterns:
                        # Convertir patrón de glob a regex
                        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
                        if re.match(f"^{regex_pattern}$", table_name):
                            delta_tables.append(table_name)
                            break
                else:
                    delta_tables.append(table_name)

            if not delta_tables:
                logger.info("ℹ️ No se encontraron tablas Delta para optimizar")
                return

            logger.info(
                f"🔧 Iniciando optimización de {len(delta_tables)} tablas Delta"
            )

            # Función para optimizar una tabla individual
            def optimize_single_table(table_name):
                try:
                    z_order_cols = (
                        z_order_configs.get(table_name) if z_order_configs else None
                    )
                    self.optimize_delta_table(
                        table_name,
                        z_order_columns=z_order_cols,
                        vacuum_hours=vacuum_hours,
                    )
                    return f"✅ {table_name}"
                except Exception as e:
                    return f"❌ {table_name}: {str(e)}"

            # Ejecutar optimizaciones en paralelo
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_concurrent
            ) as executor:
                results = list(executor.map(optimize_single_table, delta_tables))

            # Mostrar resultados
            successful = [r for r in results if r.startswith("✅")]
            failed = [r for r in results if r.startswith("❌")]

            logger.info(f"🎉 Optimización masiva completada:")
            logger.info(f"   ✅ Exitosas: {len(successful)}")
            logger.info(f"   ❌ Fallidas: {len(failed)}")

            if failed:
                logger.warning("Tablas con errores:")
                for error in failed:
                    logger.warning(f"   {error}")

        except Exception as e:
            logger.error(f"❌ Error en optimización masiva: {e}")
            raise

    def help(self):
        """
        Muestra una descripción detallada de los métodos disponibles en la clase DBXConnectionManager.
        """
        help_text = """
        🚀 **DBXConnectionManager** - Administración de conexiones JDBC en Databricks
        ✅ **CONFIGURACIÓN ESTABLE Y PROBADA PARA AZURE DATABRICKS** ✅
        ⚡ **OPTIMIZADO PARA ESCRITURA DELTA RÁPIDA Y SEGURA** ⚡

        **🔹 Métodos de Configuración y Gestión de Conexiones:**
        - `create_or_update_config(conn_name, fetch_size="20000", batch_size="20000", num_partitions="100")`
        - Crea o actualiza una configuración de conexión en la tabla Delta.

        - `list_configs()`
        - Lista todas las configuraciones disponibles en la tabla Delta.

        - `get_config(conn_name)`
        - Obtiene los detalles de una configuración específica.

        - `delete_config(conn_name)`
        - Elimina una configuración de conexión existente.

        **🔹 Métodos OPTIMIZADOS para Lectura y Escritura de Datos (JDBC y Delta Lake):**
        - `read_data(conn_name="default", sql_query=None)`
        - Lee datos desde una base de datos usando JDBC.

        - `write_data(df, table_name, conn_name="default", mode="overwrite")`
        - Escribe datos en una base de datos usando JDBC.

        - `write_delta(delta_name, dataframe, method="overwrite", fast_write_mode=True, post_write_optimize=False)`
        - ⚡ ESCRITURA DELTA CONFIGURABLE con:
          * fast_write_mode=True: MÁXIMA VELOCIDAD (por defecto) - Sin optimizaciones post-escritura
          * fast_write_mode=False: Modo completo con optimizaciones
          * post_write_optimize=True: Ejecutar OPTIMIZE después (ralentiza pero mejora lecturas futuras)
          * post_write_analyze=True: Actualizar estadísticas (ralentiza pero mejora query planning)
          * Optimización inteligente de particiones solo cuando es necesario
          * Liquid Clustering support (DBR 13.3+)

        - `write_delta_fast(delta_name, dataframe, method="overwrite", max_retries=3)`
        - ⚡ ESCRITURA DELTA ULTRA-RÁPIDA - CONCURRENCIA OPTIMIZADA:
          * Manejo automático de conflictos de concurrencia con reintentos
          * Backoff exponencial con jitter para evitar thundering herd
          * Detección inteligente de errores de transacciones concurrentes
          * Aislamiento WriteSerializable para máxima consistencia
          * Configuraciones específicas para múltiples notebooks
          * Solo procesos esenciales para máxima velocidad

        - `read_delta(delta_name, query=None, enable_caching=True, cache_level="MEMORY_AND_DISK")`
        - ⚡ LECTURA DELTA OPTIMIZADA con:
          * Cache inteligente configurable (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
          * Push-down de filtros optimizado
          * Aprovecha estadísticas de tabla para query planning

        - `optimize_delta_table(delta_name, z_order_columns=None, vacuum_hours=168, analyze_stats=True)`
        - ⚡ OPTIMIZACIÓN INDIVIDUAL de tabla Delta:
          * OPTIMIZE con Z-ORDER BY opcional
          * VACUUM para limpiar archivos antiguos
          * ANALYZE TABLE para estadísticas actualizadas
          * Información detallada post-optimización

        - `batch_optimize_delta_tables(table_patterns=None, z_order_configs=None, vacuum_hours=168, max_concurrent=3)`
        - ⚡ OPTIMIZACIÓN MASIVA en paralelo:
          * Optimiza múltiples tablas simultáneamente
          * Filtrado por patrones de nombres
          * Configuración Z-ORDER personalizada por tabla
          * Ejecución concurrente controlada

        **✅ Métodos de Validación y Monitoreo:**
        - `validate_cluster_configuration()`
        - 🔍 VALIDACIÓN DE CLUSTER ESTABLE:
          * Verifica configuraciones críticas para escritura Delta
          * Detecta problemas de configuración antes de errores
          * Recomendaciones específicas para tu cluster
          * Validación de Photon, AQE, particiones y Delta Lake

        - `get_cluster_concurrency_status()`
        - 📊 ESTADO DEL CLUSTER:
          * Información de configuraciones críticas
          * Métricas de memoria y cache
          * Estado de scheduler y recursos

        - `monitor_concurrent_operations(operation_name="operacion", duration_seconds=30)`
        - 🔍 MONITOREO EN TIEMPO REAL:
          * Supervisa jobs y tasks activos
          * Detecta contención y saturación
          * Alertas de problemas de rendimiento

        - `drop_delta(delta_name)`
        - Elimina una tabla Delta si existe.

        - `sql_delta(query)`
        - Ejecuta una consulta SQL sobre Delta usando Spark.

        **🔹 Métodos para Gestión de Tablas Delta con Control:**
        - `cleanup_delta_tables_by_notebooks(notebook_names=None, table_suffix=None)`
        - Borra todas las tablas Delta que coincidan con la subetapa detectada automáticamente
          y el table_suffix proporcionado. Identifica automáticamente la sr_subetapa actual
          y busca tablas de TODOS los notebooks que coincidan con esa subetapa.
          Si se especifica notebook_names, filtra solo esos notebooks.
          table_suffix: sufijo para completar el nombre de la tabla (ej: "123" para "demo_delta_001_123").
          Borra tablas con allow_delete=True que coincidan con la subetapa actual,
          o tablas con allow_delete=False que tengan delete_until igual a la subetapa actual.
          sr_subetapa y delete_until se manejan como enteros para comparación correcta.

        - `list_delta_tables_by_notebook(notebook_name=None)`
        - Lista las tablas Delta registradas, opcionalmente filtradas por notebook.
          Si no se especifica notebook_name, detecta automáticamente el notebook actual.
          Acepta un solo notebook (str) o una lista de notebooks.

        **🔹 Métodos para Administración de Conexiones y Seguridad:**
        - `execute_oci_dml(statement, async_mode=False)`
        - Ejecuta consultas DML en OCI a través de un notebook en Databricks.

        - `_get_credentials_from_scope(scope, user_key, pass_key)`
        - Obtiene credenciales almacenadas en un Scope de Databricks.

        **🔹 Métodos Internos de Soporte:**
        - `_ensure_dict(value)`
        - Convierte un valor en un diccionario si no lo es.

        - `_ensure_table_exists()`
        - Verifica si la tabla de configuraciones existe y la crea si es necesario.

        - `_get_notebook_params()`
        - Obtiene los parámetros del notebook actual usando el approach de handler.py.

        - `_get_sr_subetapa()`
        - Obtiene el parámetro sr_subetapa del notebook actual y lo convierte a int.

        - `_register_delta_table(delta_name, notebook_name, allow_delete=True, sr_subetapa=None, delete_until=None)`
        - Registra una tabla Delta en la tabla de control con información de subetapa.

        **📌 Nota:**
        Para más detalles sobre cada método, revisa la documentación dentro del código o usa `help(<método>)`.
        """
        logger.info(help_text)
