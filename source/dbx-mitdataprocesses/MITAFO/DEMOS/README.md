# 📌 **DEMOS - Notebooks de Ejemplo en Databricks**

Este directorio contiene notebooks de demostración diseñados para validar y probar funcionalidades clave dentro del entorno de **Databricks**.

---

## 📂 **Estructura del Directorio**

```
DEMOS/
│── Jobs/
│   ├── Notebooks/
│   │   ├── demo.ipynb
│   │   ├── startup_notebook.ipynb
│   ├── README.md  ← (Este archivo)
```

---

## 📑 **Descripción de Notebooks**
### 📌 `demo.ipynb`

# 🚀 Notebook de Procesamiento de Datos

Este notebook de Databricks está diseñado para demostrar el uso del framework de procesamiento de datos. Aquí se detallan los pasos principales, las funciones utilizadas y cómo ejecutar cada sección del notebook.

## 📌 Pasos principales:
1. **Carga del Framework**: Se ejecuta un script de inicialización.
2. **Definición y Validación de Parámetros**: Se definen los parámetros de entrada del proceso.
3. **Carga de Configuraciones Locales**: Se establecen configuraciones generales.
4. **Procesamiento de Datos**: Se ejecutan transformaciones y consultas sobre los datos.
5. **Almacenamiento de Resultados**: Se guardan los resultados en la ubicación esperada.
6. **Limpieza de Datos**: Se eliminan variables, vistas temporales y caché al finalizar el proceso.

---

## ⚙️ Cargar el Framework

Para inicializar el framework, el notebook ejecuta el siguiente comando:

```python
%run "./startup"
```
Esto asegura que todas las dependencias y configuraciones estén listas antes de proceder con el procesamiento de datos.

---

## 📊 Definir y Validar Parámetros

Antes de ejecutar cualquier operación, se definen y validan los parámetros necesarios:

```python
params = WidgetParams({
    "sr_folio_rel": str,
    "sr_proceso": str,
    "sr_fecha_liq": (str, "YYYYMMDD"),
    "sr_tipo_mov": int,
    "sr_reproceso": str,
    "sr_subproceso": str,
    "sr_usuario": str,
    "sr_instancia_proceso": str,
    "sr_origen_arc": str,
    "sr_etapa": str,
    "sr_id_snapshot": int,
    "sr_fecha_acc": (str, "YYYYMMDD"),
    "sr_folio": str,
    "sr_subetapa": str,
})
params.validate()
```

---

## 🔧 Cargar Configuraciones Locales

Para manejar las configuraciones, se utiliza la clase `ConfManager()`:

```python
conf = ConfManager()
logger.info(conf.debug)
conf.validate(["debug", "err_repo_path", "output_file_name_001"])
logger.info(conf.help())
logger.info(conf.env_variables)
```

Esto permite acceder a las configuraciones de manera segura y validarlas antes de su uso.

---

## 🔍 Manejo de Queries

Se utiliza `QueryManager()` para gestionar consultas SQL:

```python
query = QueryManager()
display(query.get_sql_list())
```

Para obtener una consulta específica:

```python
statement_demo_001 = query.get_statement(
    "demo_001.sql",
    TTSISGRAL_ETL_MOVIMIENTOS_AUX_OR_MAIN=f"{conf.schema_001}.{conf.table_name_001}",
    SR_FOLIO=params.sr_folio,
    hints="/*+ PARALLEL(4) */",
)
```

---

## 🔗 Conexión a la Base de Datos

Se crea una instancia del manejador de conexiones:

```python
db = DBXConnectionManager()
db.help()
```

Para leer datos:

```python
df = db.read_data("default", statement_demo_001)
display(df)
```

Para escribir datos en una tabla Delta:

```python
db.write_delta("demo_delta_001", df, "overwrite")
```

Para leer datos desde una tabla Delta:

```python
df_from_delta = db.read_delta("demo_delta_001")
display(df_from_delta)
```

---

## 🗑 Limpieza y Cierre

Para eliminar caché, vistas temporales y variables generadas en el notebook, se ejecuta la siguiente función:

```python
CleanUpManager.cleanup_notebook()
```

Esto libera memoria y optimiza el rendimiento para futuras ejecuciones.

---

## 📢 Notificaciones

Para enviar notificaciones sobre el estado del proceso:

```python
Notify.send_notification("INFO", params)
```

---

## ⚠️ Manejo de Errores

Para manejar errores en tiempo de ejecución:

```python
def test():
    var1 = 1
    var2 = 0
    display(var1 / var2)  # Esto generará ZeroDivisionError
```

Si se requiere simular un error:

```python
#test()
```

---

## 📌 Conclusión
Este notebook proporciona una estructura completa para procesar datos en Databricks, asegurando validación de parámetros, conexión a bases de datos, manejo de consultas y una limpieza eficiente al finalizar el proceso.



### 📌 `startup_notebook.ipynb`
- **Objetivo**: Configurar el entorno de ejecución, asegurando la correcta carga de módulos y dependencias.
- **Acciones principales**:
  - Ajusta dinámicamente `sys.path` para incluir módulos desde la carpeta `CGRLS_0010`.
  - Importa y registra los principales módulos (`logger`, `DBXConnectionManager`, `SETTINGS`, `WidgetParams`, `Notify`, `custom_exception_handler`, `WidgetParams`, `QueryManager`, `ConfManager`).
  - **Establece un manejador global de excepciones** (`custom_exception_handler`) para capturar y reportar errores en la ejecución.

---

## 🔹 Flujo de Ejecución

### 1️⃣ Inicialización del Notebook
- Ejecuta `startup_notebook.ipynb` para cargar las configuraciones necesarias.
- Define y valida los parámetros de ejecución (`WidgetParams`).

### 2️⃣ Gestión de Queries y Conexiones
- Crea una instancia de `QueryManager` para manejar consultas SQL de forma dinámica.
- Instancia `DBXConnectionManager` para establecer conexión con **Oracle Cloud Infrastructure (OCI)**.

### 3️⃣ Consulta y Escritura de Datos
- Lee datos de la tabla `CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS` utilizando `DBXConnectionManager.read_data()`.
- Almacena los datos procesados en la tabla auxiliar `CIERREN_DATAUX.TTSISGRAL_ETL_MOVIMIENTOS_AUX` con `write_data()`.

### 4️⃣ Ejecución de Procesos DML en Segundo Plano
- Ejecuta `execute_oci_dml(statement, async_mode=True)` para ejecutar consultas DML **sin bloquear** el notebook.
- Usa `while not execution.done()` para monitorear la ejecución en segundo plano.

### 5️⃣ Manejo de Errores y Notificaciones
- Captura y reporta errores con `custom_exception_handler`.
- Envía notificaciones (`INFO` o `ERROR`) usando `Notify.send_notification()`.

### 6️⃣ Simulación de Error
- Se incluye una celda con un error intencional (`ZeroDivisionError`) para validar la captura de errores en el entorno de Databricks.
---

## 📌 **Ejemplo de Parámetros Utilizados (`WidgetParams`)**
```json
{
  "sr_folio_rel": "202501301728129327",
  "sr_proceso": "7",
  "sr_fecha_liq": "20250201",
  "sr_tipo_mov": "1",
  "sr_reproceso": "1",
  "sr_subproceso": "8",
  "sr_usuario": "APPIAN",
  "sr_instancia_proceso": "28568",
  "sr_origen_arc": "na",
  "sr_etapa": "1",
  "sr_id_snapshot": "1",
  "sr_fecha_acc": "20250201",
  "sr_folio": "202501301829469328",
  "sr_subetapa": "273"
}
```