# Databricks notebook source
# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import java.sql.{Connection, DriverManager, SQLException}
# MAGIC import java.time.LocalDateTime
# MAGIC import java.time.temporal.ChronoUnit
# MAGIC import scala.util.{Try, Failure, Success}
# MAGIC import org.json4s._
# MAGIC import org.json4s.jackson.Serialization
# MAGIC import org.json4s.jackson.Serialization.write
# MAGIC
# MAGIC // Configuración para serialización JSON
# MAGIC implicit val formats: Formats = Serialization.formats(NoTypeHints)
# MAGIC
# MAGIC // Registrar el inicio de la ejecución
# MAGIC val startTime = LocalDateTime.now()
# MAGIC println(s"Inicio de ejecución: $startTime")
# MAGIC
# MAGIC // Recibir parámetros del notebook
# MAGIC val conn_user = dbutils.widgets.get("conn_user")
# MAGIC val conn_key = dbutils.widgets.get("conn_key")
# MAGIC val conn_url = dbutils.widgets.get("conn_url")
# MAGIC val conn_scope = dbutils.widgets.get("conn_scope")
# MAGIC val statement = dbutils.widgets.get("statement")
# MAGIC
# MAGIC println(s"Parámetros recibidos: conn_url=$conn_url, scope=$conn_scope")
# MAGIC
# MAGIC val driverClass = "oracle.jdbc.driver.OracleDriver"
# MAGIC Class.forName(driverClass)
# MAGIC
# MAGIC val connectionProperties = new Properties()
# MAGIC var connection: Connection = null
# MAGIC
# MAGIC // Variable para guardar la respuesta final
# MAGIC var response: String = ""
# MAGIC
# MAGIC try {
# MAGIC   val executionResult = Try {
# MAGIC     // Obtener credenciales
# MAGIC     val user = dbutils.secrets.get(scope = conn_scope, key = conn_user)
# MAGIC     val conn_key_oci = dbutils.secrets.get(scope = conn_scope, key = conn_key)
# MAGIC
# MAGIC     connectionProperties.setProperty("user", user)
# MAGIC     connectionProperties.setProperty("password", conn_key_oci)
# MAGIC     connectionProperties.setProperty("v$session.osuser", user)
# MAGIC
# MAGIC     // Conectar a la base de datos
# MAGIC     connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC     println("Conexión establecida con éxito.")
# MAGIC
# MAGIC     val stmt = connection.createStatement()
# MAGIC
# MAGIC     // Registrar la consulta
# MAGIC     println(s"Ejecutando SQL: $statement")
# MAGIC
# MAGIC     val affectedRows = stmt.executeUpdate(statement) // UPDATE/DELETE/INSERT
# MAGIC     println(s"Sentencia ejecutada con éxito. Filas afectadas: $affectedRows")
# MAGIC
# MAGIC     affectedRows
# MAGIC   } match {
# MAGIC     case Success(rowsAffected) =>
# MAGIC       val endTime = LocalDateTime.now()
# MAGIC       val duration = ChronoUnit.MILLIS.between(startTime, endTime)
# MAGIC       println(s"Ejecución finalizada en ${duration}ms")
# MAGIC
# MAGIC       val successResponse = Map(
# MAGIC         "status" -> "success",
# MAGIC         "message" -> "Consulta ejecutada correctamente",
# MAGIC         "affected_rows" -> rowsAffected,
# MAGIC         "execution_time_ms" -> duration,
# MAGIC         "timestamp" -> endTime.toString
# MAGIC       )
# MAGIC       response = write(successResponse)
# MAGIC
# MAGIC     case Failure(exception) =>
# MAGIC       val endTime = LocalDateTime.now()
# MAGIC       val duration = ChronoUnit.MILLIS.between(startTime, endTime)
# MAGIC       println(s"Error durante la ejecución tras ${duration}ms")
# MAGIC       println(s"Error: ${exception.getMessage}")
# MAGIC       exception.printStackTrace()
# MAGIC
# MAGIC       val errorResponse = Map(
# MAGIC         "status" -> "error",
# MAGIC         "message" -> exception.getMessage,
# MAGIC         "execution_time_ms" -> duration,
# MAGIC         "timestamp" -> endTime.toString
# MAGIC       )
# MAGIC       response = write(errorResponse)
# MAGIC   }
# MAGIC } finally {
# MAGIC   if (connection != null && !connection.isClosed) {
# MAGIC     connection.close()
# MAGIC     println("Conexión cerrada correctamente.")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // Salir del notebook con la respuesta final
# MAGIC dbutils.notebook.exit(response)
