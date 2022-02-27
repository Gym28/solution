package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JdbcProvisioner {

  def main(args: Array[String]) {
    val IpServer = "localhost"

    // connect to the database named "mysql" on the localhost
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "mysecretpassword"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

      println("Creando la tabla metadata (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS metadata (id TEXT, model TEXT, version TEXT, location TEXT)")

      println("Creando la tabla antenna_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")

      println("Creando la tabla antenna_1h_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_1h_agg (location TEXT, date TIMESTAMP, avg_devices_count BIGINT, max_devices_count BIGINT, min_devices_count BIGINT)")

      println("Creando la tabla antenna_errors_agg (model TEXT, version TEXT, antennas_num BIGINT, date TIMESTAMP)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_errors_agg (model TEXT, version TEXT, antennas_num BIGINT, date TIMESTAMP)")

      println("Creando la tabla antenna_percent_agg (date TIMESTAMP, id TEXT, enable DOUBLE PRECISION, disable DOUBLE PRECISION, error DOUBLE PRECISION)")
      statement.execute("CREATE TABLE IF NOT EXISTS antenna_percent_agg (date TIMESTAMP, id TEXT, enable DOUBLE PRECISION, disable DOUBLE PRECISION, error DOUBLE PRECISION)")

      println("Dando de alta la información de usuarios")
      statement.execute("INSERT INTO metadata (id, model, version, location) VALUES ('00000000-0000-0000-0000-000000000000', 'CH-2020', '1.0.0', '36.454685, -6.067934')")
      statement.execute("INSERT INTO metadata (id, model, version, location) VALUES ('11111111-1111-1111-1111-111111111111', 'CH-2121', '1.2.0', '36.459686, -6.113992')")
      statement.execute("INSERT INTO metadata (id, model, version, location) VALUES ('22222222-2222-2222-2222-222222222222', 'CH-2020', '1.0.1', '36.478596, -5.968923')")
      statement.execute("INSERT INTO metadata (id, model, version, location) VALUES ('33333333-3333-3333-3333-333333333333', 'CH-3311', '1.0.0', '36.420476, -5.822540')")
      statement.execute("INSERT INTO metadata (id, model, version, location) VALUES ('44444444-4444-4444-4444-444444444444', 'CH-2121', '1.2.0', '36.297735, -5.835083')")

    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
