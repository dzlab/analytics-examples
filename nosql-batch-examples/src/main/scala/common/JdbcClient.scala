package common

import java.sql.{DriverManager, Connection, PreparedStatement, ResultSet, ResultSetMetaData, Timestamp}

case class JdbcClient(connection: String, user: String, password: String) {
  val con: Connection = {
    Class.forName("nl.cwi.monetdb.jdbc.MonetDriver") // load the driver
    DriverManager.getConnection(connection, user, password)
  }

  def execute(query: String): ResultSet = {
    val st = con.createStatement()
    st.executeQuery(query)
  }

  def execute(query: String, withResult: Boolean) {
    val st = con.createStatement()  
    if(!withResult) {
      st.execute(query)
      return
    }
    val rs: ResultSet = st.executeQuery(query)
    //if(rs.isAfterLast()) return // no rows 
    val md: ResultSetMetaData = rs.getMetaData()
    (1 to md.getColumnCount()).foreach(i => print(md.getColumnName(i) + ":" + md.getColumnTypeName(i) + "\t"))
    println("-------")  
    while (rs.next()) {
      (1 to md.getColumnCount()).foreach(j => print(rs.getString(j) + "\t"))
      println("")
    }
  }

  def close() {
    con.close()
  }
}
