package cassandra

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.JavaConverters._

class QueryBuilder {
  var tableName = ""
  var columnNames = ""

  def withTable(namespace: String, table: String) {
    tableName = namespace + "." + table
  }

  def withDimensions(dimensions: List[String]) {
    require(dimensions != null, "List of dimentions is null.")
    columnNames = dimensions.reduce(_ + ", " + _)
  }

  def withAggregas(metrics: List[String]) {
    require(metrics != null, "List of metrics is null.")
    columnNames = columnNames + metrics.reduce(_ + ", " + _)
  }

  def execute(sc: SparkContext, conf: SparkConf): Unit = {
    CassandraConnector(conf).withSessionDo { session => 
      // get the data as an RDD
      val results: ResultSet = session.execute(s"SELECT $columnNames FROM $tableName")
      val collection = results.all.asScala
      val rdd = sc.parallelize(collection)
      // execute the aggregations
    }
  }
}
