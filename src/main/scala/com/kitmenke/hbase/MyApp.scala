package com.kitmenke.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.collection.JavaConverters._

object MyApp {
  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "cdh01.hourswith.expert:2181,cdh02.hourswith.expert:2181,cdh03.hourswith.expert:2181")
      connection = ConnectionFactory.createConnection(conf)
      question1(connection)
      question2(connection)
      question3(connection)
      question4(connection)
      question5(connection)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }

  def question1(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("ljohnson:users"))
    val get = new Get(Bytes.toBytes("10000001"))
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
    val result = table.get(get)
    val q1 = Bytes.toString(
      result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail"))
    )
    logger.debug(q1)
  }

  def question2(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("ljohnson:users"))
    val put = new Put (Bytes.toBytes("99"))
    put.addColumn(
      Bytes.toBytes("f1"),
      Bytes.toBytes("username"),
      Bytes.toBytes("DE-HWE"),
    )
    put.addColumn(
      Bytes.toBytes("f1"),
      Bytes.toBytes("name"),
      Bytes.toBytes("The Panther"),
    )
    put.addColumn(
      Bytes.toBytes("f1"),
      Bytes.toBytes("sex"),
      Bytes.toBytes("F"),
    )
    put.addColumn(
      Bytes.toBytes("f1"),
      Bytes.toBytes("favorite_color"),
      Bytes.toBytes("pink"),
    )
    table.put(put)
  }

  def question3(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("ljohnson:users"))
    val scan = new Scan().withStartRow(Bytes.toBytes("10000001")).withStopRow(Bytes.toBytes("10006001"))
    val scanner = table.getScanner(scan)
    val count = scanner.iterator().asScala.length
    logger.debug(count)
  }

  def question4(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("ljohnson:users"))
    val delete = new Delete(Bytes.toBytes("99"))
    table.delete(delete)
  }

  def question5(connection: Connection): Unit = {
    val table = connection.getTable(TableName.valueOf("ljohnson:users"))
    import scala.collection.JavaConverters._
    val gets = List("9005729", "500600", "30059640", "6005263", "800182").map(x => new Get(Bytes.toBytes(x)).addColumn(Bytes.toBytes("f1"), Bytes.toBytes("mail"))).asJava

    val result = table.get(gets)
    result.foreach(x => logger.debug(Bytes.toString(x.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))))
  }
}
