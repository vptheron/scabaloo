package com.vtheron.scabaloo

import java.sql._
import javax.sql.DataSource
import collection.immutable

class Database(connectionProvider: () => Connection) {

  def query[A](query: String, parameters: Any*)(extractor: (Record) => A): immutable.Seq[A] =
    inTransaction(_.query(query, parameters: _*)(extractor))

  def statement(statement: String, parameters: Any*): Int =
    inTransaction(_.statement(statement, parameters: _*))

  def inTransaction[A](f: (Transaction) => A): A = {
    val connection = connectionProvider()
    connection.setAutoCommit(false)
    try {
      val result = f(new Transaction(connection))
      connection.commit()
      connection.setAutoCommit(true)
      result
    } catch {
      case e: Exception => {
        connection.rollback()
        connection.setAutoCommit(true)
        throw e
      }
    } finally {
      connection.close()
    }
  }

}

object Database {

  def withDataSource(dataSource: DataSource): Database = new Database(() => dataSource.getConnection)

}

