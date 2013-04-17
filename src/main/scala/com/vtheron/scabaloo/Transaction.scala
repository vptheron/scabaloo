package com.vtheron.scabaloo

import java.sql._
import org.slf4j.LoggerFactory
import collection.immutable
import collection.mutable.ListBuffer

// Cannot be created outside of the package. The connection is package visible for testing purposes (not great but oh well).
class Transaction private[scabaloo](private[scabaloo] val connection: Connection) {

  import Transaction._

  def query[A](query: String, parameters: Any*)(extractor: (Record) => A): immutable.Seq[A] =
    withStatement(query, parameters: _*) {
      statement =>
        val rs = statement.executeQuery()
        try {
          val result = new ListBuffer[A]
          while (rs.next()) {
            result += extractor(new Record(rs, rs.getRow))
          }
          result.toList
        } finally {
          rs.close()
        }
    }

  def statement(statement: String, parameters: Any*): Int = withStatement(statement, parameters: _*)(_.executeUpdate())

  private def withStatement[A](query: String, parameters: Any*)(f: PreparedStatement => A): A = {
    val statement = connection.prepareStatement(query)
    try {
      (0 until parameters.length).foreach(i => setParam(statement, i + 1, parameters(i)))
      f(statement)
    } finally {
      statement.close()
    }
  }

}

object Transaction {

  private val logger = LoggerFactory.getLogger(classOf[Transaction])

  // Package visible for testing purposes.
  private[scabaloo] def setParam(statement: PreparedStatement, index: Int, element: Any) {
    element match {
      case null => throw new NullPointerException("Element at index " + index + " is null! Use NullType instance instead.")
      case d: BigDecimal => statement.setBigDecimal(index, d.bigDecimal)
      case b: Boolean => statement.setBoolean(index, b)
      case d: Date => statement.setDate(index, d)
      case d: Double => statement.setDouble(index, d)
      case f: Float => statement.setFloat(index, f)
      case i: Int => statement.setInt(index, i)
      case l: Long => statement.setLong(index, l)
      case s: Short => statement.setShort(index, s)
      case s: String => statement.setString(index, s)
      case t: Time => statement.setTime(index, t)
      case t: Timestamp => statement.setTimestamp(index, t)
      case Some(o) => setParam(statement, index, o)
      case NullType(sqlType) => statement.setNull(index, sqlType)
      case None => {
        logger.warn("Element for index " + index + " is None. " +
          "Trying to set sql value to null, might fail if the driver does not support it. Consider using NullType instead.")
        statement.setObject(index, null)
      }
      case _ => {
        logger.warn("Element " + element + " for index " + index + " has unsupported type " + element.getClass + ". " +
          "Trying to set it as an object, might fail if the driver does not support it.")
        statement.setObject(index, element)
      }
    }
  }

}