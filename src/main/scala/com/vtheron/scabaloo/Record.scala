package com.vtheron.scabaloo

import java.sql.{Date, Time, Timestamp, ResultSet}

// A Record cannot be created outside of the package, the ResultSet is hidden.
class Record private[scabaloo](rs: ResultSet, val rowNumber: Int) {

  def intAtIndex(idx: Int): Int = throwIfNull(rs.getInt: Int => Int, idx)

  def stringAtIndex(idx: Int): String = throwIfNull(rs.getString: Int => String, idx)

  def longAtIndex(idx: Int): Long = throwIfNull(rs.getLong: Int => Long, idx)

  def doubleAtIndex(idx: Int): Double = throwIfNull(rs.getDouble: Int => Double, idx)

  def floatAtIndex(idx: Int): Float = throwIfNull(rs.getFloat: Int => Float, idx)

  def shortAtIndex(idx: Int): Short = throwIfNull(rs.getShort: Int => Short, idx)

  def booleanAtIndex(idx: Int): Boolean = throwIfNull(rs.getBoolean: Int => Boolean, idx)

  def bigDecimalAtIndex(idx: Int): BigDecimal = {
    val bd: java.math.BigDecimal = throwIfNull(rs.getBigDecimal: Int => java.math.BigDecimal, idx)
    BigDecimal(bd)
  }

  def timestampAtIndex(idx: Int): Timestamp = throwIfNull(rs.getTimestamp: Int => Timestamp, idx)

  def timeAtIndex(idx: Int): Time = throwIfNull(rs.getTime: Int => Time, idx)

  def dateAtIndex(idx: Int): Date = throwIfNull(rs.getDate: Int => Date, idx)

  def objectAtIndex(idx: Int): AnyRef = throwIfNull(rs.getObject: Int => AnyRef, idx)

  def intInColumn(name: String): Int = throwIfNull(rs.getInt: String => Int, name)

  def stringInColumn(name: String): String = throwIfNull(rs.getString: String => String, name)

  def longInColumn(name: String): Long = throwIfNull(rs.getLong: String => Long, name)

  def doubleInColumn(name: String): Double = throwIfNull(rs.getDouble: String => Double, name)

  def floatInColumn(name: String): Float = throwIfNull(rs.getFloat: String => Float, name)

  def shortInColumn(name: String): Short = throwIfNull(rs.getShort: String => Short, name)

  def booleanInColumn(name: String): Boolean = throwIfNull(rs.getBoolean: String => Boolean, name)

  def bigDecimalInColumn(name: String): BigDecimal = {
    val bd: java.math.BigDecimal = throwIfNull(rs.getBigDecimal: String => java.math.BigDecimal, name)
    BigDecimal(bd)
  }

  def timestampInColumn(name: String): Timestamp = throwIfNull(rs.getTimestamp: String => Timestamp, name)

  def timeInColumn(name: String): Time = throwIfNull(rs.getTime: String => Time, name)

  def dateInColumn(name: String): Date = throwIfNull(rs.getDate: String => Date, name)

  def objectInColumn(name: String): AnyRef = throwIfNull(rs.getObject: String => AnyRef, name)

  def intAtIndex_?(idx: Int): Option[Int] = toOption(rs.getInt(idx))

  def stringAtIndex_?(idx: Int): Option[String] = toOption(rs.getString(idx))

  def longAtIndex_?(idx: Int): Option[Long] = toOption(rs.getLong(idx))

  def doubleAtIndex_?(idx: Int): Option[Double] = toOption(rs.getDouble(idx))

  def floatAtIndex_?(idx: Int): Option[Float] = toOption(rs.getFloat(idx))

  def shortAtIndex_?(idx: Int): Option[Short] = toOption(rs.getShort(idx))

  def booleanAtIndex_?(idx: Int): Option[Boolean] = toOption(rs.getBoolean(idx))

  def bigDecimalAtIndex_?(idx: Int): Option[BigDecimal] =
    toOption(rs.getBigDecimal(idx))
      .map((bd: java.math.BigDecimal) => BigDecimal(bd))

  def timestampAtIndex_?(idx: Int): Option[Timestamp] = toOption(rs.getTimestamp(idx))

  def timeAtIndex_?(idx: Int): Option[Time] = toOption(rs.getTime(idx))

  def dateAtIndex_?(idx: Int): Option[Date] = toOption(rs.getDate(idx))

  def objectAtIndex_?(idx: Int): Option[AnyRef] = toOption(rs.getObject(idx))

  def intInColumn_?(name: String): Option[Int] = toOption(rs.getInt(name))

  def stringInColumn_?(name: String): Option[String] = toOption(rs.getString(name))

  def longInColumn_?(name: String): Option[Long] = toOption(rs.getLong(name))

  def doubleInColumn_?(name: String): Option[Double] = toOption(rs.getDouble(name))

  def floatInColumn_?(name: String): Option[Float] = toOption(rs.getFloat(name))

  def shortInColumn_?(name: String): Option[Short] = toOption(rs.getShort(name))

  def booleanInColumn_?(name: String): Option[Boolean] = toOption(rs.getBoolean(name))

  def bigDecimalInColumn_?(name: String): Option[BigDecimal] =
    toOption(rs.getBigDecimal(name))
      .map((bd: java.math.BigDecimal) => BigDecimal(bd))

  def timestampInColumn_?(name: String): Option[Timestamp] = toOption(rs.getTimestamp(name))

  def timeInColumn_?(name: String): Option[Time] = toOption(rs.getTime(name))

  def dateInColumn_?(name: String): Option[Date] = toOption(rs.getDate(name))

  def objectInColumn_?(name: String): Option[AnyRef] = toOption(rs.getObject(name))

  private def throwIfNull[A](rsGetter: (Int) => A, idx: Int): A = {
    val value = rsGetter(idx)
    if (rs.wasNull || value == null) throw new NullPointerException("Element at index " + idx + " was null.") else value
  }

  private def throwIfNull[A](rsGetter: (String) => A, name: String): A = {
    val value = rsGetter(name)
    if (rs.wasNull || value == null) throw new NullPointerException("Element in column " + name + " was null.") else value
  }

  private def toOption[A](a: A): Option[A] = if (!rs.wasNull) Some(a) else None

}