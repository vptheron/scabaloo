package com.vtheron.scabaloo

import java.sql.Types

// Not possible to create new instances of NullType.
final case class NullType private(sqlType: Int)

object NullType {
  val nullBigDecimal = NullType(Types.NUMERIC)
  val nullBoolean = NullType(Types.BOOLEAN)
  val nullDate = NullType(Types.DATE)
  val nullDouble = NullType(Types.DOUBLE)
  val nullFloat = NullType(Types.FLOAT)
  val nullInt = NullType(Types.INTEGER)
  val nullLong = NullType(Types.BIGINT)
  val nullShort = NullType(Types.SMALLINT)
  val nullString = NullType(Types.VARCHAR)
  val nullTime = NullType(Types.TIME)
  val nullTimestamp = NullType(Types.TIMESTAMP)
}
