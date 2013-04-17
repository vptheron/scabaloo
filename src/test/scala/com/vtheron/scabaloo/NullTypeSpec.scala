package com.vtheron.scabaloo

import org.specs2.mutable.Specification
import java.sql.Types

class NullTypeSpec extends Specification {

  import NullType._

  "A NullType object" should {

    "offer valid null type instances" in {
      nullBigDecimal.sqlType must beEqualTo(Types.NUMERIC)
      nullBoolean.sqlType must beEqualTo(Types.BOOLEAN)
      nullDate.sqlType must beEqualTo(Types.DATE)
      nullDouble.sqlType must beEqualTo(Types.DOUBLE)
      nullFloat.sqlType must beEqualTo(Types.FLOAT)
      nullInt.sqlType must beEqualTo(Types.INTEGER)
      nullLong.sqlType must beEqualTo(Types.BIGINT)
      nullShort.sqlType must beEqualTo(Types.SMALLINT)
      nullString.sqlType must beEqualTo(Types.VARCHAR)
      nullTime.sqlType must beEqualTo(Types.TIME)
      nullTimestamp.sqlType must beEqualTo(Types.TIMESTAMP)
    }

  }

}
