/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.types

import scala.math.Numeric._
import scala.math.Ordering

import org.apache.spark.sql.types.Decimal.DecimalIsConflicted


object ByteExactNumeric extends ByteIsIntegral with Ordering.ByteOrdering {
  private def checkOverflow(res: Int, x: Byte, y: Byte, op: String): Unit = {
    if (res > Byte.MaxValue || res < Byte.MinValue) {
      throw new ArithmeticException(s"$x $op $y caused overflow.")
    }
  }

  override def plus(x: Byte, y: Byte): Byte = {
    val tmp = x + y
    checkOverflow(tmp, x, y, "+")
    tmp.toByte
  }

  override def minus(x: Byte, y: Byte): Byte = {
    val tmp = x - y
    checkOverflow(tmp, x, y, "-")
    tmp.toByte
  }

  override def times(x: Byte, y: Byte): Byte = {
    val tmp = x * y
    checkOverflow(tmp, x, y, "*")
    tmp.toByte
  }

  override def negate(x: Byte): Byte = {
    if (x == Byte.MinValue) { // if and only if x is Byte.MinValue, overflow can happen
      throw new ArithmeticException(s"- $x caused overflow.")
    }
    (-x).toByte
  }
}


object ShortExactNumeric extends ShortIsIntegral with Ordering.ShortOrdering {
  private def checkOverflow(res: Int, x: Short, y: Short, op: String): Unit = {
    if (res > Short.MaxValue || res < Short.MinValue) {
      throw new ArithmeticException(s"$x $op $y caused overflow.")
    }
  }

  override def plus(x: Short, y: Short): Short = {
    val tmp = x + y
    checkOverflow(tmp, x, y, "+")
    tmp.toShort
  }

  override def minus(x: Short, y: Short): Short = {
    val tmp = x - y
    checkOverflow(tmp, x, y, "-")
    tmp.toShort
  }

  override def times(x: Short, y: Short): Short = {
    val tmp = x * y
    checkOverflow(tmp, x, y, "*")
    tmp.toShort
  }

  override def negate(x: Short): Short = {
    if (x == Short.MinValue) { // if and only if x is Byte.MinValue, overflow can happen
      throw new ArithmeticException(s"- $x caused overflow.")
    }
    (-x).toShort
  }
}


object IntegerExactNumeric extends IntIsIntegral with Ordering.IntOrdering {
  override def plus(x: Int, y: Int): Int = Math.addExact(x, y)

  override def minus(x: Int, y: Int): Int = Math.subtractExact(x, y)

  override def times(x: Int, y: Int): Int = Math.multiplyExact(x, y)

  override def negate(x: Int): Int = Math.negateExact(x)
}

object LongExactNumeric extends LongIsIntegral with Ordering.LongOrdering {
  override def plus(x: Long, y: Long): Long = Math.addExact(x, y)

  override def minus(x: Long, y: Long): Long = Math.subtractExact(x, y)

  override def times(x: Long, y: Long): Long = Math.multiplyExact(x, y)

  override def negate(x: Long): Long = Math.negateExact(x)

  override def toInt(x: Long): Int =
    if (x == x.toInt) {
      x.toInt
    } else {
      throw new ArithmeticException(s"Casting $x to int causes overflow")
    }
}

object FloatExactNumeric extends FloatIsFractional with Ordering.FloatOrdering {
  private def overflowException(x: Float, dataType: String) =
    throw new ArithmeticException(s"Casting $x to $dataType causes overflow")

  private val intUpperBound = Int.MaxValue.toFloat
  private val intLowerBound = Int.MinValue.toFloat
  private val longUpperBound = Long.MaxValue.toFloat
  private val longLowerBound = Long.MinValue.toFloat

  override def toInt(x: Float): Int = {
    // When casting floating values to integral types, Spark uses the method `Numeric.toInt`
    // Or `Numeric.toLong` directly. For positive floating values, it is equivalent to `Math.floor`;
    // for negative floating values, it is equivalent to `Math.ceil`.
    // So, we can use the condition `Math.floor(x) <= upperBound && Math.ceil(x) >= lowerBound`
    // to check if the floating value x is in the range of an integral type after rounding.
    // This condition applies to converting Float/Double value to any integral types.
    if (Math.floor(x) <= intUpperBound && Math.ceil(x) >= intLowerBound) {
      x.toInt
    } else {
      overflowException(x, "int")
    }
  }

  override def toLong(x: Float): Long = {
    if (Math.floor(x) <= longUpperBound && Math.ceil(x) >= longLowerBound) {
      x.toLong
    } else {
      overflowException(x, "int")
    }
  }
}

object DoubleExactNumeric extends DoubleIsFractional with Ordering.DoubleOrdering {
  private def overflowException(x: Double, dataType: String) =
    throw new ArithmeticException(s"Casting $x to $dataType causes overflow")

  private val intUpperBound = Int.MaxValue.toDouble
  private val intLowerBound = Int.MinValue.toDouble
  private val longUpperBound = Long.MaxValue.toDouble
  private val longLowerBound = Long.MinValue.toDouble

  override def toInt(x: Double): Int = {
    if (Math.floor(x) <= intUpperBound && Math.ceil(x) >= intLowerBound) {
      x.toInt
    } else {
      overflowException(x, "int")
    }
  }

  override def toLong(x: Double): Long = {
    if (Math.floor(x) <= longUpperBound && Math.ceil(x) >= longLowerBound) {
      x.toLong
    } else {
      overflowException(x, "long")
    }
  }
}

object DecimalExactNumeric extends DecimalIsConflicted {
  override def toInt(x: Decimal): Int = x.roundToInt()

  override def toLong(x: Decimal): Long = x.roundToLong()
}
