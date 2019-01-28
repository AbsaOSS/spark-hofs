/*
 * Copyright 2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spark.hofs

import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}

import DataFrameExtensions._

class TransformFunctionSuite extends FunSuite with TestBase with Matchers {
  import spark.implicits._

  test("transform function with anonymous variables") {
    val df = Seq(Seq(Seq(1, 9), Seq(8, 9))).toDF("array")
    val result = df.applyFunction(transform('array, x => concat(array(lit(1)), transform(x, y => y + 1))))

    result shouldEqual Array(Array(1, 2, 10), Array(1, 9, 10))
  }

  test("transform function with named variables") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val function = transform('array, y => y + 1, "myvar")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual Array(2, 5, 6, 8)
    resultField shouldEqual "transform(array, lambdafunction((myvar + 1), myvar))"
  }

  test("transform function with anonymous variables and an index") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val result = df.applyFunction(transform('array, (x, i) => x + i))

    result shouldEqual Array(1, 5, 7, 10)
  }

  test("transform function with named variables and an index") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val function = transform('array, (x, i) => x + i, "myvar", "myidx")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual Array(1, 5, 7, 10)
    resultField shouldEqual "transform(array, lambdafunction((myvar + myidx), myvar, myidx))"
  }

}
