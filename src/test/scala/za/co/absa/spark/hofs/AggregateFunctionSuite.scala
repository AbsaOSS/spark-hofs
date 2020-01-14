/*
 * Copyright 2018 ABSA Group Limited
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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.scalatest.{FunSuite, Matchers}

import DataFrameExtensions._

class AggregateFunctionSuite extends FunSuite with TestBase with Matchers {
  import spark.implicits._

  private val df = Seq(Seq(2, 4, 5, 7)).toDF("array")
  private val merge = (x: Column, y: Column) => x + y
  private val finish = (x: Column) => x * x
  private val zeroElement = lit(1)

  test("aggregate function with anonymous variables") {
    val result = df.applyFunction(aggregate('array, zeroElement, merge))

    result shouldEqual 19
  }

  test("aggregate function with named variables") {
    val function = aggregate('array, zeroElement, merge, "myacc", "myelm")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual 19
    resultField shouldEqual "aggregate(array, 1, lambdafunction((myacc + myelm), myacc, myelm), lambdafunction(myacc, myacc))"
  }

  test("aggregate function with anonymous variables and finish function") {
    val result = df.applyFunction(aggregate('array, zeroElement, merge, finish))

    result shouldEqual 361
  }

  test("aggregate function with named variables and finish function") {
    val function = aggregate('array, zeroElement, merge, finish, "myacc", "myelm")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual 361
    resultField shouldEqual "aggregate(array, 1, lambdafunction((myacc + myelm), myacc, myelm), lambdafunction((myacc * myacc), myacc))"
  }
}
