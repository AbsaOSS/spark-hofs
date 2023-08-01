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
import DataFrameExtensions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FilterFunctionSuite extends AnyFunSuite with TestBase with Matchers {
  import spark.implicits._

  private val predicate = (x: Column) => x % 2 === 0
  private val df = Seq(Seq(1, 4, 5, 7, 8, 6, 3)).toDF("array")
  private val expected = Array(4, 8, 6)

  test("filter function with anonymous variables") {
    val result = df.applyFunction(filter('array, predicate))

    result shouldEqual expected
  }

  test("filter function with named variables") {
    val function = filter('array, predicate, "myelm")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual expected
    if (spark.version.startsWith("2") || spark.version.startsWith("3.1")) {
      resultField shouldEqual "filter(array, lambdafunction(((myelm % 2) = 0), myelm))"
    } else {
      resultField shouldEqual "filter(array, lambdafunction(((namedlambdavariable() % 2) = 0), namedlambdavariable()))"
    }
  }
}
