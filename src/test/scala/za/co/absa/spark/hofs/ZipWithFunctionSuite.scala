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

import org.apache.spark.sql.Column
import org.scalatest.{FunSuite, Matchers}

import DataFrameExtensions._

class ZipWithFunctionSuite extends FunSuite with TestBase with Matchers {
  import spark.implicits._

  private val df = Seq((Seq(1, 4, 5, 7), Seq(2, 6, 5, 8))).toDF("array1", "array2")
  private val expected = Array(3, 10, 10, 15)
  private val merge = (x: Column, y: Column) => x + y

  test("zip_with function with anonymous variables") {
    val result = df.applyFunction(zip_with('array1, 'array2, merge))

    result shouldEqual expected
  }

  test("zip_with function with named variables") {
    val function = zip_with('array1, 'array2, merge, "myelm1", "myelm2")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual expected
    resultField shouldEqual "zip_with(array1, array2, lambdafunction((myelm1 + myelm2), myelm1, myelm2))"
  }
}
