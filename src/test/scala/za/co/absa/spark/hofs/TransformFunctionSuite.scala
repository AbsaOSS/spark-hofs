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

import org.apache.spark.sql.functions._
import DataFrameExtensions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.hofs

class TransformFunctionSuite extends AnyFunSuite with TestBase with Matchers {
  import spark.implicits._

  test("transform function with anonymous variables") {
    val df = Seq(Seq(Seq(1, 9), Seq(8, 9))).toDF("array")
    val result = df.applyFunction(hofs.transform('array, x => concat(array(lit(1)), hofs.transform(x, y => y + 1))))

    result shouldEqual Array(Array(1, 2, 10), Array(1, 9, 10))
  }

  test("transform function with named variables") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val function = hofs.transform('array, y => y + 1, "myelm")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual Array(2, 5, 6, 8)
    if (spark.version.startsWith("2") || spark.version.startsWith("3.1")) {
      resultField shouldEqual "transform(array, lambdafunction((myelm + 1), myelm))"
    } else {
      resultField shouldEqual "transform(array, lambdafunction((namedlambdavariable() + 1), namedlambdavariable()))"
    }
  }

  test("transform function with anonymous variables and an index") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val result = df.applyFunction(hofs.transform('array, (x, i) => x + i))

    result shouldEqual Array(1, 5, 7, 10)
  }

  test("transform function with named variables and an index") {
    val df = Seq(Seq(1, 4, 5, 7)).toDF("array")
    val function = hofs.transform('array, (x, i) => x + i, "myelm", "myidx")
    val result = df.applyFunction(function)
    val resultField = df.select(function).schema.fields.head.name

    result shouldEqual Array(1, 5, 7, 10)
    if (spark.version.startsWith("2") || spark.version.startsWith("3.1")) {
      resultField shouldEqual "transform(array, lambdafunction((myelm + myidx), myelm, myidx))"
    } else {
      resultField shouldEqual "transform(array, lambdafunction((namedlambdavariable() + namedlambdavariable()), namedlambdavariable(), namedlambdavariable()))"
    }
  }

}
