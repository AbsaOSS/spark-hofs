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
import org.apache.spark.sql.catalyst.expressions.Expression

/**
  * The object is a container of extension methods used by the wrapper of high-order functions.
  */
object Extensions {

  /**
    * The class represents an extension wrapper for an [[org.apache.spark.sql.catalyst.expressions.Expression expression]].
    * @param expression An expression to be extended with methods contained in this class.
    */
  implicit class ExpressionExtension(expression: Expression) {

    /**
      * The method converts wrapped expression to a column.
      * @return A column
      */
    def toCol: Column = new Column(expression)
  }
}
