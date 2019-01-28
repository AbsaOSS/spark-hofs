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

package za.co.absa.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._

import hofs.Extensions._

package object hofs {

  private def createLambda(
      lv: NamedExpression,
      f: Column => Column): Expression = {
    val function = f(lv.toCol).expr
    LambdaFunction(function, Seq(lv))
  }

  private def createLambda(
      lv1: NamedExpression,
      lv2: NamedExpression,
      f: (Column, Column) => Column): Expression = {
    val function = f(lv1.toCol, lv2.toCol).expr
    LambdaFunction(function, Seq(lv1, lv2))
  }

  private def createLambda(
      lv1: NamedExpression,
      lv2: NamedExpression,
      lv3: NamedExpression,
      f: (Column, Column, Column) => Column): Expression = {
    val function = f(lv1.toCol, lv2.toCol, lv3.toCol).expr
    LambdaFunction(function, Seq(lv1, lv2, lv3))
  }

  def transform(array: Column, f: Column => Column): Column = transform(array, f, "arg")

  def transform(
      array: Column,
      f: Column => Column,
      variableName: String): Column = {
    val variable = UnresolvedAttribute(variableName)
    val lambda = createLambda(variable, f)
    ArrayTransform(array.expr, lambda).toCol
  }

  def transform(array: Column, f: (Column, Column) => Column): Column = transform(array, f, "arg", "idx")

  def transform(
      array: Column,
      f: (Column, Column) => Column,
      variableName: String,
      indexName: String): Column = {
    val variable = UnresolvedAttribute(variableName)
    val index = UnresolvedAttribute(indexName)
    val lambda = createLambda(variable, index, f)
    ArrayTransform(array.expr, lambda).toCol
  }

  def filter(array: Column, f: Column => Column): Column = filter(array, f, "arg")

  def filter(
      array: Column,
      f: Column => Column,
      variableName: String): Column = {
    val variable = UnresolvedAttribute(variableName)
    val lambda = createLambda(variable, f)
    ArrayFilter(array.expr, lambda).toCol
  }

  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column): Column = {
    aggregate(array, zero, merge, "zeroElm", "arg")
  }

  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      zeroElementName: String,
      variableName: String): Column = {
    aggregate(array, zero, merge, identity, zeroElementName, variableName)
  }

  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column): Column = aggregate(array, zero, merge, finish, "zeroElm", "arg")

  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column,
      zeroElementName: String,
      variableName: String): Column = {
    val zeroElement = UnresolvedAttribute(zeroElementName)
    val variable = UnresolvedAttribute(variableName)
    ArrayAggregate(
      array.expr,
      zero.expr,
      createLambda(zeroElement, variable, merge),
      createLambda(zeroElement, finish)
    ).toCol
  }

  def zip_with(
      left: Column,
      right: Column,
      f: (Column, Column) => Column): Column = {
    zip_with(left, right, f, "left", "right")
  }

  def zip_with(
      left: Column,
      right: Column,
      f: (Column, Column) => Column,
      leftVariableName: String,
      rightVariableName: String): Column = {
    val leftVariable = UnresolvedAttribute(leftVariableName)
    val rightVariable = UnresolvedAttribute(rightVariableName)
    val lambda = createLambda(leftVariable, rightVariable, f)
    ZipWith(left.expr, right.expr, lambda).toCol
  }

}
