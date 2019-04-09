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

/**
  * The package object represents A Scala wrapper for all high-order functions.
  */
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

  /**
    * Applies the function `f` to every element in the `array`. The method is an equivalent to the `map` function
    * from functional programming. The lambda variable will be seen as `elm` in Spark execution plans.
    * @param array A column of arrays
    * @param f A function transforming individual elements of the array
    * @return A column of arrays with transformed elements
    */
  def transform(array: Column, f: Column => Column): Column = transform(array, f, "elm")

  /**
    * Applies the function `f` to every element in the `array`. The method is an equivalent to the `map` function
    * from functional programming.
    * @param array A column of arrays
    * @param f A function transforming individual elements of the array
    * @param elementName The name of the lambda variable. The value is used in Spark execution plans.
    * @return A column of arrays with transformed elements
    */
  def transform(
      array: Column,
      f: Column => Column,
      elementName: String): Column = {
    val variable = UnresolvedNamedLambdaVariable(Seq(elementName))
    val lambda = createLambda(variable, f)
    ArrayTransform(array.expr, lambda).toCol
  }

  /**
    * Applies the function `f` to every element in the `array`. The function 'f also obtains an index of a given element
    * when iterating over the array. The index starts from 0. The lambda variable will be seen as `elm` and the index as
    * `idx` in Spark execution plans.
    * @param array A column of arrays
    * @param f A function transforming individual elements of the array
    * @return A column of arrays with transformed elements
    */
  def transform(array: Column, f: (Column, Column) => Column): Column = transform(array, f, "elm", "idx")

  /**
    * Applies the function `f` to every element in the `array`. The function 'f also obtains an index of a given element
    * when iterating over the array. The index starts from 0.
    * @param array A column of arrays
    * @param f A function transforming individual elements of the array
    * @param elementName The name of the lambda variable representing an array element
    * @param indexName The name of the lambda variable representing the index that changes with each iteration
    * @return A column of arrays with transformed elements
    */
  def transform(
      array: Column,
      f: (Column, Column) => Column,
      elementName: String,
      indexName: String): Column = {
    val variable = UnresolvedNamedLambdaVariable(Seq(elementName))
    val index = UnresolvedNamedLambdaVariable(Seq(indexName))
    val lambda = createLambda(variable, index, f)
    ArrayTransform(array.expr, lambda).toCol
  }

  /**
    * Filters out elements that do not satisfy the predicate `f` from the input `array`. The lambda variable
    * within the predicate will be seen as `elm` in Spark execution plans.
    * @param array An input column of arrays
    * @param f A function representing the predicate
    * @return A column of filtered arrays (All elements within the arrays satisfy the predicate).
    */
  def filter(array: Column, f: Column => Column): Column = filter(array, f, "elm")

  /**
    * Filters out elements that do not satisfy the predicate `f` from the input `array`.
    * @param array An input column of arrays
    * @param f A function representing the predicate
    * @param elementName The name of the lambda variable representing an array element within the predicate logic
    * @return A column of filtered arrays (All elements within the arrays satisfy the predicate).
    */
  def filter(
      array: Column,
      f: Column => Column,
      elementName: String): Column = {
    val variable = UnresolvedNamedLambdaVariable(Seq(elementName))
    val lambda = createLambda(variable, f)
    ArrayFilter(array.expr, lambda).toCol
  }

  /**
    * Applies the binary `merge` function to gradually reduce the `zero` element and all elements from the input
    * `array` into one element. The function is an equivalent of the foldLeft function from functional programming.
    * The lambda variable for the accumulator will be represented as `acc` and the lambda variable for the element
    * as `elm` in Spark execution plans.
    * @param array A column of input arrays
    * @param zero A column of zero elements
    * @param merge A function that takes and and accumulator and a given element and returns another accumulator
    *              for the next iteration
    * @return A column with single elements
    */
  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column): Column = {
    aggregate(array, zero, merge, "acc", "elm")
  }

  /**
    * Applies the binary `merge` function to gradually reduce the `zero` element and all elements from the input
    * `array` into one element. The function is an equivalent of the foldLeft function from functional programming.
    * @param array A column of input arrays
    * @param zero A column of zero elements
    * @param merge A function that takes and and accumulator and a given element and returns another accumulator
    *              for the next iteration
    * @param accumulatorName A name of the lambda variable representing an accumulator
    * @param elementName A name of the lambda variable representing an array element
    * @return A column with single elements
    */
  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      accumulatorName: String,
      elementName: String): Column = {
    aggregate(array, zero, merge, identity, accumulatorName, elementName)
  }

  /**
    * Applies the binary `merge` function to gradually reduce the `zero` element and all elements from the input
    * `array` into one element. The function is an equivalent of the foldLeft function from functional programming.
    * After obtaining a single element the function converts the element into a suitable form by passing the element
    * into the `finish` function. The lambda variable for the accumulator will be represented as `acc` and the lambda
    * variable for the element as `elm` in Spark execution plans.
    * @param array A column of input arrays
    * @param zero A column of zero elements
    * @param merge A function that takes and and accumulator and a given element and returns another accumulator
    *              for the next iteration
    * @param finish A function converting the reduced element into a suitable form.
    * @return A column with single elements
    */
  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column): Column = aggregate(array, zero, merge, finish, "acc", "elm")

  /**
    * Applies the binary `merge` function to gradually reduce the `zero` element and all elements from the input
    * `array` into one element. The function is an equivalent of the foldLeft function from functional programming.
    * After obtaining a single element the function converts the element into a suitable form by passing the element
    * into the `finish` function.
    *
    * @param array  A column of input arrays
    * @param zero   A column of zero elements
    * @param merge  A function that takes and and accumulator and a given element and returns another accumulator
    *               for the next iteration
    * @param finish A function converting the reduced element into a suitable form
    * @param accumulatorName A name of the lambda variable representing an accumulator
    * @param elementName A name of the lambda variable representing an array element
    * @return A column with single elements
    */
  def aggregate(
      array: Column,
      zero: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column,
      accumulatorName: String,
      elementName: String): Column = {
    val zeroElement = UnresolvedNamedLambdaVariable(Seq(accumulatorName))
    val variable = UnresolvedNamedLambdaVariable(Seq(elementName))
    ArrayAggregate(
      array.expr,
      zero.expr,
      createLambda(zeroElement, variable, merge),
      createLambda(zeroElement, finish)
    ).toCol
  }

  /**
    * Merges two arrays into one by iterating over the both arrays at the same time and calling the function `f`.
    * If one array is shorter, null elements are appended this array to be the same length as the longer array.
    * Values returned from the functions `f` will become elements of the output array. The lambda variables indicating
    * input elements to the function `f` will be seen as `left` and `right` in Spark execution plans.
    * @param left An input column of arrays
    * @param right An input column of arrays
    * @param f A function producing result elements based on two elements from the input arrays
    * @return A column of merged arrays
    */
  def zip_with(
      left: Column,
      right: Column,
      f: (Column, Column) => Column): Column = {
    zip_with(left, right, f, "left", "right")
  }

  /**
    * Merges two arrays into one by iterating over the both arrays at the same time and calling the function `f`.
    * If one array is shorter, null elements are appended this array to be the same length as the longer array.
    * Values returned from the functions `f` will become elements of the output array.
    * @param left An input column of arrays
    * @param right An input column of arrays
    * @param f A function producing result elements based on two elements from the input arrays
    * @param leftElementName The name of the lambda variable representing an element of the first array
    * @param rightElementName The name of the lambda variable representing an element of the second array
    * @return A column of merged arrays
    */
  def zip_with(
      left: Column,
      right: Column,
      f: (Column, Column) => Column,
      leftElementName: String,
      rightElementName: String): Column = {
    val leftVariable = UnresolvedNamedLambdaVariable(Seq(leftElementName))
    val rightVariable = UnresolvedNamedLambdaVariable(Seq(rightElementName))
    val lambda = createLambda(leftVariable, rightVariable, f)
    ZipWith(left.expr, right.expr, lambda).toCol
  }

}
