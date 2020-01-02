    Copyright 2019 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# spark-hofs
Apache Spark 2.4.0 introduced high-order functions as a part of SQL expressions. These new functions are accessible
only via textual representation of Spark SQL.

This library makes the high-order functions accessible also for Dataframe/Dataset Scala API to get type safety when
using the functions. 

## Usage

Reference the library

### Scala 2.11
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa.cobrix/spark-hofs_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa.cobrix/spark-hofs_2.11)

```
groupId: za.co.absa.cobrix
artifactId: spark-hofs_2.11
version: 0.4.0
```

### Scala 2.12
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa.cobrix/spark-hofs_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa.cobrix/spark-hofs_2.12)

```
groupId: za.co.absa.cobrix
artifactId: spark-hofs_2.12
version: 0.4.0
```

Please, use the table below to determine what version of spark-hofs to use for Spark compatibility.

| Scala version |  Spark version  |  spark-hofs version   |
|:-------------:|:---------------:|:---------------------:|
|     2.11      |     2.4.0       |         0.1.0         |
|     2.11      |     2.4.1       |         0.2.0         |
|     2.11      |     2.4.2       |         0.3.x         |
|  2.11, 2.12   |     2.4.3+      |         0.4.x+        |

Import Scala API of the high-order functions into your scope.
```scala
import za.co.absa.spark.hofs._
```

## Functions

### Transform
The **transform** function is an equivalent to the *map* function from functional programming. It takes a column of
arrays as the first argument and projects every element in each array with using a function passed as the second argument.
```scala
scala> df.withColumn("output", transform('input, x => x + 1)).show
+------------+------------+
|       input|      output|
+------------+------------+
|[1, 4, 5, 7]|[2, 5, 6, 8]|
+------------+------------+
```
If the logic of the projection function requires information about the element position of a given array,
the **transform** function can pass an index starting from 0 to the projection function as the second argument.
```scala
scala> df.withColumn("output", transform('input, (x, i) => x + i)).show
+------------+-------------+
|       input|       output|
+------------+-------------+
|[1, 4, 5, 7]|[1, 5, 7, 10]|
+------------+-------------+
```
By default, the lambda variable representing the element will be seen as `elm` and the lambda variable representing
the index as `idx` in Spark execution plans.
```scala
scala> df.withColumn("output", transform('input, (x, i) => x + i)).explain(true)
== Parsed Logical Plan ==
'Project [input#8, transform('input, lambdafunction(('elm + 'idx), 'elm, 'idx, false)) AS output#45]
+- Project [value#6 AS input#8]
   +- LocalRelation [value#6]

== Analyzed Logical Plan ==
input: array<int>, output: array<int>
Project [input#8, transform(input#8, lambdafunction((lambda elm#51 + lambda idx#52), lambda elm#51, lambda idx#52, false)) AS output#45]
+- Project [value#6 AS input#8]
   +- LocalRelation [value#6]

...
```
Names of the lambda variables can be changed by passing extra argument to the **transform** function.
```scala
scala> df.withColumn("output", transform('input, (x, i) => x + i, "myelm", "myidx")).explain(true)
== Parsed Logical Plan ==
'Project [input#8, transform('input, lambdafunction(('myelm + 'myidx), 'myelm, 'myidx, false)) AS output#53]
+- Project [value#6 AS input#8]
   +- LocalRelation [value#6]

== Analyzed Logical Plan ==
input: array<int>, output: array<int>
Project [input#8, transform(input#8, lambdafunction((lambda myelm#59 + lambda myidx#60), lambda myelm#59, lambda myidx#60, false)) AS output#53]
+- Project [value#6 AS input#8]
   +- LocalRelation [value#6]
   
...   
```

### Filter
The **filter** function takes a column of arrays as the first argument and eliminates all elements that do not satisfy 
the predicate that is passed as the second argument.
```scala
scala> df.withColumn("output", filter('input, x => x % 2 === 1)).show
+------------------+---------+
|             input|   output|
+------------------+---------+
|[1, 2, 4, 5, 7, 8]|[1, 5, 7]|
+------------------+---------+
```
The lambda variable within the predicate will be seen as `elm` in Spark execution plans. This name can be changed by
passing the third argument to the **filter** function. 

### Aggregate
The **aggregate** function is an equivalent of the *foldLeft* function from functional programming. The method takes
a column of arrays and a column of zero elements as first two arguments. The next argument is a binary function merging
a zero element and all elements from an input array into one element. The first argument of the merging function is
an accumulated value and the second one is an element of given iteration.
```scala
scala> df.withColumn("output", aggregate('input, 'zero, (acc, x)  => acc + x)).show
+------------------+----+------+
|             input|zero|output|
+------------------+----+------+
|[1, 2, 4, 5, 7, 8]| 100|   127|
+------------------+----+------+
```
If an user wants to transform the reduced value before returning the result, the user can pass a function performing
the transformation logic as the fourth argument.
```scala
scala> df.withColumn("output", aggregate('input, 'zero, (acc, x)  => acc + x, y => concat(y, y))).show
+------------------+----+------+
|             input|zero|output|
+------------------+----+------+
|[1, 2, 4, 5, 7, 8]| 100|127127|
+------------------+----+------+
```

The lambda variable representing the accumulator will be seen as `acc` and the lambda variable representing the element 
as `elm` in Spark execution plans. The names can be changed by passing extra arguments to the **aggregate** function.

### Zip With
The **zip_with** function takes two columns of arrays as the first two arguments and performs element-wise merge into
a single column of arrays. The third argument ia a function taking one element from each array at the same
position and specifying the merge logic. If one array is shorter, null elements are appended this array to be the same 
length as the longer array.
```scala
scala> df.withColumn("output", zip_with('input1, 'input2, (x, y) => x + y)).show
+---------------+-------------+---------------+
|         input1|       input2|         output|
+---------------+-------------+---------------+
|[1, 2, 4, 5, 7]|[2, 4, 8, 12]|[3, 6, 12, 17,]|
+---------------+-------------+---------------+
```
The lambda variables indicating input elements to the merging function will be seen as `left` and `right` in
Spark execution plans. The names can be changed by passing extra arguments to the **zip_with** function.