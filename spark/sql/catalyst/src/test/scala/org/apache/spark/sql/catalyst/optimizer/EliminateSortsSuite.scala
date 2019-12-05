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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CASE_SENSITIVE, ORDER_BY_ORDINAL}
import org.apache.spark.sql.types.IntegerType

class EliminateSortsSuite extends PlanTest {
  override val conf = new SQLConf().copy(CASE_SENSITIVE -> true, ORDER_BY_ORDINAL -> false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Default", FixedPoint(10),
        FoldablePropagation,
        LimitPushDown) ::
      Batch("Eliminate Sorts", Once,
        EliminateSorts) ::
      Batch("Collapse Project", Once,
        CollapseProject) :: Nil
  }

  object PushDownOptimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Limit PushDown", FixedPoint(10), LimitPushDown) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelationB = LocalRelation('d.int)

  test("Empty order by clause") {
    val x = testRelation

    val query = x.orderBy()
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = x.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("All the SortOrder are no-op") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), SortOrder(-1, Ascending))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x)

    comparePlans(optimized, correctAnswer)
  }

  test("Partial order-by clauses contain no-op SortOrder") {
    val x = testRelation

    val query = x.orderBy(SortOrder(3, Ascending), 'a.asc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(x.orderBy('a.asc))

    comparePlans(optimized, correctAnswer)
  }

  test("Remove no-op alias") {
    val x = testRelation

    val query = x.select('a.as('x), Year(CurrentDate()).as('y), 'b)
      .orderBy('x.asc, 'y.asc, 'b.desc)
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = analyzer.execute(
      x.select('a.as('x), Year(CurrentDate()).as('y), 'b).orderBy('x.asc, 'b.desc))

    comparePlans(optimized, correctAnswer)
  }

  test("remove redundant order by") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val unnecessaryReordered = orderedPlan.limit(2).select('a).orderBy('a.asc, 'b.desc_nullsFirst)
    val optimized = Optimize.execute(unnecessaryReordered.analyze)
    val correctAnswer = orderedPlan.limit(2).select('a).analyze
    comparePlans(Optimize.execute(optimized), correctAnswer)
  }

  test("do not remove sort if the order is different") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val reorderedDifferently = orderedPlan.limit(2).select('a).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(reorderedDifferently.analyze)
    val correctAnswer = reorderedDifferently.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("filters don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.where('a > Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.where('a > Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("limits don't affect order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.limit(Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.limit(Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("different sorts are not simplified if limit is in between") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('b.desc).limit(Literal(10))
      .orderBy('a.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = orderedPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("range is already sorted") {
    val inputPlan = Range(1L, 1000L, 1, 10)
    val orderedPlan = inputPlan.orderBy('id.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = inputPlan.analyze
    comparePlans(optimized, correctAnswer)

    val reversedPlan = inputPlan.orderBy('id.desc)
    val reversedOptimized = Optimize.execute(reversedPlan.analyze)
    val reversedCorrectAnswer = reversedPlan.analyze
    comparePlans(reversedOptimized, reversedCorrectAnswer)

    val negativeStepInputPlan = Range(10L, 1L, -1, 10)
    val negativeStepOrderedPlan = negativeStepInputPlan.orderBy('id.desc)
    val negativeStepOptimized = Optimize.execute(negativeStepOrderedPlan.analyze)
    val negativeStepCorrectAnswer = negativeStepInputPlan.analyze
    comparePlans(negativeStepOptimized, negativeStepCorrectAnswer)
  }

  test("sort should not be removed when there is a node which doesn't guarantee any order") {
    val orderedPlan = testRelation.select('a, 'b)
    val groupedAndResorted = orderedPlan.groupBy('a)(sum('a)).orderBy('a.asc)
    val optimized = Optimize.execute(groupedAndResorted.analyze)
    val correctAnswer = groupedAndResorted.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove two consecutive sorts") {
    val orderedTwice = testRelation.orderBy('a.asc).orderBy('b.desc)
    val optimized = Optimize.execute(orderedTwice.analyze)
    val correctAnswer = testRelation.orderBy('b.desc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove sorts separated by Filter/Project operators") {
    val orderedTwiceWithProject = testRelation.orderBy('a.asc).select('b).orderBy('b.desc)
    val optimizedWithProject = Optimize.execute(orderedTwiceWithProject.analyze)
    val correctAnswerWithProject = testRelation.select('b).orderBy('b.desc).analyze
    comparePlans(optimizedWithProject, correctAnswerWithProject)

    val orderedTwiceWithFilter =
      testRelation.orderBy('a.asc).where('b > Literal(0)).orderBy('b.desc)
    val optimizedWithFilter = Optimize.execute(orderedTwiceWithFilter.analyze)
    val correctAnswerWithFilter = testRelation.where('b > Literal(0)).orderBy('b.desc).analyze
    comparePlans(optimizedWithFilter, correctAnswerWithFilter)

    val orderedTwiceWithBoth =
      testRelation.orderBy('a.asc).select('b).where('b > Literal(0)).orderBy('b.desc)
    val optimizedWithBoth = Optimize.execute(orderedTwiceWithBoth.analyze)
    val correctAnswerWithBoth =
      testRelation.select('b).where('b > Literal(0)).orderBy('b.desc).analyze
    comparePlans(optimizedWithBoth, correctAnswerWithBoth)

    val orderedThrice = orderedTwiceWithBoth.select(('b + 1).as('c)).orderBy('c.asc)
    val optimizedThrice = Optimize.execute(orderedThrice.analyze)
    val correctAnswerThrice = testRelation.select('b).where('b > Literal(0))
      .select(('b + 1).as('c)).orderBy('c.asc).analyze
    comparePlans(optimizedThrice, correctAnswerThrice)
  }

  test("remove orderBy in groupBy clause with count aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = unnecessaryOrderByPlan.groupBy('a)(count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = projectPlan.groupBy('a)(count(1)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove orderBy in groupBy clause with sum aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = unnecessaryOrderByPlan.groupBy('a)(sum('a) + 10 as "sum")
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = projectPlan.groupBy('a)(sum('a) + 10 as "sum").analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy in groupBy clause with first aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(first('a))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy in groupBy clause with first and count aggs") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(first('a), count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy in groupBy clause with PythonUDF as aggs") {
    val pythonUdf = PythonUDF("pyUDF", null,
      IntegerType, Seq.empty, PythonEvalType.SQL_BATCHED_UDF, true)
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(pythonUdf)
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy in groupBy clause with ScalaUDF as aggs") {
    val scalaUdf = ScalaUDF((s: Int) => s, IntegerType, 'a :: Nil, true :: Nil)
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val groupByPlan = orderByPlan.groupBy('a)(scalaUdf)
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy with limit in groupBy clause") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc).limit(10)
    val groupByPlan = orderByPlan.groupBy('a)(count(1))
    val optimized = Optimize.execute(groupByPlan.analyze)
    val correctAnswer = groupByPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove orderBy in join clause") {
    val projectPlan = testRelation.select('a, 'b)
    val unnecessaryOrderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = unnecessaryOrderByPlan.join(projectPlanB).select('a, 'd)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = projectPlan.join(projectPlanB).select('a, 'd).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy with limit in join clause") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc).limit(10)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan.join(projectPlanB).select('a, 'd)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = joinPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("should not remove orderBy in left join clause if there is an outer limit") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan
      .join(projectPlanB, LeftOuter)
      .limit(10)
    val optimized = Optimize.execute(joinPlan.analyze)
    val correctAnswer = PushDownOptimizer.execute(joinPlan.analyze)
    comparePlans(optimized, correctAnswer)
  }

  test("remove orderBy in right join clause event if there is an outer limit") {
    val projectPlan = testRelation.select('a, 'b)
    val orderByPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val projectPlanB = testRelationB.select('d)
    val joinPlan = orderByPlan
      .join(projectPlanB, RightOuter)
      .limit(10)
    val optimized = Optimize.execute(joinPlan.analyze)
    val noOrderByPlan = projectPlan
      .join(projectPlanB, RightOuter)
      .limit(10)
    val correctAnswer = PushDownOptimizer.execute(noOrderByPlan.analyze)
    comparePlans(optimized, correctAnswer)
  }
}
