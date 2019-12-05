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

package org.apache.spark.sql.dynamicpruning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, BindReferences, DynamicPruningExpression, DynamicPruningSubquery, Expression, ListQuery, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{InSubqueryExec, QueryExecution, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.internal.SQLConf

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
case class PlanDynamicPruningFilters(sparkSession: SparkSession)
    extends Rule[SparkPlan] with PredicateHelper {

  private def reuseBroadcast: Boolean =
    SQLConf.get.dynamicPartitionPruningReuseBroadcast && SQLConf.get.exchangeReuseEnabled

  /**
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], plan: LogicalPlan): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), plan.output)
    HashedRelationBroadcastMode(packedKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan transformAllExpressions {
      case DynamicPruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId) =>
        val qe = new QueryExecution(sparkSession, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        val canReuseExchange = reuseBroadcast && buildKeys.nonEmpty &&
          plan.find {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _) =>
              left.sameResult(qe.sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right) =>
              right.sameResult(qe.sparkPlan)
            case _ => false
          }.isDefined

        if (canReuseExchange) {
          val mode = broadcastMode(buildKeys, buildPlan)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, qe.executedPlan)
          val name = s"dynamicpruning#${exprId.id}"
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues =
            SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) {
          // it is not worthwhile to execute the query, so we fall-back to a true literal
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val alias = Alias(buildKeys(broadcastKeyIndex), buildKeys(broadcastKeyIndex).toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
          DynamicPruningExpression(expressions.InSubquery(
            Seq(value), ListQuery(aggregate, childOutputs = aggregate.output)))
        }
    }
  }
}
