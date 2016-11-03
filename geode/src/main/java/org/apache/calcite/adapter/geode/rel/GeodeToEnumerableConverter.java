/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Relational expression representing a scan of a table in a Geode data source.
 */
public class GeodeToEnumerableConverter extends ConverterImpl implements EnumerableRel {

  protected GeodeToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new GeodeToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }


  /**
   * From {@link EnumerableRel}
   * Creates a plan for this expression according to a calling convention.
   *
   * @param implementor Implementor
   * @param pref Preferred representation for rows in result expression
   * @return Plan for this expression according to a calling convention
   */
  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Generates a call to "query" with the appropriate fields and predicates
    final BlockBuilder list = new BlockBuilder();
    final GeodeRel.Implementor geodeImplementor = new GeodeRel.Implementor();
    geodeImplementor.visitChild(0, getInput());
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(
                implementor.getTypeFactory(), rowType,
                pref.prefer(JavaRowFormat.ARRAY));
    final Expression fields =
        list.append("fields",
            constantArrayList(
                Pair.zip(GeodeRules.geodeFieldNames(rowType),
                    new AbstractList<Class>() {
                      @Override public Class get(int index) {
                        return physType.fieldClass(index);
                      }

                      @Override public int size() {
                        return rowType.getFieldCount();
                      }
                    }),
                Pair.class));
    List<Map.Entry<String, String>> selectList = new ArrayList<Map.Entry<String, String>>();
    for (Map.Entry<String, String> entry
            : Pair.zip(geodeImplementor.selectFields.keySet(),
                geodeImplementor.selectFields.values())) {
      selectList.add(entry);
    }
    final Expression selectFields =
        list.append("selectFields", constantArrayList(selectList, Pair.class));
    final Expression table =
        list.append("table",
            geodeImplementor.table.getExpression(
                GeodeTable.GeodeQueryable.class));
    final Expression predicates =
        list.append("predicates",
            constantArrayList(geodeImplementor.whereClause, String.class));
    final Expression order =
        list.append("order",
            constantArrayList(geodeImplementor.order, String.class));
    final Expression limit =
        list.append("limit",
            Expressions.constant(geodeImplementor.limitValue));
    Expression enumerable =
        list.append("enumerable",
            Expressions.call(table,
                GeodeMethod.GEODE_QUERYABLE_QUERY.method, fields,
                selectFields, predicates, order, limit));
    if (CalcitePrepareImpl.DEBUG) {
      System.out.println("Geode: " + predicates);
    }
    Hook.QUERY_PLAN.run(predicates);
    list.add(
        Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  /** E.g. {@code constantArrayList("x", "y")} returns
   * "Arrays.asList('x', 'y')". */
  private static <T> MethodCallExpression constantArrayList(List<T> values,
      Class clazz) {
    return Expressions.call(
        BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(clazz, constantList(values)));
  }

  /** E.g. {@code constantList("x", "y")} returns
   * {@code {ConstantExpression("x"), ConstantExpression("y")}}. */
  private static <T> List<Expression> constantList(List<T> values) {
    return Lists.transform(values,
        new Function<T, Expression>() {
          public Expression apply(T a0) {
            return Expressions.constant(a0);
          }
        });
  }
}

// End GeodeToEnumerableConverter.java
