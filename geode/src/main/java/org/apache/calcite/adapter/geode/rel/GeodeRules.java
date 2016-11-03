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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.AbstractList;
import java.util.List;

/**
 * Rules and relational operators for {@link GeodeRel#CONVENTION} calling convention.
 */
public class GeodeRules {

  public static final RelOptRule[] RULES = {
      GeodeFilterRule.INSTANCE,
      GeodeProjectRule.INSTANCE,
      GeodeLimitRule.INSTANCE,
      GeodeSpecialSortRule.INSTANCE
  };

  private GeodeRules() {
  }

  static List<String> geodeFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override public String get(int index) {
            return rowType.getFieldList().get(index).getName();
          }

          @Override public int size() {
            return rowType.getFieldCount();
          }
        });
  }

  /**
   * Translator from {@link RexNode} to strings in Geode's expression
   * language.
   */
  static class RexToGeodeTranslator extends RexVisitorImpl<String> {
  //  private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToGeodeTranslator(
        //JavaTypeFactory typeFactory,
                                   List<String> inFields) {
      super(true);
//      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }


  /**
   * Geode supports ORDER BY only in combination with SELECT DISTINCT statement. The relational
   * algebra representation of the the SELECT DISTINCT (field list) statement
   * is GROUP BY (field list).
   */
  private static class GeodeSpecialSortRule extends RelOptRule {

    private static final GeodeSpecialSortRule INSTANCE = new GeodeSpecialSortRule();

    public GeodeSpecialSortRule() {
      super(operand(Sort.class, operand(Aggregate.class, any())), "GeodeSpecialSortRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final Aggregate aggregate = call.rel(1);
      boolean same = sort.getRowType().equals(aggregate.getRowType());
      System.out.println("Same:" + same);
      ImmutableBitSet groupSet = aggregate.getGroupSet();

      int groupSetCardinality = groupSet.cardinality();

      List<String> aggregateFields = GeodeRules.geodeFieldNames(aggregate.getRowType());

      //aggregate.getRowType().getFieldList().get(aggregate.)
      List<RelFieldCollation> fieldCollations = sort.getCollation().getFieldCollations();
//      System.out.println(fieldCollations.size() <= groupSetCardinality);
//
//      System.out.println(sort.getRowType().getFieldList().get(fieldCollations.get(1).getFieldIndex()));
//      System.out.println(sort.getCollation().getFieldCollations());
      return fieldCollations.size() <= groupSetCardinality;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      //Do nothing

    }
  }


  /**
   * Rule to convert the Limit in {@link org.apache.calcite.rel.core.Sort} to a
   * {@link GeodeSort}.
   */
  private static class GeodeLimitRule extends RelOptRule {

    private static final GeodeLimitRule INSTANCE = new GeodeLimitRule(new Predicate<Sort>() {
      public boolean apply(Sort input) {
        // OQL has no support for offsets (e.g. LIMIT 10 OFFSET 500)
        return input.offset == null;
      }
    });

    public GeodeLimitRule(Predicate<Sort> predicate) {
      super(operand(Sort.class, null, predicate, any()), "GeodeLimitRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      return sort.fetch != null;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);

      final RelTraitSet traitSet =
          sort.getTraitSet().replace(GeodeRel.CONVENTION)
              .replace(sort.getCollation());

      GeodeSort geodeSort = new GeodeSort(sort.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
          sort.getCollation(), sort.fetch);

      call.transformTo(geodeSort);
    }
  }

  /**
   * Rule to convert a {@link LogicalFilter} to a
   * {@link GeodeFilter}.
   */
  private static class GeodeFilterRule extends RelOptRule {

    private static final GeodeFilterRule INSTANCE = new GeodeFilterRule();

    private GeodeFilterRule() {
      super(operand(LogicalFilter.class, operand(GeodeTableScan.class, none())),
          "GeodeFilterRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      // Get the condition from the filter operation
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      // Get field names from the scan operation
      //GeodeTableScan scan = call.rel(1);

      List<String> fieldNames = GeodeRules.geodeFieldNames(filter.getInput().getRowType());

      List<RexNode> disjunctions = RelOptUtil.disjunctions(condition);
      if (disjunctions.size() != 1) {
        //return false;
        return true;
      } else {
        //Check that all conjunctions are primary field conditions.
        condition = disjunctions.get(0);
        for (RexNode predicate : RelOptUtil.conjunctions(condition)) {
          if (!isEqualityOnKey(predicate, fieldNames)) {
            return false;
          }
        }
      }

      return true;
    }

    /**
     * Check if the node is a supported predicate (primary field condition).
     *
     * @param node       Condition node to check
     * @param fieldNames Names of all columns in the table
     * @return True if the node represents an equality predicate on a primary key
     */
    private boolean isEqualityOnKey(RexNode node, List<String> fieldNames) {

      RexCall call = (RexCall) node;
      final RexNode left = call.operands.get(0);
      final RexNode right = call.operands.get(1);

      if (checkConditionContasInputRefOrLiterals(left, right, fieldNames)) {
        return true;
      }
      return checkConditionContasInputRefOrLiterals(right, left, fieldNames);

    }

    /**
     * @param left       Left operand of the equality
     * @param right      Right operand of the equality
     * @param fieldNames Names of all columns in the table
     * @return The {true} if condition is supported
     */
    private boolean checkConditionContasInputRefOrLiterals(RexNode left, RexNode right, List<String> fieldNames) {
      // FIXME Ignore casts for rel and assume they aren't really necessary
      if (left.isA(SqlKind.CAST)) {
        left = ((RexCall) left).getOperands().get(0);
      }

      if (right.isA(SqlKind.CAST)) {
        right = ((RexCall) right).getOperands().get(0);
      }

      if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL)) {
        final RexInputRef left1 = (RexInputRef) left;
        String name = fieldNames.get(left1.getIndex());
        return name != null;
      } else if (left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.INPUT_REF)) {

        final RexInputRef left1 = (RexInputRef) left;
        String leftName = fieldNames.get(left1.getIndex());

        final RexInputRef right1 = (RexInputRef) right;
        String rightName = fieldNames.get(right1.getIndex());

        return (leftName != null) && (rightName != null);
      }

      return false;
    }

    /**
     * @see ConverterRule
     */
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      GeodeTableScan scan = call.rel(1);
      if (filter.getTraitSet().contains(Convention.NONE)) {
        final RelNode converted = convert(filter, scan);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }

    public RelNode convert(LogicalFilter filter, GeodeTableScan scan) {
      final RelTraitSet traitSet = filter.getTraitSet().replace(GeodeRel.CONVENTION);
      return new GeodeFilter(
          filter.getCluster(),
          traitSet,
          convert(filter.getInput(), GeodeRel.CONVENTION),
          filter.getCondition());
    }
  }

  /**
   * Base class for planner rules that convert a relational expression to
   * Geode calling convention.
   */
  abstract static class GeodeConverterRule extends ConverterRule {
    protected final Convention out;

    public GeodeConverterRule(
        Class<? extends RelNode> clazz,
        String description) {
      this(clazz, Predicates.<RelNode>alwaysTrue(), description);
    }

    public <R extends RelNode> GeodeConverterRule(
        Class<R> clazz,
        Predicate<? super R> predicate,
        String description) {
      super(clazz, predicate, Convention.NONE, GeodeRel.CONVENTION, description);
      this.out = GeodeRel.CONVENTION;
    }
  }

  /**
   * Rule to convert a {@link LogicalProject}
   * to a {@link GeodeProject}.
   */
  private static class GeodeProjectRule extends GeodeConverterRule {

    private static final GeodeProjectRule INSTANCE = new GeodeProjectRule();

    private GeodeProjectRule() {
      super(LogicalProject.class, "GeodeProjectRule");
    }

    @Override public boolean matches(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      for (RexNode e : project.getProjects()) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }

      return true;
    }

    @Override public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new GeodeProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(),
          project.getRowType());
    }
  }
}

// End GeodeRules.java
