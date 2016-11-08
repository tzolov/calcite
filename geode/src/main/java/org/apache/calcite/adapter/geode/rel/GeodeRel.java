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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational expression that uses Geode calling convention.
 */
public interface GeodeRel extends RelNode {

  /**
   * Calling convention for relational operations that occur in Geode.
   */
  Convention CONVENTION = new Convention.Impl("GEODE", GeodeRel.class);

  /**
   * Callback for the implementation process that collects the context from the
   * @link GeodeRel} required to converts the relational tree into physical such.
   *
   * @param geodeImplementContext - Context class that collects the feedback from the
   *                    call back method calls
   */
  void implement(GeodeImplementContext geodeImplementContext);

  /**
   * Shared context used by the GoedeRel relations.
   *
   * Callback context class for the implementation process that converts a tree of
   * {@link GeodeRel} nodes into a OQL query.
   */
  class GeodeImplementContext {
    final Map<String, String> selectFields = new LinkedHashMap<String, String>();
    final List<String> whereClause = new ArrayList<String>();
    final List<String> order = new ArrayList<String>();
    String limitValue = null;
    final List<String> groupByFields = new ArrayList<String>();
    RelOptTable table;
    GeodeTable geodeTable;

    /**
     * Adds newly projected fields and restricted predicates.
     *
     * @param fields     New fields to be projected from a query
     * @param predicates New predicates to be applied to the query
     */
    public void add(Map<String, String> fields, List<String> predicates) {
      if (fields != null) {
        selectFields.putAll(fields);
      }
      if (predicates != null) {
        whereClause.addAll(predicates);
      }
    }

    public void addGroupByFields(List<String> groupFields) {
      groupFields.addAll(groupFields);
    }

    public void addOrder(List<String> newOrder) {
      order.addAll(newOrder);
    }

    public void setLimit(String limit) {
      limitValue = limit;
    }

    @Override
    public String toString() {
      return "GeodeImplementContext{" +
              "selectFields=" + selectFields +
              ", whereClause=" + whereClause +
              ", order=" + order +
              ", limitValue='" + limitValue + '\'' +
              ", groupByFields=" + groupByFields +
              ", table=" + table +
              ", geodeTable=" + geodeTable +
              '}';
    }
  }
}

// End GeodeRel.java
