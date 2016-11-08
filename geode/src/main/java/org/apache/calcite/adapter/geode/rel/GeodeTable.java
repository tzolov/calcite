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

import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table based on a Geode Region
 */
public class GeodeTable extends AbstractQueryableTable implements TranslatableTable {

  private GeodeSchema schema;
  private String regionName;
  private RelDataType relDataType;
  private ClientCache clientCache;

  public GeodeTable(GeodeSchema schema,
                    String regionName,
                    RelDataType relDataType,
                    ClientCache clientCache) {
    super(Object[].class);
    this.schema = schema;
    this.regionName = regionName;
    this.relDataType = relDataType;
    this.clientCache = clientCache;
  }

  public String toString() {
    return "GeodeTable {" + regionName + "}";
  }

  /**
   * Executes a OQL query on the underlying table. Called by the GeodeQueryable which in turn is called via the
   * generated code.
   *
   * @param clientCache Geode client cache
   * @param fields      List of fields to project
   * @param predicates  A list of predicates which should be used in the query
   * @return Enumerator of results
   */
  public Enumerable<Object> query(
      final ClientCache clientCache,
      List<Map.Entry<String, Class>> fields,
      final List<Map.Entry<String, String>> selectFields,
      List<String> predicates,
      List<String> order, String limit) {

    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();

    //final RelDataType rowType = protoRowType.apply(typeFactory); TODO ?
    final RelDataType rowType = this.getRowType(typeFactory);

    Function1<String, Void> addField = new Function1<String, Void>() {
      public Void apply(String fieldName) {
        RelDataTypeField field = rowType.getField(fieldName, true, false);
        SqlTypeName typeName = field.getType().getSqlTypeName();
        fieldInfo.add(fieldName, typeFactory.createSqlType(typeName)).nullable(true);
        return null;
      }
    };

    if (selectFields.isEmpty()) {
      for (Map.Entry<String, Class> field : fields) {
        addField.apply(field.getKey());
      }
    } else {
      for (Map.Entry<String, String> field : selectFields) {
        addField.apply(field.getKey());
      }
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    // Construct the list of fields to project
    final String selectString;
    if (selectFields.isEmpty()) {
      selectString = "*";
    } else {
      selectString = Util.toString(new Iterable<String>() {
        public Iterator<String> iterator() {
          final Iterator<Map.Entry<String, String>> selectIterator = selectFields.iterator();
          return new Iterator<String>() {
            @Override public boolean hasNext() {
              return selectIterator.hasNext();
            }

            @Override public String next() {
              Map.Entry<String, String> entry = selectIterator.next();
              return entry.getKey() + " AS " + entry.getValue();
            }

            @Override public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      }, "", ", ", "");
    }

    // Combine all predicates conjunctively
    String whereClause = "";
    if (!predicates.isEmpty()) {
      whereClause = " WHERE ";
      whereClause += Util.toString(predicates, "", " AND ", "");
    }

    // Build and issue the query and return an Enumerator over the results
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    queryBuilder.append(selectString);
    queryBuilder.append(" FROM /" + regionName);
    queryBuilder.append(whereClause);
    if (!order.isEmpty()) {
      queryBuilder.append(Util.toString(order, " ORDER BY ", ", ", ""));
    }
    if (limit != null) {
      queryBuilder.append(" LIMIT " + limit);
    }

    final String oqlQuery = queryBuilder.toString();

    System.out.println("OQL: " + oqlQuery);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        SelectResults results = null;
        QueryService queryService = clientCache.getQueryService();

        try {
          results = (SelectResults) queryService.newQuery(oqlQuery).execute();
        } catch (Exception e) {
          e.printStackTrace();
        }

        return new GeodeEnumerator(results, resultRowType);
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    return new GeodeQueryable<>(queryProvider, schema, this, tableName);
  }

  @Override public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {

    final RelOptCluster cluster = context.getCluster();
    return new GeodeTableScanRel(cluster, cluster.traitSetOf(GeodeRel.CONVENTION),
        relOptTable, this, null);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return relDataType;
  }

  /**
   * Implementation of {@link Queryable} based on
   * a {@link GeodeTable}.
   */
  public static class GeodeQueryable<T> extends AbstractTableQueryable<T> {

    public GeodeQueryable(QueryProvider queryProvider, SchemaPlus schema,
                          GeodeTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }
    //tzolov: this should never be called for queryable tables???
    public Enumerator<T> enumerator() {
      throw new UnsupportedOperationException("Enumberator on Queryable should never be called (tzolov)!");
    }

    private GeodeTable getTable() {
      return (GeodeTable) table;
    }

    private ClientCache getClientCache() {
      return schema.unwrap(GeodeSchema.class).clientCache;
    }

    /**
     * Called via code-generation.
     *
     * @see GeodeToEnumerableConverterRel#implement(EnumerableRelImplementor, Prefer)
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(
        List<Map.Entry<String, Class>> fields,
        List<Map.Entry<String, String>> selectFields,
        List<String> predicates,
        List<String> order,
        String limit) {
      return getTable().query(getClientCache(), fields, selectFields, predicates, order, limit);
    }
  }
}

// End GeodeTable.java
