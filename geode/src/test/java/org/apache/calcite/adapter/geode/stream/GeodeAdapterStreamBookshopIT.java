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
package org.apache.calcite.adapter.geode.stream;

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@code org.apache.calcite.adapter.geode} package.
 *
 * <p>Before calling this rel, you need to populate Geode, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-rel-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Geode and the "bookshop"
 * and "zips" rel dataset.
 */
public class GeodeAdapterStreamBookshopIT {
  /**
   * Connection factory based on the "geode relational " model.
   */
  public static final ImmutableMap<String, String> GEODE_STREAM =
      ImmutableMap.of("model",
          GeodeAdapterStreamBookshopIT.class.getResource("/model-bookshop-stream.json")
              .getPath());

  /**
   * Whether to run Geode tests. Enabled by default, however rel is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.rel.geode=false} on the Java command line.
   */
  public static final boolean ENABLED = Util.getBooleanProperty("calcite.rel.geode", true);

  /**
   * Whether to run this rel.
   */
  protected boolean enabled() {
    return ENABLED;
  }

  @Test
  public void testStreamSelect() {

    String sqlAll = "SELECT STREAM  * FROM \"BookMaster\"\n";

    String sqlOrder = "SELECT STREAM\n"
        + "  FLOOR(\"rowtime\" TO hour) AS \"rowtime2\", \n"
        + "  \"retailCost\"\n"
        + "FROM \"BookMaster\"\n"
        + "ORDER BY FLOOR(\"rowtime\" TO hour) \n";

    String sqlGroupBy = "SELECT STREAM\n"
        + "  FLOOR(\"rowtime\" TO hour) AS \"rowtime\", \n"
        + "  SUM(\"retailCost\") \n"
        + "FROM \"BookMasterStream\" \n"
        + "GROUP BY FLOOR(\"rowtime\" TO hour) \n"
        + "HAVING count(*) > 1 \n";

    String sql2 = "SELECT STREAM\n"
        + "  FLOOR(\"rowtime\" TO SECOND) AS \"rowtime2\",\n"
        + "  SUM(\"retailCost\")\n"
        + "FROM \"BookMaster\"\n"
        + "GROUP BY FLOOR(\"rowtime\" TO SECOND)\n";

    CalciteAssert.that()
        .enable(enabled())
        .withDefaultSchema("bookshopstream")
        .with(GEODE_STREAM)
        .query(sqlGroupBy)
//        .limit(2)
//        .returnsCount(1)
        .returns(startsWith("boza"))

        .explainContains("PLAN=EnumerableAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
            + "  EnumerableCalc(expr#0..6=[{inputs}], expr#7=[FLAG(HOUR)], expr#8=[FLOOR($t0, "
            + "$t7)], rowtime2=[$t8], retailCost=[$t3])\n"
            + "    EnumerableInterpreter\n"
            + "      BindableTableScan(table=[[bookshopstream, BookMaster, (STREAM)]])\n");
  }

  private Function<ResultSet, Void> startsWith(String... rows) {
    final ImmutableList<String> rowList = ImmutableList.copyOf(rows);
    return new Function<ResultSet, Void>() {
      public Void apply(ResultSet input) {
        try {
          final ResultSetFormatter formatter =
              new ResultSetFormatter();
          final ResultSetMetaData metaData = input.getMetaData();
          for (String expectedRow : rowList) {
            if (!input.next()) {
              throw new AssertionError("input ended too soon");
            }
            formatter.rowToString(input, metaData);
            String actualRow = formatter.string();
            assertThat(actualRow, equalTo(expectedRow));
          }
          return null;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /** Converts a {@link ResultSet} to string. */
  static class ResultSetFormatter {
    final StringBuilder buf = new StringBuilder();

    public ResultSetFormatter resultSet(ResultSet resultSet)
        throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        buf.append("\n");
      }
      return this;
    }

    /** Converts one row to a string. */
    ResultSetFormatter rowToString(ResultSet resultSet,
        ResultSetMetaData metaData) throws SQLException {
      int n = metaData.getColumnCount();
      if (n > 0) {
        for (int i = 1;; i++) {
          buf.append(metaData.getColumnLabel(i))
              .append("=")
              .append(adjustValue(resultSet.getString(i)));
          if (i == n) {
            break;
          }
          buf.append("; ");
        }
      }
      return this;
    }

    protected String adjustValue(String string) {
      return string;
    }

    public Collection<String> toStringList(ResultSet resultSet,
        Collection<String> list) throws SQLException {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        rowToString(resultSet, metaData);
        list.add(buf.toString());
        buf.setLength(0);
      }
      return list;
    }

    /** Flushes the buffer and returns its previous contents. */
    public String string() {
      String s = buf.toString();
      buf.setLength(0);
      return s;
    }
  }

}

// End GeodeAdapterStreamBookshopIT.java
