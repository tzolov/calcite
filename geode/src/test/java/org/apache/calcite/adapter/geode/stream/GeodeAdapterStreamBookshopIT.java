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

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

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

    String sql = "SELECT STREAM\n"
        + "  FLOOR(\"rowtime\" TO SECOND) AS \"rowtime2\" \n"
//        + "  FLOOR(\"rowtime\" TO SECOND) AS \"rowtime2\",\n"
//        + "  SUM(\"retailCost\")\n"
        + "FROM \"BookMaster\"\n"
        + "GROUP BY FLOOR(\"rowtime\" TO SECOND)\n";

    String sql2 = "SELECT STREAM\n"
        + "  FLOOR(\"rowtime\" TO SECOND) AS \"rowtime2\",\n"
        + "  SUM(\"retailCost\")\n"
        + "FROM \"BookMaster\"\n"
        + "GROUP BY FLOOR(\"rowtime\" TO SECOND)\n";

    CalciteAssert.that()
        .enable(enabled())
        .withDefaultSchema("bookshopstream")
        .with(GEODE_STREAM)
        .query(sql)
//        .returnsCount(1);
        .returns("boza");
//        .explainContains("PLAN=GeodeToEnumerableConverterRel\n"
//            + "  GeodeFilterRel(condition=[=(CAST($0):INTEGER, 123)])\n"
//            + "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

}

// End GeodeAdapterStreamBookshopIT.java
