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
package org.apache.calcite.adapter.geode.test;

import java.sql.SQLException;

import org.junit.Test;

/**
 * Tests for the {@code org.apache.calcite.adapter.geode} package.
 *
 * <p>Before calling this test, you need to populate Geode, as follows:
 *
 * Download a single-node Geode demo cluster (geode-sample-bootstrap-0.0.1-SNAPSHOT.jar) from:
 * https://drive.google.com/file/d/0Bw0P8rbcmBaJaGlVZWVEaWE4Tmc/view?usp=sharing
 *
 * Start it like this:
 * <blockquote><code>
 *
 *
 * java -Xmx128M -Dgemfire.name=server1 -Dgemfire.server.port=40405 -Dgemfire.jmx-manager-port=1199
 *      -Dgemfire.jmx-manager=true -Dgemfire.jmx-manager-start=true  -Dmcast-port=0
 *      -Dgemfire.STORE_ALL_VALUE_FORMS=false -Dgemfire.locators=localhost[10334]
 *      -Dgemfire.start-locator=localhost[10334] -Dgemfire.use-cluster-configuration=false
 *      -jar ./target/geode-sample-bootstrap-0.0.1-SNAPSHOT.jar
 * </code></blockquote>
 *
 * This will run a single member Geode cluster populated with Book Store sample data.
 */
public class GeodeAdapterIT extends BaseGeodeAdapterIT {

  @Test public void testSqlSimple() throws SQLException {
    checkSql("model-rel", "SELECT \"itemNumber\" FROM \"BookMaster\" WHERE \"itemNumber\" > 123");
  }

  @Test public void testSqlSingleNumberWhereFilter() throws SQLException {
    checkSql("model-rel", "SELECT * FROM \"BookMaster\" "
        + "WHERE \"itemNumber\" = 123");
  }

  @Test public void testSqlDistinctSort() throws SQLException {
    checkSql("model-rel", "SELECT DISTINCT \"itemNumber\", \"author\" "
        + "FROM \"BookMaster\" ORDER BY \"itemNumber\", \"author\"");
  }

  @Test public void testSqlDistinctSort2() throws SQLException {
    checkSql("model-rel", "SELECT \"itemNumber\", \"author\" "
        + "FROM \"BookMaster\" GROUP BY \"itemNumber\", \"author\" ORDER BY \"itemNumber\", \"author\"");
  }

  @Test public void testSqlDistinctSort3() throws SQLException {
    checkSql("model-rel", "SELECT DISTINCT * FROM \"BookMaster\"");
  }


  @Test public void testSqlLimit2() throws SQLException {
    checkSql("model-rel", "SELECT DISTINCT * FROM \"BookMaster\" LIMIT 2");
  }


  @Test public void testSqlDisjunciton() throws SQLException {
    checkSql("model-rel", "SELECT \"author\" FROM \"BookMaster\" "
        + "WHERE \"itemNumber\" = 789 OR \"itemNumber\" = 123");
  }

  @Test public void testSqlConjunciton() throws SQLException {
    checkSql("model-rel", "SELECT \"author\" FROM \"BookMaster\" "
        + "WHERE \"itemNumber\" = 789 AND \"author\" = 'Jim Heavisides'");
  }

  @Test public void testSqlBookMasterWhere() throws SQLException {
    checkSql("model-rel", "select \"author\", \"title\" from \"BookMaster\" "
        + "WHERE \"author\" = \'Jim Heavisides\' LIMIT 2");
  }

  @Test public void testSqlBookMasterCount() throws SQLException {
    checkSql("model-rel", "select count(*) from \"BookMaster\"");
  }
}
// End GeodeAdapterIT.java
