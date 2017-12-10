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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example of using Geode via JDBC.
 *
 * <p>Before calling this rel, you need to populate Geode, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-rel-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with Geode and the "bookshop" and "zips" rel dataset.
 *
 * https://github.com/apache/calcite/blob/master/core/src/test/java/org/apache/calcite/examples
 * /foodmart/java/JdbcExample.java
 * https://github.com/apache/calcite/blob/master/core/src/test/java/org/apache/calcite/jdbc
 * /CalciteRemoteDriverTest.java
 */
public class StreamJdbcExample {

  protected static final Logger LOGGER = LoggerFactory.getLogger(
      StreamJdbcExample.class.getName());

  private StreamJdbcExample() {
  }

  public static void main(String[] args) throws Exception {

    final String streamSql1 = "SELECT STREAM FLOOR(\"rowtime\" TO MINUTE) AS \"rowtime\", * FROM "
        + "\"BookMasterStream\"";

    final String streamSql = "SELECT STREAM FLOOR(\"rowtime\" TO MINUTE) AS \"rowtime\" FROM "
        + "\"BookMasterStream\" GROUP BY FLOOR(\"rowtime\" TO MINUTE )";

    final String geodeModelJson =
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'bookshopstream', \n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'bookshopstream',\n"
            + "       factory: 'org.apache.calcite.adapter.geode.stream"
            + ".GeodeStreamSchemaFactory',\n"
            + "       operand: {\n"
            + "         locatorHost: 'localhost', \n"
            + "         locatorPort: '10334', \n"
            + "         regions: 'BookMasterStream', \n"
            + "         pdxSerializablePackagePath: 'org.apache.calcite.adapter.geode.*' \n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}";

    Class.forName("org.apache.calcite.jdbc.Driver");

    Properties info = new Properties();
    info.put("model", geodeModelJson);

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(streamSql);

    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        String label = metaData.getColumnLabel(i);
        Object value = resultSet.getObject(i);
        buf.append(i > 1 ? "; " : "").append(label).append("=").append(value);
      }
      LOGGER.info("Result entry: " + buf.toString());
      buf.setLength(0);
    }
    resultSet.close();
    statement.close();
    connection.close();
  }
}

// End StreamJdbcExample.java
