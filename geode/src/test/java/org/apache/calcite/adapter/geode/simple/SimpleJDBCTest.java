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
package org.apache.calcite.adapter.geode.simple;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

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
 *
 * https://github.com/apache/calcite/blob/master/core/src/test/java/org/apache/calcite/examples/foodmart/java/JdbcExample.java
 */
public class SimpleJDBCTest {


	public static void main(String[] args) throws Exception {

		Properties info = new Properties();
		info.put("model",
				"inline:"
						+ "{\n"
						+ "  version: '1.0',\n"
						+ "  schemas: [\n"
						+ "     {\n"
						+ "       type: 'custom',\n"
						+ "       name: 'TEST',\n"
						+ "       factory: 'org.apache.calcite.adapter.geode.simple.GeodeSchemaFactory',\n"
						+ "       operand: {\n"
						+ "         locatorHost: 'localhost', \n"
						+ "         locatorPort: '10334', \n"
						+ "         regions: 'BookMaster', \n"
						+ "         pdxSerializablePackagePath: 'net.tzolov.geode.bookstore.domain.*' \n"
						+ "       }\n"
						+ "     }\n"
						+ "   ]\n"
						+ "}");

		Class.forName("org.apache.calcite.jdbc.Driver");

		Connection connection =
				DriverManager.getConnection("jdbc:calcite:", info);

		Statement statement = connection.createStatement();

	    ResultSet resultSet = statement.executeQuery("SELECT * FROM \"TEST\".\"BookMaster\"");


		final StringBuilder buf = new StringBuilder();
		while (resultSet.next()) {
			int n = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= n; i++) {
				buf.append(i > 1 ? "; " : "")
						.append(resultSet.getMetaData().getColumnLabel(i))
						.append("=")
						.append(resultSet.getObject(i));
			}
			System.out.println(buf.toString());
			buf.setLength(0);
		}

		resultSet.close();
		statement.close();
		connection.close();
	}
}
// End MainFooTest.java
