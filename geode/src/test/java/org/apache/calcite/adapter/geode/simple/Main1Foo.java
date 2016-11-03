package org.apache.calcite.adapter.geode.simple;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;
import static org.apache.calcite.adapter.geode.util.GeodeUtils.createRegionProxy;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

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
public class Main1Foo {

	public static void main(String[] args) throws Exception {

		ClientCache clientCache = createClientCache("localhost", 10334, "net.tzolov.geode.bookstore.domain.*", true);

		QueryService queryService = clientCache.getQueryService();


		Region bookMaster = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("BookMaster");

		System.out.println("First key on server = " + bookMaster.keySetOnServer().iterator().next());
		System.out.println("First value on server = " + bookMaster.get(bookMaster.keySetOnServer().iterator().next()));

		String sqlQuery1 = "select itemNumber, description, retailCost from /BookMaster";
		String sqlQuery2 = "select myBookOrders, lastName, primaryAddress from /Customer";
		String sqlQuery3 = "select * from /Customer";

		SelectResults execute = (SelectResults) queryService.newQuery(sqlQuery3).execute();

		System.out.println("Result first element class type (1) = " + (execute.asList().get(0)).getClass());
		System.out.println("Result first element class type (2) = " + (execute.iterator().next()).getClass());
	}
}
