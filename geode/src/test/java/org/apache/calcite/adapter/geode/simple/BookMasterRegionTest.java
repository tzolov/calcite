package org.apache.calcite.adapter.geode.simple;


import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import net.tzolov.geode.bookstore.domain.BookMaster;

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

public class BookMasterRegionTest {

	public static void main(String[] args) throws Exception {


		ClientCache clientCache = new ClientCacheFactory()
				.addPoolLocator("localhost", 10334)
				.setPdxSerializer(new ReflectionBasedAutoSerializer(BookMaster.class.getCanonicalName()))
				.create();

		// Using Key/Value
		Region bookMaster = clientCache
				.createClientRegionFactory(ClientRegionShortcut.PROXY)
				.create("BookMaster");

		System.out.println("BookMaster = " + bookMaster.get(789));

		// Using OQL
		QueryService queryService = clientCache.getQueryService();
		String OQL = "select itemNumber, description, retailCost from /BookMaster";
		SelectResults result = (SelectResults) queryService.newQuery(OQL).execute();
		System.out.println(result.asList());


	}
}
