package org.apache.calcite.adapter.geode.simple;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import net.tzolov.geode.bookstore.domain.BookMaster;

/**
 * Created by tzoloc on 5/1/16.
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
