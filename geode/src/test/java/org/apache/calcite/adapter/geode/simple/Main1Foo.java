package org.apache.calcite.adapter.geode.simple;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;
import static org.apache.calcite.adapter.geode.util.GeodeUtils.createRegionProxy;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * Created by tzoloc on 5/1/16.
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
