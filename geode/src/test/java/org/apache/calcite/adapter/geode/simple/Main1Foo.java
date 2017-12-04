package org.apache.calcite.adapter.geode.simple;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;

import java.util.List;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.createClientCache;


/**
 * Tests for the {@code org.apache.calcite.adapter.geode} package.
 */
public class Main1Foo {


  public static void main(String[] args) throws Exception {

    ClientCache clientCache = createClientCache("localhost", 10334, "net.tzolov.geode.bookstore" +
        ".domain.*", true);

    QueryService queryService = clientCache.getQueryService();


    Region bookMaster = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("BookMaster");


    System.out.println("First key on server = " + bookMaster.keySetOnServer().iterator().next());
    System.out.println("First value on server = " + bookMaster.get(bookMaster.keySetOnServer()
        .iterator().next()));

    String sqlQuery4 = "select yearPublished, MAX(retailCost), COUNT(*) as cnt from /BookMaster " +
        "GROUP BY yearPublished";

    SelectResults execute = (SelectResults) queryService.newQuery(sqlQuery4).execute();

    List b = execute.asList();

    System.out.println("Result first element class type (1) = " + (execute.asList().get(0))
        .getClass());
    System.out.println("Result first element class type (2) = " + (execute.iterator().next())
        .getClass());

    execute = (SelectResults) queryService.newQuery("select * from /BookMaster").execute();

    JsonLoader loader = new JsonLoader(clientCache, "BookMaster",
        "net.tzolov.geode.bookstore.domain.*");
    loader.generateBookMasterEntries(100, 100, 1000);
  }
}

// End Main1Foo.java