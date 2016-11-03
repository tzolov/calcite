package org.apache.calcite.adapter.geode.test;

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Util;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Created by tzoloc on 5/16/16.
 */
public class GeodeAdapter2IT {
  /**
   * Connection factory based on the "mongo-zips" model.
   */
  public static final ImmutableMap<String, String> GEODE =
      ImmutableMap.of("model",
          GeodeAdapter2IT.class.getResource("/model-rel.json")
              .getPath());


  /**
   * Whether to run Geode tests. Enabled by default, however test is only
   * included if "it" profile is activated ({@code -Pit}). To disable,
   * specify {@code -Dcalcite.test.geode=false} on the Java command line.
   */
  public static final boolean ENABLED = Util.getBooleanProperty("calcite.test.geode", true);

  /**
   * Whether to run this test.
   */
  protected boolean enabled() {
    return ENABLED;
  }

  @Test public void testSelect() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\"")
        .returnsCount(3);
  }

  @Test public void testWhereEqual() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" WHERE \"itemNumber\" = 123")
        .returnsCount(1)
        .returns("itemNumber=123; retailCost=34.99; yearPublished=2011; description=Run on sentences and drivel on " +
                "all things mundane; author=Daisy Mae West; title=A Treatise of Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeFilter(condition=[=(CAST($0):INTEGER, 123)])\n" +
            "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test public void testWhereWithAnd() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" WHERE \"itemNumber\" > 122 AND \"itemNumber\" <= 123")
        .returnsCount(1)
        .returns("itemNumber=123; retailCost=34.99; yearPublished=2011; description=Run on sentences and drivel on " +
                "all things mundane; author=Daisy Mae West; title=A Treatise of Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeFilter(condition=[AND(>($0, 122), <=($0, 123))])\n" +
            "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test public void testWhereWithOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"author\" from \"BookMaster\" " +
            "WHERE \"itemNumber\" = 123 OR \"itemNumber\" = 789")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeProject(author=[$4])\n" +
            "    GeodeFilter(condition=[OR(=(CAST($0):INTEGER, 123), " +
            "=(CAST($0):INTEGER, 789))])\n" +
            "      GeodeTableScan(table=[[TEST, BookMaster]])\n");
  }


  @Test public void testWhereWithAndOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"author\" from \"BookMaster\" " +
            "WHERE (\"itemNumber\" > 123 AND \"itemNumber\" = 789) OR \"author\"='Daisy Mae West'")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeProject(author=[$4])\n" +
            "    GeodeFilter(condition=[OR(AND(>($0, 123), =(CAST($0):INTEGER, 789)), " +
            "=(CAST($4):VARCHAR(14) CHARACTER SET \"ISO-8859-1\" " +
            "COLLATE \"ISO-8859-1$en_US$primary\", 'Daisy Mae West'))])\n" +
            "      GeodeTableScan(table=[[TEST, BookMaster]])\n" +
            "\n");
  }

  //TODO: Not supported YET
  @Test public void testWhereWithOrAnd() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"author\" from \"BookMaster\" " +
            "WHERE (\"itemNumber\" > 100 OR \"itemNumber\" = 789) AND \"author\"='Daisy Mae West'")
        .returnsCount(1)
        .returnsUnordered("author=Daisy Mae West")
        .explainContains("");
  }


  @Test public void testProjectionsAndWhereGreatThan() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"author\" from \"BookMaster\" WHERE \"itemNumber\" > 123")
        .returnsCount(2)
        .returns("author=Clarence Meeks\n" +
            "author=Jim Heavisides\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeProject(author=[$4])\n" +
            "    GeodeFilter(condition=[>($0, 123)])\n" +
            "      GeodeTableScan(table=[[TEST, BookMaster]])");
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" LIMIT 1")
        .returnsCount(1)
        .returns("itemNumber=123; retailCost=34.99; yearPublished=2011; description=Run on sentences and drivel on " +
                "all things mundane; author=Daisy Mae West; title=A Treatise of Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverter\n" +
            "  GeodeSort(fetch=[1])\n" +
            "    GeodeTableScan(table=[[TEST, BookMaster]])");
  }

}
