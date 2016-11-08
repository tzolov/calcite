package org.apache.calcite.adapter.geode.test;

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Util;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

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
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
            "  GeodeFilterRel(condition=[=(CAST($0):INTEGER, 123)])\n" +
            "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  @Test public void testWhereWithAnd() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" WHERE \"itemNumber\" > 122 AND \"itemNumber\" <= 123")
        .returnsCount(1)
        .returns("itemNumber=123; retailCost=34.99; yearPublished=2011; description=Run on sentences and drivel on " +
                "all things mundane; author=Daisy Mae West; title=A Treatise of Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
            "  GeodeFilterRel(condition=[AND(>($0, 122), <=($0, 123))])\n" +
            "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  @Test public void testWhereWithOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select \"author\" from \"BookMaster\" " +
            "WHERE \"itemNumber\" = 123 OR \"itemNumber\" = 789")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
            "  GeodeProjectRel(author=[$4])\n" +
            "    GeodeFilterRel(condition=[OR(=(CAST($0):INTEGER, 123), " +
            "=(CAST($0):INTEGER, 789))])\n" +
            "      GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }


  @Test public void testWhereWithAndOr() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("SELECT \"author\" from \"BookMaster\" " +
            "WHERE (\"itemNumber\" > 123 AND \"itemNumber\" = 789) OR \"author\"='Daisy Mae West'")
        .returnsCount(2)
        .returnsUnordered("author=Jim Heavisides", "author=Daisy Mae West")
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                         "  GeodeProjectRel(author=[$4])\n" +
                         "    GeodeFilterRel(condition=[OR(AND(>($0, 123), =(CAST($0):INTEGER, 789)), " +
                                  "=(CAST($4):VARCHAR(14) CHARACTER SET \"ISO-8859-1\" " +
                                  "COLLATE \"ISO-8859-1$en_US$primary\", 'Daisy Mae West'))])\n" +
                         "      GeodeTableScanRel(table=[[TEST, BookMaster]])\n" +
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
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
            "  GeodeProjectRel(author=[$4])\n" +
            "    GeodeFilterRel(condition=[>($0, 123)])\n" +
            "      GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  @Test public void testLimit() {
    CalciteAssert.that()
        .enable(enabled())
        .with(GEODE)
        .query("select * from \"BookMaster\" LIMIT 1")
        .returnsCount(1)
        .returns("itemNumber=123; retailCost=34.99; yearPublished=2011; description=Run on sentences and drivel on " +
                "all things mundane; author=Daisy Mae West; title=A Treatise of Treatises\n")
        .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
            "  GeodeSortRel(fetch=[1])\n" +
            "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

}
