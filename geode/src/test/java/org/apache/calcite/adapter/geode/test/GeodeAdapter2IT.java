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
   * Connection factory based on the "geode relational " model.
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

  @Test public void testSortWithLimit() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select * from \"BookMaster\" ORDER BY \"yearPublished\" LIMIT 1")
            .returnsCount(1)
            .returns("itemNumber=456; retailCost=11.99; yearPublished=1971; description=A book about a dog; " +
                    "author=Clarence Meeks; title=Clifford the Big Red Dog\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeSortRel(sort0=[$2], dir0=[ASC], fetch=[1])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  @Test public void testSortBy2Columns() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\", \"itemNumber\" from \"BookMaster\" ORDER BY \"yearPublished\" ASC, \"itemNumber\" DESC")
            .returnsCount(3)
            .returns("yearPublished=1971; itemNumber=456\n" +
                    "yearPublished=2011; itemNumber=789\n" +
                    "yearPublished=2011; itemNumber=123\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC])\n" +
                    "    GeodeProjectRel(yearPublished=[$2], itemNumber=[$0])\n" +
                    "      GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  //
  // TEST Group By and Aggregation Function Support
  //

  /**
   * OQL Error: Query contains group by columns not present in projected fields
   * Solution: Automatically expand the projections to include all missing GROUP By columns.
   */
  @Test public void testAddMissingGroupByCoumnToProjectedFields() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\" from \"BookMaster\" GROUP BY  \"yearPublished\", \"author\"")
            .returnsCount(3)
            .returns("yearPublished=1971\n" +
                    "yearPublished=2011\n" +
                    "yearPublished=2011\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeProjectRel(yearPublished=[$0])\n" +
                    "    GeodeAggregateRel(group=[{2, 4}])\n" +
                    "      GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected relation.
   */
  @Test public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\" from \"BookMaster\" GROUP BY \"yearPublished\"")
            .returnsCount(2)
            .returns("yearPublished=1971\n" +
                    "yearPublished=2011\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{2}])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  /**
   * When the group by columns match the projected fields, the optimizers removes the projected relation.
   */
  @Test public void testMissingProjectRelationOnGroupByColumnMatchingProjectedFields2() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\", MAX(\"retailCost\") from \"BookMaster\" GROUP BY \"yearPublished\"")
            .returnsCount(2)
            .returns("yearPublished=1971; EXPR$1=11.99\n" +
                    "yearPublished=2011; EXPR$1=59.99\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{2}], EXPR$1=[MAX($1)])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])");
  }

  @Test public void testCount() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select COUNT(\"retailCost\") from \"BookMaster\"")
            .returnsCount(1)
            .returns("EXPR$0=3\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{}], EXPR$0=[COUNT($1)])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }

  @Test public void testCountStar() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select COUNT(*) from \"BookMaster\"")
            .returnsCount(1)
            .returns("EXPR$0=3\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{}], EXPR$0=[COUNT()])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }

  @Test public void testCountInGroupBy() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\", COUNT(\"retailCost\") from \"BookMaster\" GROUP BY \"yearPublished\"")
            .returnsCount(2)
            .returns("yearPublished=1971; EXPR$1=1\n" +
                    "yearPublished=2011; EXPR$1=2\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{2}], EXPR$1=[COUNT($1)])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }

  @Test public void testMaxMinSumAvg() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select MAX(\"retailCost\"), MIN(\"retailCost\"), SUM(\"retailCost\"), AVG(\"retailCost\") from \"BookMaster\"")
            .returnsCount(1)
            .returns("EXPR$0=59.99; EXPR$1=11.99; EXPR$2=106.97; EXPR$3=35.656666\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{}], EXPR$0=[MAX($1)], EXPR$1=[MIN($1)], EXPR$2=[SUM($1)], EXPR$3=[AVG($1)])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }

  @Test public void testMaxMinSumAvgInGroupBy() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\", MAX(\"retailCost\"), MIN(\"retailCost\"), SUM(\"retailCost\"), AVG(\"retailCost\") from \"BookMaster\" GROUP BY  \"yearPublished\"")
            .returnsCount(2)
            .returns("yearPublished=2011; EXPR$1=59.99; EXPR$2=34.99; EXPR$3=94.98; EXPR$4=47.49\n" +
                    "yearPublished=1971; EXPR$1=11.99; EXPR$2=11.99; EXPR$3=11.99; EXPR$4=11.99\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeAggregateRel(group=[{2}], EXPR$1=[MAX($1)], EXPR$2=[MIN($1)], EXPR$3=[SUM($1)], EXPR$4=[AVG($1)])\n" +
                    "    GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }

  @Test public void testGroupBy() {
    CalciteAssert.that()
            .enable(enabled())
            .with(GEODE)
            .query("select \"yearPublished\", MAX(\"retailCost\") AS MAXCOST, \"author\" from \"BookMaster\" GROUP BY \"yearPublished\", \"author\"")
            .returnsCount(3)
            .returns("yearPublished=2011; MAXCOST=59.99; author=Jim Heavisides\n" +
                    "yearPublished=2011; MAXCOST=34.99; author=Daisy Mae West\n" +
                    "yearPublished=1971; MAXCOST=11.99; author=Clarence Meeks\n")
            .explainContains("PLAN=GeodeToEnumerableConverterRel\n" +
                    "  GeodeProjectRel(yearPublished=[$0], MAXCOST=[$2], author=[$1])\n" +
                    "    GeodeAggregateRel(group=[{2, 4}], MAXCOST=[MAX($1)])\n" +
                    "      GeodeTableScanRel(table=[[TEST, BookMaster]])\n");
  }
}
