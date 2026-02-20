/*
 * Teragrep DPL Spark Integration PTH-07
 * Copyright (C) 2022  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_07.ui.elements.table_dynamic.formats;

import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;

class UPlotFormatTest {
    final SparkSession sparkSession;
    final StructType schema;
    final List<Row> rows;
    final StructType timeChartSchema;
    final List<Row> timeChartRows;
    public UPlotFormatTest() {
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
                .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        schema = new StructType(
                new StructField[] {
                        new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("filesModified", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );

        rows = new ArrayList<>();
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("delete",true,1));
        rows.add(RowFactory.create("delete",false,4));
        rows.add(RowFactory.create("delete",false,5));
        rows.add(RowFactory.create("delete",false,2));
        rows.add(RowFactory.create("delete",false,3));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("update",true,1));
        rows.add(RowFactory.create("update",false,1));
        rows.add(RowFactory.create("delete",false,4));
        rows.add(RowFactory.create("update",true,1));
        rows.add(RowFactory.create("update",false,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("create",true,1));
        rows.add(RowFactory.create("delete",true,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("update",true,1));
        rows.add(RowFactory.create("update",false,1));
        rows.add(RowFactory.create("delete",false,1));
        rows.add(RowFactory.create("update",true,1));
        rows.add(RowFactory.create("update",false,1));

        timeChartSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                }
        );
        timeChartRows = new ArrayList<>();
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777771),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777772),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777773),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777774),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777775),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777776),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777778),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777779),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777780),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777781),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777782),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777783),true,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777784),false,"create"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777785),true,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777786),false,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777787),true,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777788),false,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777789),true,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777790),false,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777791),true,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777792),false,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777793),true,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777794),false,"update"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777790),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777791),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777792),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777793),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777794),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777795),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777796),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777797),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777798),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777799),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777790),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777791),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777792),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777793),false,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777794),true,"delete"));
        timeChartRows.add(RowFactory.create(Instant.ofEpochSecond(1777777795),false,"delete"));
    }
    @Test
    void testSingleRowAggregationFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| stats max(filesModified) min(filesModified)";
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema).agg(org.apache.spark.sql.functions.max("filesModified"),org.apache.spark.sql.functions.min("filesModified"));
        final int groupByCount = 0;

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));
        Assertions.assertTrue(formatted.containsKey("type"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(0,formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is aggregated but there are no groups uPlot can plot into a graph, so isAggregated should be false
        Assertions.assertEquals(false,formatted.getBoolean("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }

    @Test
    void testSingleSeriesAggregationFormat() {

        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| stats max(filesModified) min(filesModified) by success";
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema).groupBy("success").agg(org.apache.spark.sql.functions.max("filesModified"),org.apache.spark.sql.functions.min("filesModified"));
        final int groupByCount = 1;


        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is aggregated, so isAggregated should be true
        Assertions.assertEquals(true,formatted.containsKey("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }

    @Test
    void testTimechartFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test earliest=-5y\n" +
                "| spath\n" +
                "| timechart count(operation) avg(operation) max(operation)";
        final int groupByCount = 1; // DPL query contains two group by clauses due to usage of 'timechart' command
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(timeChartRows,timeChartSchema).groupBy("_time").agg(org.apache.spark.sql.functions.count("operation"));

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is aggregated, so isAggregated should be true
        Assertions.assertEquals(true,formatted.containsKey("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }

    @Test
    void testAggregatedFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test earliest=-5y\n" +
                "| spath\n" +
                "| stats count(operation) avg(operation) max(operation) by operation success";
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema).groupBy("operation","success").agg(org.apache.spark.sql.functions.count("success"),org.apache.spark.sql.functions.avg("filesModified"),org.apache.spark.sql.functions.max("filesModified"));
        final int groupByCount = 2; //  contains two group by clauses

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is aggregated, so isAggregated should be true
        Assertions.assertEquals(true,formatted.containsKey("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }

    // If other Spark methods (such as filter) are called during the creation of the Dataset, the first LogicalPlan of the dataset might not be of type Aggregate, even if aggregations were used at some point.
    // Verify that if the final operation is not a group by, aggregations are still detected and formatting still works.
    @Test
    void testPreviouslyAggregatedDatasetFormat() {
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema).groupBy("operation","success")
                .agg(org.apache.spark.sql.functions.count("success").as("countSuccess")
                        ,org.apache.spark.sql.functions.avg("filesModified").as("avgModified")
                        ,org.apache.spark.sql.functions.max("filesModified").as("maxModified"))
                .filter("countSuccess > 3");
        final int groupByCount = 2; //  contains two group by clauses

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is aggregated, so isAggregated should be true
        Assertions.assertEquals(true,formatted.containsKey("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }

    @Test
    public void testEmptyDataFrame(){
        final StructType schema = new StructType();
        final List<Row> rows = new ArrayList<>();
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema);

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        Assertions.assertThrows(InterpreterException.class,()->format.format());
    }

    @Test
    void testNonNumericalFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath";
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema);

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        // Trying to display string data (such as operation name: "create") should result in an Exception as uPlot only supports numerical data
        Assertions.assertThrows(InterpreterException.class,()-> format.format());
    }
    @Test
    void testUnaggregatedFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        final String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath";
        final Dataset<Row> resultDataset = sparkSession.createDataFrame(rows,schema).select("filesModified");
        final int groupByCount = 0;

        // Create options and Format objects to be tested
        final String graphType = "graph";
        final UPlotOptions options = new UPlotOptions(graphType);
        final UPlotFormat format = new UPlotFormat(resultDataset, options);

        final JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain "data" array, "options" object and "isAggregated" boolean
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));
        Assertions.assertTrue(formatted.containsKey("isAggregated"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);

        // First array of Data is the indexes for the series names used for X axis. It's length should be the number of unique combinations you can make with the values of the "group by" clause used.
        // In cases where aggregations are used, the dataset's size should always equal this number. If no aggregations aren't used, the number should be zero
        Assertions.assertEquals(0,formatted.getJsonArray("data").getJsonArray(0).size());

        // Second array of Data must contain one value for each column of data in the result dataset (minus number of group by fields)
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());

        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(resultDataset.count(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));

        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // Labels size must match with size of first array of Data so that each index is mapped to a label.
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());

        // Series size must match with the size of second array of Data and the number of columns in the result dataset schema (minus number of group by fields used)
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(resultDataset.schema().size()-groupByCount, formatted.getJsonObject("options").getJsonArray("series").size());

        // This dataset is not, so isAggregated should be false
        Assertions.assertEquals(false,formatted.getBoolean("isAggregated"));

        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,formatted.getString("type"));
    }
}