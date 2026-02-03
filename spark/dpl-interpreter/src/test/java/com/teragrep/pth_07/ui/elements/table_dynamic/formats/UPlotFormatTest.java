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

import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.UPlotFormatOptions;
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
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    @Test
    void testSingleRowAggregationFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats max(countOperation) min(countOperation)";
        int groupByCount = 0; // DPL query does not contain any group by clauses
        final StructType datasetSchema = new StructType(
                new StructField[] {
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("min(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("avg(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );
        List<Row> rows = new ArrayList<>();
        Random random = new Random();
        rows.add(RowFactory.create(random.nextInt(500),random.nextInt(5),random.nextInt(25)));
        final Dataset<Row> dataset = sparkSession.createDataFrame(rows,datasetSchema);

        // Create a map containing  object to simulate a formatting request received from UI
        String graphType = "graph";
        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);

        // Create options and Format objects to be tested
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, dplQuery);
        UPlotFormat format = new UPlotFormat(dataset, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain both "data" array and "options" object
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);
        // First array of Data is the indexes for the series names used for X axis, so it's length should be zero if there are no group by clauses in the DPL query
        Assertions.assertEquals(0,formatted.getJsonArray("data").getJsonArray(0).size());
        // Second array of Data must contain one value for each column of data in the original dataset (minus number of group by fields)
        Assertions.assertEquals(datasetSchema.size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());
        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));
        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        // Labels size must match with size of first array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());
        // Series size must match with the size of second array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
    }

    @Test
    void testSingleSeriesAggregationFormat() {

        // Create test dataset and a query string to simulate most recent dataset received from DPL
        String dplQuery = "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats max(countOperation) min(countOperation) by success";
        int groupByCount = 1; // DPL query contains one group by clause
        final StructType datasetSchema = new StructType(
                new StructField[] {
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("min(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("avg(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                }
        );

        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(true,random.nextInt(500),random.nextInt(5),random.nextInt(25)));
        rows.add(RowFactory.create(false,random.nextInt(500),random.nextInt(5),random.nextInt(25)));
        final Dataset<Row> dataset = sparkSession.createDataFrame(rows,datasetSchema);

        // Create a map containing  object to simulate a formatting request received from UI
        String graphType = "graph";
        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);

        // Create options and Format objects to be tested
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, dplQuery);
        UPlotFormat format = new UPlotFormat(dataset, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain both "data" array and "options" object
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);
        // First array of Data is the indexes for the series names used for X axis, so it's length should be zero if there are no group by clauses in the DPL query
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(0).size());
        // Second array of Data must contain one value for each column of data in the original dataset (minus number of group by fields)
        Assertions.assertEquals(datasetSchema.size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());
        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));
        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        // Labels size must match with size of first array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());
        // Series size must match with the size of second array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
    }

    @Test
    void testTimechartFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        String dplQuery = "%dpl\n" +
                "index=test earliest=-5y\n" +
                "| spath\n" +
                "| timechart count(operation) avg(operation) max(operation) by success";
        int groupByCount = 2; // DPL query contains two group by clauses due to usage of 'timechart' command
        final StructType datasetSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("count(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("avg(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );
        List<Row> rows = new ArrayList<>();
        Random random = new Random();
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777771),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777772),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777773),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777774),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777775),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777776),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        final Dataset<Row> dataset = sparkSession.createDataFrame(rows,datasetSchema);

        // Create a map containing  object to simulate a formatting request received from UI
        String graphType = "graph";
        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);

        // Create options and Format objects to be tested
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, dplQuery);
        UPlotFormat format = new UPlotFormat(dataset, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain both "data" array and "options" object
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);
        // First array of Data is the indexes for the series names used for X axis, so it's length should be zero if there are no group by clauses in the DPL query
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(0).size());
        // Second array of Data must contain one value for each column of data in the original dataset (minus number of group by fields)
        Assertions.assertEquals(datasetSchema.size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());
        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));
        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        // Labels size must match with size of first array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());
        // Series size must match with the size of second array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
    }

    @Test
    void testAggregatedFormat() {
        // Create test dataset and a query string to simulate most recent dataset received from DPL
        String dplQuery = "%dpl\n" +
                "index=test earliest=-5y\n" +
                "| spath\n" +
                "| stats count(operation) avg(operation) max(operation) by operation success";
        int groupByCount = 2; // DPL query contains two group by clauses
        final StructType datasetSchema = new StructType(
                new StructField[] {
                        new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("count(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("avg(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );

        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("create",true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create("create",false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create("delete",true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create("delete",false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create("update",true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create("update",false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        final Dataset<Row> dataset = sparkSession.createDataFrame(rows,datasetSchema);

        // Create a map containing  object to simulate a formatting request received from UI
        String graphType = "graph";
        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);

        // Create options and Format objects to be tested
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, dplQuery);
        UPlotFormat format = new UPlotFormat(dataset, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Object must contain both "data" array and "options" object
        Assertions.assertTrue(formatted.containsKey("data"));
        Assertions.assertTrue(formatted.containsKey("options"));

        // Data must contain at least two arrays
        Assertions.assertTrue(formatted.getJsonArray("data").size() > 1);
        // First array of Data is the indexes for the series names used for X axis, so it's length should be zero if there are no group by clauses in the DPL query
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(0).size());
        // Second array of Data must contain one value for each column of data in the original dataset (minus number of group by fields)
        Assertions.assertEquals(datasetSchema.size()-groupByCount,formatted.getJsonArray("data").getJsonArray(1).size());
        // Each sub-array within the second array of Data should contain one value for each row of data in the original dataset
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Options must contain a series array, a labels array and a graphType
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("series"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("labels"));
        Assertions.assertTrue(formatted.getJsonObject("options").containsKey("graphType"));
        // GraphType must match with what's given in the UI request
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        // Labels size must match with size of first array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(0).size(), formatted.getJsonObject("options").getJsonArray("labels").size());
        // Series size must match with the size of second array of Data
        Assertions.assertEquals(formatted.getJsonArray("data").getJsonArray(1).size(), formatted.getJsonObject("options").getJsonArray("series").size());
    }
}