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

import com.teragrep.pth_07.ui.elements.table_dynamic.DTHeader;
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.DataTablesFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.formatOptions.UPlotFormatOptions;
import com.teragrep.pth_07.ui.elements.table_dynamic.testdata.TestDPLData;
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

import java.awt.font.NumericShaper;
import java.sql.Timestamp;
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
        final StructType nonAggregetedSchema = new StructType(
                new StructField[] {
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("min(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                }
        );

        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(random.nextInt(500),random.nextInt(5)));
        final Dataset<Row> testDs = sparkSession.createDataFrame(rows,nonAggregetedSchema);
        String graphType = "graph";

        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats max(countOperation) min(countOperation)");
        UPlotFormat format = new UPlotFormat(testDs, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Formatted dataset should contain the data in a transposed array.
        Assertions.assertEquals(testDs.schema().size(),formatted.getJsonArray("data").getJsonArray(0).size());
        Assertions.assertEquals(2,formatted.getJsonArray("data").getJsonArray(1).size());

        Assertions.assertEquals(rows.size(), formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Formatted dataset should contain options object with correct data required by the uPlot library
        Assertions.assertEquals(3, formatted.getJsonObject("options").size());
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        Assertions.assertEquals(testDs.schema().size(), formatted.getJsonObject("options").getJsonArray("labels").size());
        Assertions.assertEquals(0, formatted.getJsonObject("options").getJsonArray("series").size());
    }

    @Test
    void testSingleSeriesAggregationFormat() {
        final StructType nonAggregetedSchema = new StructType(
                new StructField[] {
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("min(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                }
        );

        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(true,random.nextInt(500),random.nextInt(5)));
        rows.add(RowFactory.create(false,random.nextInt(500),random.nextInt(5)));
        final Dataset<Row> testDs = sparkSession.createDataFrame(rows,nonAggregetedSchema);
        String graphType = "graph";

        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, "%dpl\n" +
                "index=test\n" +
                "| spath\n" +
                "| rename count AS countOperation\n" +
                "| stats max(countOperation) min(countOperation) by success");
        UPlotFormat format = new UPlotFormat(testDs, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Formatted dataset should contain the data in a transposed array.
        Assertions.assertEquals(2,formatted.getJsonArray("data").getJsonArray(0).size());
        Assertions.assertEquals(testDs.schema().size()-1,formatted.getJsonArray("data").getJsonArray(1).size());

        Assertions.assertEquals(rows.size(), formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Formatted dataset should contain options object with correct data required by the uPlot library
        Assertions.assertEquals(3, formatted.getJsonObject("options").size());
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        Assertions.assertEquals(2, formatted.getJsonObject("options").getJsonArray("labels").size());
        Assertions.assertEquals("true",formatted.getJsonObject("options").getJsonArray("labels").getString(0));
        Assertions.assertEquals("false",formatted.getJsonObject("options").getJsonArray("labels").getString(1));
        Assertions.assertEquals(2, formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(testDs.schema().fieldNames()[1],formatted.getJsonObject("options").getJsonArray("series").getString(0));
        Assertions.assertEquals(testDs.schema().fieldNames()[2],formatted.getJsonObject("options").getJsonArray("series").getString(1));
    }

    @Test
    void testTimechartFormat() {
        final StructType aggregatedTestSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                        new StructField("count(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("avg(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                        new StructField("max(operation)", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );

        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777771),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777772),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777773),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777774),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777775),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777776),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),true,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        rows.add(RowFactory.create(Instant.ofEpochSecond(1777777777),false,random.nextInt(500),random.nextInt(5),random.nextInt(50)));
        final Dataset<Row> aggregatedTestDs = sparkSession.createDataFrame(rows,aggregatedTestSchema);

        String graphType = "chart";

        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, "index=test earliest=-5y\n" +
                "| spath\n" +
                "| timechart count(operation) avg(operation) max(operation) by success");
        UPlotFormat format = new UPlotFormat(aggregatedTestDs, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Formatted dataset should contain the data in a transposed array.
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(0).size());
        Assertions.assertEquals(aggregatedTestDs.schema().size()-2,formatted.getJsonArray("data").getJsonArray(1).size());
        Assertions.assertEquals(rows.size(), formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Formatted dataset should contain options object with correct data required by the uPlot library
        Assertions.assertEquals(3, formatted.getJsonObject("options").size());
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        // There are two identical labels in the test dataset, so the number of unique labels should be 10-2 = 8
        Assertions.assertEquals(rows.size()-2, formatted.getJsonObject("options").getJsonArray("labels").size());
        Assertions.assertEquals("1777777771.true", formatted.getJsonObject("options").getJsonArray("labels").getString(0));
        Assertions.assertEquals("1777777772.false", formatted.getJsonObject("options").getJsonArray("labels").getString(1));

        Assertions.assertEquals(3, formatted.getJsonObject("options").getJsonArray("series").size());
    }

    @Test
    void testAggregatedFormat() {
        final StructType aggregatedTestSchema = new StructType(
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
        final Dataset<Row> aggregatedTestDs = sparkSession.createDataFrame(rows,aggregatedTestSchema);

        DTHeader schema = new DTHeader(aggregatedTestDs.schema());
        String graphType = "chart";

        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, "index=test earliest=-5y\n" +
                "| spath\n" +
                "| stats count(operation) avg(operation) max(operation) by operation success");
        UPlotFormat format = new UPlotFormat(aggregatedTestDs, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Formatted dataset should contain the data in a transposed array.
        Assertions.assertEquals(rows.size(),formatted.getJsonArray("data").getJsonArray(0).size());
        Assertions.assertEquals(schema.schema().size()-2,formatted.getJsonArray("data").getJsonArray(1).size());

        Assertions.assertEquals(rows.size(), formatted.getJsonArray("data").getJsonArray(1).getJsonArray(0).size());

        // Formatted dataset should contain options object with correct data required by the uPlot library
        Assertions.assertEquals(3, formatted.getJsonObject("options").size());
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));

        Assertions.assertEquals(3, formatted.getJsonObject("options").getJsonArray("series").size());
        Assertions.assertEquals(6, formatted.getJsonObject("options").getJsonArray("labels").size());
    }
}