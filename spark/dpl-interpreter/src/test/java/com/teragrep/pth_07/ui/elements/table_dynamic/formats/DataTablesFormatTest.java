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

import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.DataTablesSearch;
import com.teragrep.zep_01.interpreter.thrift.Options;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;


class DataTablesFormatTest {

    private final String sourceDataFile = "src/test/resources/formatTestSourceData.csv";
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    StructType schema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                    new StructField("filesModified", DataTypes.IntegerType, false, new MetadataBuilder().build())
            }
    );
    private final Dataset<Row> sourceData = sparkSession.read().option("header",true).schema(schema).csv(sourceDataFile);

    @Test
    void testFormat() {

        final int draw = 1;
        final int start = 3;
        final int length = 2;
        final String searchString = "";
        final DataTablesOptions options = new DataTablesOptions(draw,start,length,new DataTablesSearch(searchString,false,new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        // Get rows 3-5 of the dataset, check that every value is present
        final DataTablesFormat format = new DataTablesFormat();
        final JsonObject formatted = Assertions.assertDoesNotThrow(()->format.format(sourceData, Options.dataTablesOptions(options)));
        final JsonObject data = formatted.getJsonObject("data");
        final JsonArray headers = data.getJsonArray("headers");
        final boolean isAggregated = formatted.getBoolean("isAggregated");
        final String type = formatted.getString("type");
        Assertions.assertEquals(length,data.getJsonArray("data").size());

        // Check metadata
        final int rowCount = sourceData.collectAsList().size();
        Assertions.assertEquals(draw,data.getInt("draw"));
        Assertions.assertEquals(rowCount,data.getInt("recordsTotal"));
        Assertions.assertEquals(rowCount,data.getInt("recordsFiltered"));

        // Check headers
        Assertions.assertEquals(schema.size(),data.getJsonArray("headers").size());

        Assertions.assertEquals(schema.fieldNames()[0],headers.getString(0));
        Assertions.assertEquals(schema.fieldNames()[1],headers.getString(1));
        Assertions.assertEquals(schema.fieldNames()[2],headers.getString(2));
        Assertions.assertEquals(schema.fieldNames()[3],headers.getString(3));

        // Check data
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",data.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",data.getJsonArray("data").getJsonObject(1).getString("_time"));

        Assertions.assertEquals("create",data.getJsonArray("data").getJsonObject(0).getString("operation"));
        Assertions.assertEquals("delete",data.getJsonArray("data").getJsonObject(1).getString("operation"));

        Assertions.assertEquals(true,data.getJsonArray("data").getJsonObject(0).getBoolean("success"));
        Assertions.assertEquals(true,data.getJsonArray("data").getJsonObject(1).getBoolean("success"));

        Assertions.assertEquals(1,data.getJsonArray("data").getJsonObject(0).getInt("filesModified"));
        Assertions.assertEquals(1,data.getJsonArray("data").getJsonObject(1).getInt("filesModified"));

        Assertions.assertEquals(false,isAggregated);
        Assertions.assertEquals(InterpreterResult.Type.DATATABLES.label,type);
    }

    // If other Spark methods (such as filter) are called during the creation of the Dataset, the first LogicalPlan of the dataset might not be of type Aggregate, even if aggregations were used at some point.
    // Verify that if the final operation is not a group by, aggregations are still detected and formatting still works.
    @Test
    void testAggregatedDatasetFormat(){
        final StructType aggSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("avg", DataTypes.IntegerType, false, new MetadataBuilder().build()),
                }
        );
        final Dataset aggDataset = sourceData
                .groupBy("_time")
                .agg(org.apache.spark.sql.functions.avg("filesModified").as("avg"))
                .withMetadata("_time",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());;


        final int draw = 1;
        final int start = 2;
        final int length = 2;
        final String searchString = "";
        final DataTablesOptions options = new DataTablesOptions(draw,start,length,new DataTablesSearch(searchString,false,new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        final DataTablesFormat format = new DataTablesFormat();
        final JsonObject formatted = Assertions.assertDoesNotThrow(()->format.format(aggDataset, Options.dataTablesOptions(options)));
        final JsonObject data = formatted.getJsonObject("data");
        final JsonArray headers = data.getJsonArray("headers");
        final boolean isAggregated = formatted.getBoolean("isAggregated");
        final String type = formatted.getString("type");


        // Check metadata
        final int rowCount = aggDataset.collectAsList().size();
        Assertions.assertEquals(draw,data.getInt("draw"));
        Assertions.assertEquals(rowCount,data.getInt("recordsTotal"));
        Assertions.assertEquals(rowCount,data.getInt("recordsFiltered"));

        // Check headers
        Assertions.assertEquals(aggSchema.size(),data.getJsonArray("headers").size());

        Assertions.assertEquals(aggSchema.fieldNames()[0],headers.getString(0));
        Assertions.assertEquals(aggSchema.fieldNames()[1],headers.getString(1));

        // Check data
        Assertions.assertEquals("2025-01-02T12:00:00.000Z",data.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("2025-01-03T12:00:00.000Z",data.getJsonArray("data").getJsonObject(1).getString("_time"));

        Assertions.assertEquals(2.4,data.getJsonArray("data").getJsonObject(0).getJsonNumber("avg").doubleValue());
        Assertions.assertEquals(1.0,data.getJsonArray("data").getJsonObject(1).getJsonNumber("avg").doubleValue());

        Assertions.assertEquals(true,isAggregated);
        Assertions.assertEquals(InterpreterResult.Type.DATATABLES.label,type);
    }

    @Test
    void testPreviouslyAggregatedDatasetFormat(){
        final StructType aggSchema = new StructType(
                new StructField[] {
                        new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                        new StructField("avg", DataTypes.DoubleType, false, new MetadataBuilder().build()),
                        new StructField("max", DataTypes.IntegerType, false, new MetadataBuilder().build())
                }
        );
        // use multiple group bys
        final Dataset aggDataset = sourceData.groupBy("_time")
                .agg(org.apache.spark.sql.functions.avg("filesModified").as("avg"),
                        org.apache.spark.sql.functions.sum("filesModified").as("sum"),
                        org.apache.spark.sql.functions.max("filesModified").as("max"))
                .groupBy("_time","avg")
                .agg(org.apache.spark.sql.functions.sum("max").as("max"))
                .withMetadata("_time",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build())
                .withMetadata("avg",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());

        final int draw = 1;
        final int start = 3;
        final int length = 2;
        final String searchString = "";
        final DataTablesOptions options = new DataTablesOptions(draw,start,length,new DataTablesSearch(searchString,false,new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        final DataTablesFormat format = new DataTablesFormat();
        final JsonObject formatted = Assertions.assertDoesNotThrow(()->format.format(aggDataset, Options.dataTablesOptions(options)));
        final JsonObject data = formatted.getJsonObject("data");
        final JsonArray headers = data.getJsonArray("headers");
        final boolean isAggregated = formatted.getBoolean("isAggregated");
        final String type = formatted.getString("type");

        // Check metadata
        final int rowCount = aggDataset.collectAsList().size();
        Assertions.assertEquals(draw,data.getInt("draw"));
        Assertions.assertEquals(rowCount,data.getInt("recordsTotal"));
        Assertions.assertEquals(rowCount,data.getInt("recordsFiltered"));

        // Check headers
        Assertions.assertEquals(aggSchema.size(),data.getJsonArray("headers").size());

        Assertions.assertEquals(aggSchema.fieldNames()[0],headers.getString(0));
        Assertions.assertEquals(aggSchema.fieldNames()[1],headers.getString(1));
        Assertions.assertEquals(aggSchema.fieldNames()[2],headers.getString(2));

        // Check data
        Assertions.assertEquals("2025-01-03T12:00:00.000Z",data.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("2025-01-14T12:00:00.000Z",data.getJsonArray("data").getJsonObject(1).getString("_time"));

        Assertions.assertEquals(1.0,data.getJsonArray("data").getJsonObject(0).getJsonNumber("avg").doubleValue());
        Assertions.assertEquals(1.0,data.getJsonArray("data").getJsonObject(1).getJsonNumber("avg").doubleValue());

        Assertions.assertEquals(1,data.getJsonArray("data").getJsonObject(0).getInt("max"));
        Assertions.assertEquals(1,data.getJsonArray("data").getJsonObject(1).getInt("max"));

        Assertions.assertEquals(true,isAggregated);
        Assertions.assertEquals(InterpreterResult.Type.DATATABLES.label,type);
    }

    @Test
    void testPagination() {
        // Get first 5 rows of the dataset,
        final int draw1 = 0;
        final int start1 = 0;
        final int length1 = 5;
        final String searchString1 = "";
        final DataTablesOptions options1 = new DataTablesOptions(draw1,start1,length1,new DataTablesSearch(searchString1,false,new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        final DataTablesFormat format1 = new DataTablesFormat();
        final JsonObject formatted1 = Assertions.assertDoesNotThrow(()->format1.format(sourceData, Options.dataTablesOptions(options1)).getJsonObject("data"));
        Assertions.assertEquals(5,formatted1.getJsonArray("data").size());

        Assertions.assertEquals("2025-01-01T12:00:00.000Z",formatted1.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",formatted1.getJsonArray("data").getJsonObject(1).getString("_time"));
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",formatted1.getJsonArray("data").getJsonObject(2).getString("_time"));
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",formatted1.getJsonArray("data").getJsonObject(3).getString("_time"));
        Assertions.assertEquals("2025-01-01T12:00:00.000Z",formatted1.getJsonArray("data").getJsonObject(4).getString("_time"));


        Assertions.assertEquals("create",formatted1.getJsonArray("data").getJsonObject(0).getString("operation"));
        Assertions.assertEquals("delete",formatted1.getJsonArray("data").getJsonObject(1).getString("operation"));
        Assertions.assertEquals("update",formatted1.getJsonArray("data").getJsonObject(2).getString("operation"));
        Assertions.assertEquals("create",formatted1.getJsonArray("data").getJsonObject(3).getString("operation"));
        Assertions.assertEquals("delete",formatted1.getJsonArray("data").getJsonObject(4).getString("operation"));

        Assertions.assertEquals(true,formatted1.getJsonArray("data").getJsonObject(0).getBoolean("success"));
        Assertions.assertEquals(true,formatted1.getJsonArray("data").getJsonObject(1).getBoolean("success"));
        Assertions.assertEquals(true,formatted1.getJsonArray("data").getJsonObject(2).getBoolean("success"));
        Assertions.assertEquals(true,formatted1.getJsonArray("data").getJsonObject(3).getBoolean("success"));
        Assertions.assertEquals(true,formatted1.getJsonArray("data").getJsonObject(4).getBoolean("success"));


        Assertions.assertEquals(1,formatted1.getJsonArray("data").getJsonObject(0).getInt("filesModified"));
        Assertions.assertEquals(2,formatted1.getJsonArray("data").getJsonObject(1).getInt("filesModified"));
        Assertions.assertEquals(1,formatted1.getJsonArray("data").getJsonObject(2).getInt("filesModified"));
        Assertions.assertEquals(1,formatted1.getJsonArray("data").getJsonObject(3).getInt("filesModified"));
        Assertions.assertEquals(1,formatted1.getJsonArray("data").getJsonObject(4).getInt("filesModified"));
        Assertions.assertEquals(1,formatted1.getInt("draw"));

        // Get rows 10-15 of the dataset
        final int draw2 = 1;
        final int start2 = 9;
        final int length2 = 5;
        final String searchString2 = "";
        final DataTablesOptions options2 = new DataTablesOptions(draw2,start2,length2,new DataTablesSearch(searchString2,false,new ArrayList<>()),new ArrayList<>(), new ArrayList<>());

        final JsonObject formatted2 = Assertions.assertDoesNotThrow(()->format1.format(sourceData, Options.dataTablesOptions(options2)).getJsonObject("data"));

        Assertions.assertEquals(5,formatted2.getJsonArray("data").size());

        Assertions.assertEquals("2025-01-02T12:00:00.000Z",formatted2.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("2025-01-02T12:00:00.000Z",formatted2.getJsonArray("data").getJsonObject(1).getString("_time"));
        Assertions.assertEquals("2025-01-03T12:00:00.000Z",formatted2.getJsonArray("data").getJsonObject(2).getString("_time"));
        Assertions.assertEquals("2025-01-04T12:00:00.000Z",formatted2.getJsonArray("data").getJsonObject(3).getString("_time"));
        Assertions.assertEquals("2025-01-05T12:00:00.000Z",formatted2.getJsonArray("data").getJsonObject(4).getString("_time"));


        Assertions.assertEquals("delete",formatted2.getJsonArray("data").getJsonObject(0).getString("operation"));
        Assertions.assertEquals("update",formatted2.getJsonArray("data").getJsonObject(1).getString("operation"));
        Assertions.assertEquals("update",formatted2.getJsonArray("data").getJsonObject(2).getString("operation"));
        Assertions.assertEquals("delete",formatted2.getJsonArray("data").getJsonObject(3).getString("operation"));
        Assertions.assertEquals("update",formatted2.getJsonArray("data").getJsonObject(4).getString("operation"));

        Assertions.assertEquals(false,formatted2.getJsonArray("data").getJsonObject(0).getBoolean("success"));
        Assertions.assertEquals(true,formatted2.getJsonArray("data").getJsonObject(1).getBoolean("success"));
        Assertions.assertEquals(false,formatted2.getJsonArray("data").getJsonObject(2).getBoolean("success"));
        Assertions.assertEquals(false,formatted2.getJsonArray("data").getJsonObject(3).getBoolean("success"));
        Assertions.assertEquals(true,formatted2.getJsonArray("data").getJsonObject(4).getBoolean("success"));


        Assertions.assertEquals(1,formatted2.getJsonArray("data").getJsonObject(0).getInt("filesModified"));
        Assertions.assertEquals(1,formatted2.getJsonArray("data").getJsonObject(1).getInt("filesModified"));
        Assertions.assertEquals(1,formatted2.getJsonArray("data").getJsonObject(2).getInt("filesModified"));
        Assertions.assertEquals(4,formatted2.getJsonArray("data").getJsonObject(3).getInt("filesModified"));
        Assertions.assertEquals(1,formatted2.getJsonArray("data").getJsonObject(4).getInt("filesModified"));

        Assertions.assertEquals(2,formatted2.getInt("draw"));
    }
}