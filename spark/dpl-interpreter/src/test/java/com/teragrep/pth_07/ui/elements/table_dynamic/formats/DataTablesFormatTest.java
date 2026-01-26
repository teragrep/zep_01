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
import com.teragrep.pth_07.ui.elements.table_dynamic.testdata.TestDPLData;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DataTablesFormatTest {
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("index", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("sourcetype", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("host", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("source", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("partition", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("origin", DataTypes.StringType, false, new MetadataBuilder().build())
            }
    );
    private final TestDPLData testDataset = new TestDPLData(sparkSession, testSchema);
    private final Dataset<Row> testDs = testDataset.createDataset(49, Timestamp.from(Instant.ofEpochSecond(0)),0L,"data data","index_A","stream","host","input",String.valueOf(0),0L,"test data");


    @Test
    void testFormat() {
        Dataset<Row> datasetAsJSON = testDs;

        final int draw = 3;
        final int start = 3;
        final int length = 2;
        final String searchString = "";
        Map<String,String> optionsMap = new HashMap<>();
        optionsMap.put("draw",Integer.toString(draw));
        optionsMap.put("start",Integer.toString(start));
        optionsMap.put("length",Integer.toString(length));
        optionsMap.put("search",searchString);

        // Get rows 3-5 of the dataset, check that every value is present
        DataTablesFormatOptions options1 = new DataTablesFormatOptions(optionsMap);
        DataTablesFormat request1 = new DataTablesFormat(datasetAsJSON,options1);
        JsonObject response1 = Assertions.assertDoesNotThrow(()->request1.format());
        Assertions.assertEquals(length,response1.getJsonArray("data").size());

        // Check metadata
        int rowCount = datasetAsJSON.collectAsList().size();
        Assertions.assertEquals(draw,response1.getInt("draw"));
        Assertions.assertEquals(rowCount,response1.getInt("recordsTotal"));
        Assertions.assertEquals(rowCount,response1.getInt("recordsFiltered"));

        // Check headers
        Assertions.assertEquals(testSchema.size(),response1.getJsonArray("headers").size());

        Assertions.assertEquals(testSchema.fieldNames()[0],response1.getJsonArray("headers").getString(0));
        Assertions.assertEquals(testSchema.fieldNames()[1],response1.getJsonArray("headers").getString(1));
        Assertions.assertEquals(testSchema.fieldNames()[2],response1.getJsonArray("headers").getString(2));
        Assertions.assertEquals(testSchema.fieldNames()[3],response1.getJsonArray("headers").getString(3));
        Assertions.assertEquals(testSchema.fieldNames()[4],response1.getJsonArray("headers").getString(4));
        Assertions.assertEquals(testSchema.fieldNames()[5],response1.getJsonArray("headers").getString(5));
        Assertions.assertEquals(testSchema.fieldNames()[6],response1.getJsonArray("headers").getString(6));
        Assertions.assertEquals(testSchema.fieldNames()[7],response1.getJsonArray("headers").getString(7));
        Assertions.assertEquals(testSchema.fieldNames()[8],response1.getJsonArray("headers").getString(8));
        Assertions.assertEquals(testSchema.fieldNames()[9],response1.getJsonArray("headers").getString(9));

        // Check data
        Assertions.assertEquals("1970-01-01T00:00:46.000Z",response1.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("1970-01-01T00:00:45.000Z",response1.getJsonArray("data").getJsonObject(1).getString("_time"));

        Assertions.assertEquals(0,response1.getJsonArray("data").getJsonObject(0).getInt("id"));
        Assertions.assertEquals(0,response1.getJsonArray("data").getJsonObject(1).getInt("id"));

        Assertions.assertEquals("index_A",response1.getJsonArray("data").getJsonObject(0).getString("index"));
        Assertions.assertEquals("index_A",response1.getJsonArray("data").getJsonObject(1).getString("index"));

        Assertions.assertEquals("data data",response1.getJsonArray("data").getJsonObject(0).getString("_raw"));
        Assertions.assertEquals("data data",response1.getJsonArray("data").getJsonObject(1).getString("_raw"));

        Assertions.assertEquals("stream",response1.getJsonArray("data").getJsonObject(0).getString("sourcetype"));
        Assertions.assertEquals("stream",response1.getJsonArray("data").getJsonObject(1).getString("sourcetype"));

        Assertions.assertEquals("host",response1.getJsonArray("data").getJsonObject(0).getString("host"));
        Assertions.assertEquals("host",response1.getJsonArray("data").getJsonObject(1).getString("host"));

        Assertions.assertEquals("input",response1.getJsonArray("data").getJsonObject(0).getString("source"));
        Assertions.assertEquals("input",response1.getJsonArray("data").getJsonObject(1).getString("source"));

        Assertions.assertEquals("0",response1.getJsonArray("data").getJsonObject(0).getString("partition"));
        Assertions.assertEquals("0",response1.getJsonArray("data").getJsonObject(1).getString("partition"));

        Assertions.assertEquals(0,response1.getJsonArray("data").getJsonObject(0).getInt("offset"));
        Assertions.assertEquals(0,response1.getJsonArray("data").getJsonObject(1).getInt("offset"));

        Assertions.assertEquals("test data",response1.getJsonArray("data").getJsonObject(0).getString("origin"));
        Assertions.assertEquals("test data",response1.getJsonArray("data").getJsonObject(1).getString("origin"));
    }

    @Test
    void testPagination() {
        // Get first 5 rows of the dataset, check values of first and last field
        Map<String,String> optionsMap1 = new HashMap<>();
        optionsMap1.put("draw",Integer.toString(0));
        optionsMap1.put("start",Integer.toString(0));
        optionsMap1.put("length",Integer.toString(5));
        optionsMap1.put("search","");
        DataTablesFormatOptions options1 = new DataTablesFormatOptions(optionsMap1);

        DataTablesFormat request1 = new DataTablesFormat(testDs,options1);
        JsonObject response1 = Assertions.assertDoesNotThrow(()->request1.format());
        Assertions.assertEquals(5,response1.getJsonArray("data").size());
        Assertions.assertEquals("1970-01-01T00:00:49.000Z",response1.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("1970-01-01T00:00:45.000Z",response1.getJsonArray("data").getJsonObject(4).getString("_time"));


        // Get rows 6-15 of the dataset, check values of first and last field

        Map<String,String> optionsMap2 = new HashMap<>();
        optionsMap2.put("draw",Integer.toString(0));
        optionsMap2.put("start",Integer.toString(5));
        optionsMap2.put("length",Integer.toString(10));
        optionsMap2.put("search","");
        DataTablesFormatOptions options2 = new DataTablesFormatOptions(optionsMap2);

        DataTablesFormat request2 = new DataTablesFormat(testDs,options2);
        JsonObject response2 = Assertions.assertDoesNotThrow(()->request2.format());
        Assertions.assertEquals(10,response2.getJsonArray("data").size());
        Assertions.assertEquals("1970-01-01T00:00:44.000Z",response2.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("1970-01-01T00:00:35.000Z",response2.getJsonArray("data").getJsonObject(9).getString("_time"));
    }
}