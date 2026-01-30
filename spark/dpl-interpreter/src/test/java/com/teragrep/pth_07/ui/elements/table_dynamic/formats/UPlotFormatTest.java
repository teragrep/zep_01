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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

class UPlotFormatTest {
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
    private final int rowsToGenerate = 49;
    private final TestDPLData testDataset = new TestDPLData(sparkSession, testSchema);
    private final Dataset<Row> testDs = testDataset.createDataset(rowsToGenerate, Timestamp.from(Instant.ofEpochSecond(0)),0L,"data data","index_A","stream","host","input",String.valueOf(0),0L,"test data");


    @Test
    void testFormat() {
        DTHeader schema = new DTHeader(testDs.schema());
        String graphType = "chart";

        HashMap<String,String> optionsMap = new HashMap<>();
        optionsMap.put("graphType",graphType);
        UPlotFormatOptions options = new UPlotFormatOptions(optionsMap, "index=test earliest=-5y\n" +
                "| spath\n" +
                "| timechart count by source\n" +
                "| sort source");
        UPlotFormat format = new UPlotFormat(testDs, options);

        JsonObject formatted = Assertions.assertDoesNotThrow(()-> format.format());

        // Formatted dataset should contain the data in a transposed array.
        Assertions.assertEquals(rowsToGenerate,formatted.getJsonObject("data").getJsonArray("xAxis").size());
        Assertions.assertEquals(schema.schema().size()-1,formatted.getJsonObject("data").getJsonArray("yAxis").size());

        Assertions.assertEquals(rowsToGenerate, formatted.getJsonObject("data").getJsonArray("yAxis").getJsonArray(0).size());

        // Formatted dataset should contain options object with correct data required by the uPlot library
        Assertions.assertEquals(3, formatted.getJsonObject("options").size());
        Assertions.assertEquals(graphType, formatted.getJsonObject("options").getString("graphType"));
        Assertions.assertEquals(0, formatted.getJsonObject("options").getJsonArray("labels").size());
        Assertions.assertEquals(schema.schema().size(), formatted.getJsonObject("options").getJsonArray("series").size());

    }
}