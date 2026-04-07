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

import jakarta.json.JsonArray;
import jakarta.json.JsonValue;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class UPlotDataTest {
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
    void asJsonTest(){
        List<Row> rows = sourceData.collectAsList();
        UPlotData data = new UPlotData(rows,false);
        JsonArray json = Assertions.assertDoesNotThrow(()->data.asJson().asJsonArray());
        Assertions.assertEquals(5,json.size());
        JsonArray xAxis = Assertions.assertDoesNotThrow(()->json.getJsonArray(0));
        JsonArray time = Assertions.assertDoesNotThrow(()->json.getJsonArray(1));
        JsonArray operation = Assertions.assertDoesNotThrow(()->json.getJsonArray(2));
        JsonArray success = Assertions.assertDoesNotThrow(()->json.getJsonArray(3));
        JsonArray filesModified = Assertions.assertDoesNotThrow(()->json.getJsonArray(4));

        // Data is not aggregated, so X-axis should be 0, others should contain one value for every row
        Assertions.assertEquals(0, xAxis.size());
        Assertions.assertEquals(rows.size(), time.size());
        Assertions.assertEquals(rows.size(), operation.size());
        Assertions.assertEquals(rows.size(), success.size());
        Assertions.assertEquals(rows.size(), filesModified.size());

        // Values should be nulls for every non-numerical piece of data. In this case only filesModified is numerical.
        Assertions.assertEquals(JsonValue.ValueType.NULL,time.get(0).getValueType());
        Assertions.assertEquals(JsonValue.ValueType.NULL,operation.get(0).getValueType());
        Assertions.assertEquals(JsonValue.ValueType.NULL,success.get(0).getValueType());
        Assertions.assertEquals(JsonValue.ValueType.NUMBER,filesModified.get(0).getValueType());
    }

    @Test
    void aggregatedAsJsonTest() {
        List<Row> rows = sourceData.groupBy("_time").agg(functions.max("filesModified")).withMetadata("_time", new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn", true).build()).collectAsList();
        UPlotData data = new UPlotData(rows, true);
        JsonArray json = Assertions.assertDoesNotThrow(() -> data.asJson().asJsonArray());
        Assertions.assertEquals(2, json.size());
        JsonArray xAxis = Assertions.assertDoesNotThrow(() -> json.getJsonArray(0));
        JsonArray maxFilesModified = Assertions.assertDoesNotThrow(() -> json.getJsonArray(1));

        // Data is  aggregated, so X-axis should be the same size as rows in the dataset.
        Assertions.assertEquals(rows.size(), xAxis.size());
        Assertions.assertEquals(rows.size(), maxFilesModified.size());

        // Both X-axis and data should be numerical
        Assertions.assertEquals(JsonValue.ValueType.NUMBER, xAxis.get(0).getValueType());
        Assertions.assertEquals(JsonValue.ValueType.NUMBER, maxFilesModified.get(0).getValueType());
    }

    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(UPlotData.class)
                .verify();
    }
}