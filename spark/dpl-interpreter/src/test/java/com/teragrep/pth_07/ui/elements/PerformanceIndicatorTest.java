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
package com.teragrep.pth_07.ui.elements;

import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
final class PerformanceIndicatorTest {
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .getOrCreate();

    private final AngularObjectRegistry registry = new AngularObjectRegistry("test",null);
    private final InterpreterContext context = InterpreterContext.builder().setAngularObjectRegistry(registry).build();
    private final PerformanceIndicator indicator = new PerformanceIndicator(context);
    @Test
    void performanceUpdateTest() {
        StructType schema = new StructType();

        final String firstColumnName = "firstColumn";
        final String secondColumnName = "secondColumn";
        final String thirdColumnName = "thirdColumn";

        schema = schema.add(new StructField(firstColumnName, DataTypes.IntegerType, true, Metadata.empty()));
        schema = schema.add(new StructField(secondColumnName, DataTypes.DoubleType, true, Metadata.empty()));
        schema = schema.add(new StructField(thirdColumnName, DataTypes.StringType, true, Metadata.empty()));

        final int firstInt = 100;
        final int secondInt = 300;
        final int thirdInt = 600;

        final double firstDouble = 100.5;
        final double secondDouble = 300.5;
        final double thirdDouble = 600.5;

        final String firstString = "100";
        final String secondString = "300";
        final String thirdString = "600";

        final List<Row> rows = new ArrayList<Row>();
        rows.add(RowFactory.create(firstInt,firstDouble,firstString));
        rows.add(RowFactory.create(secondInt,secondDouble,secondString));
        rows.add(RowFactory.create(thirdInt,thirdDouble,thirdString));

        // Set a Dataset to PerformanceIndicator and trigger a performance udpate
        final Dataset<Row> testDataset = sparkSession.createDataFrame(rows,schema);
        indicator.setPerformanceDataset(testDataset);
        indicator.sendPerformanceUpdate();

        // Performance update should be available in AngularObjectRegistry.
        final AngularObject batchMsg = registry.get("batchMsg",null,null);
        final String output = (String) batchMsg.get();
        final JsonArray outputJson = Assertions.assertDoesNotThrow(()->Json.createReader(new StringReader(output)).readArray());

        final JsonArray expectedJson = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                        .add(firstColumnName,firstInt)
                        .add(secondColumnName,firstDouble)
                        .add(thirdColumnName,firstString)
                        .build())
                .add(Json.createObjectBuilder()
                        .add(firstColumnName,secondInt)
                        .add(secondColumnName,secondDouble)
                        .add(thirdColumnName,secondString)
                        .build())
                .add(Json.createObjectBuilder()
                        .add(firstColumnName,thirdInt)
                        .add(secondColumnName,thirdDouble)
                        .add(thirdColumnName,thirdString)
                        .build())
                .build();

        // Resulting AngularObject should contain an array with the rows of given dataset in JSON format.
        Assertions.assertEquals(outputJson,expectedJson);
    }
}