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
package com.teragrep.pth_07.ui.elements.table_dynamic;

import com.teragrep.pth_07.ui.elements.table_dynamic.formats.AvailableFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.DataTablesAvailableFormat;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UIOptionImpl;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.UPlotAvailableFormat;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;
import jakarta.json.JsonValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class DatasetStoreTest {
    private final String sourceDataFile = "src/test/resources/formatTestSourceData.csv";
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();

    private final StructType schema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("operation", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("success", DataTypes.BooleanType, false, new MetadataBuilder().build()),
                    new StructField("filesModified", DataTypes.IntegerType, false, new MetadataBuilder().build())
            }
    );
    private final Dataset<Row> sourceData = sparkSession.read().option("header",true).schema(schema).csv(sourceDataFile);
    private final InterpreterOutput testOutput = new InterpreterOutput();
    private final InterpreterContext testInterpreterContext = InterpreterContext.builder().setInterpreterOut(testOutput).build();

    // SaveOutput method must update the InterpreterOutput associated with the datasetStore whenever new data is received.
    @Test
    void testSaveOutput() {
        final List<AvailableFormat> availableFormats = new ArrayList<>();
        final UPlotAvailableFormat uPlotAvailableFormat = new UPlotAvailableFormat();
        final DataTablesAvailableFormat dataTablesAvailableFormat = new DataTablesAvailableFormat();
        availableFormats.add(uPlotAvailableFormat);
        availableFormats.add(dataTablesAvailableFormat);
        final DatasetStore datasetStore = new DatasetStore(sourceData,availableFormats,testInterpreterContext,new UIOptionImpl(JsonValue.EMPTY_JSON_OBJECT));
        final String payload1 = "{{\"ev1l\n\rp@ylóad]]";

        // output should be empty to begin with
        Assertions.assertEquals(0,testInterpreterContext.out().size());

        Assertions.assertDoesNotThrow(()->datasetStore.saveOutput(InterpreterResult.Type.DATATABLES,payload1));
        // output should be saved to InterpreterOutput with proper type
        Assertions.assertNotEquals(0, testInterpreterContext.out().size());
        final InterpreterResultMessage resultMessage1 = Assertions.assertDoesNotThrow(()->testInterpreterContext.out().getCurrentOutput().toInterpreterResultMessage());
        Assertions.assertEquals(payload1,resultMessage1.getData());
        Assertions.assertEquals(InterpreterResult.Type.DATATABLES,resultMessage1.getType());

        final String payload2 = "{{\"ôddlÿfo\rmд\t][ed\nPayL0ad>>]";
        Assertions.assertDoesNotThrow(()->datasetStore.saveOutput(InterpreterResult.Type.UPLOT,payload2));
        // output should be saved to InterpreterOutput with proper type
        Assertions.assertNotEquals(0, testInterpreterContext.out().size());
        final InterpreterResultMessage resultMessage2 = Assertions.assertDoesNotThrow(()->testInterpreterContext.out().getCurrentOutput().toInterpreterResultMessage());
        Assertions.assertEquals(payload2,resultMessage2.getData());
        Assertions.assertEquals(InterpreterResult.Type.UPLOT,resultMessage2.getType());
    }
}