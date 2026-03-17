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
package com.teragrep.pth_07.ui;

import com.teragrep.pth_07.ui.elements.table_dynamic.testdata.TestDPLData;
import com.teragrep.zep_01.display.AngularObject;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.display.AngularObjectRegistryListener;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterOutput;
import com.teragrep.zep_01.interpreter.InterpreterOutputListener;
import com.teragrep.zep_01.interpreter.InterpreterResultMessageOutput;
import com.teragrep.zep_01.interpreter.thrift.Options;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;


class UserInterfaceManagerTest {
    private final SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
            .config("checkpointLocation","/tmp/pth_10/test/StackTest/checkpoints/" + UUID.randomUUID() + "/")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    private final StructType testSchema = new StructType(
            new StructField[] {
                    new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("offset", DataTypes.LongType, false, new MetadataBuilder().build())
            }
    );
    private final TestDPLData testDataset = new TestDPLData(sparkSession, testSchema);
    private final Dataset<Row> testDs = testDataset.createDataset(2,0L,0L);


    String noteId = "testNote";
    String paragraphId = "testParagraph";
    String interpreterGroupId = "testInterpreterGroupId";
    TestInterpreterOutputListener testOutputListener = new TestInterpreterOutputListener();
    InterpreterOutput testOutput = new InterpreterOutput(testOutputListener);
    AngularObjectRegistryListener testRegistryListener = new AngularObjectRegistryListener() {

        @Override
        public void onAddAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        }

        @Override
        public void onUpdateAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        }

        @Override
        public void onRemoveAngularObject(final String interpreterGroupId, final AngularObject angularObject) {
        }
    };
    AngularObjectRegistry registry = new AngularObjectRegistry(interpreterGroupId, testRegistryListener);
    InterpreterContext context = InterpreterContext.builder()
            .setNoteId(noteId)
            .setParagraphId(paragraphId)
            .setInterpreterOut(testOutput)
            .setAngularObjectRegistry(registry)
            .build();
    UserInterfaceManager userInterfaceManager = new UserInterfaceManager(context);


    // Call to UserInterfaceManager.updateDataset() should result in a formatted representation of the dataset to be written to InterpreterOutput.
    @Test
    void updateDatasetTest() {
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(testDs));
        final String output = Assertions.assertDoesNotThrow(()->testOutputListener.latestOutput());
        final String expectedOutput = "%datatables {\"data\":" +
                "{\"data\":[" +
                "{\"id\":0,\"offset\":0}," +
                "{\"id\":0,\"offset\":0}]," +
                "\"draw\":1," +
                "\"recordsTotal\":2," +
                "\"recordsFiltered\":2}," +
                "\"options\":{\"headers\":" +
                "[\"id\",\"offset\"]}," +
                "\"isAggregated\":false," +
                "\"type\":\"dataTables\"}";
        Assertions.assertEquals(expectedOutput,output);
    }

    // Call to UserInterfaceManager.formatDataset() should return a formatted representation of the dataset as a String.
    @Test
    void formatDatasetTest() {
        final UPlotOptions uPlotOptions = new UPlotOptions("line");
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(testDs));
        final String formatted = Assertions.assertDoesNotThrow(()->userInterfaceManager.formatDataset(Options.uPlotOptions(uPlotOptions)));
        final String expectedOutput = "{\"data\":[[],[0,0],[0,0]],\"options\":{\"labels\":[],\"series\":[\"id\",\"offset\"],\"graphType\":\"line\"},\"isAggregated\":false,\"type\":\"uPlot\"}";
        Assertions.assertEquals(expectedOutput,formatted);
    }

    @Test
    void equalsVerifier() {
        final InterpreterContext redPerformanceIndicator = InterpreterContext.builder().setNoteId("red").build();
        final InterpreterContext bluePerformanceIndicactor = InterpreterContext.builder().setNoteId("blue").build();
        EqualsVerifier.forClass(UserInterfaceManager.class)
                .withPrefabValues(InterpreterContext.class, redPerformanceIndicator, bluePerformanceIndicactor);
    }

    private final class TestInterpreterOutputListener implements InterpreterOutputListener{

            private InterpreterResultMessageOutput latestOutput;

            @Override
            public void onUpdateAll(final InterpreterOutput out) {
            }

            @Override
            public void onAppend(final int index, final InterpreterResultMessageOutput out, final byte[] line) {
            }

            @Override
            public void onUpdate(final int index, final InterpreterResultMessageOutput out) {
                latestOutput = out;
            }

            public String latestOutput(){
                return latestOutput.toString();
            }
        }
    }
