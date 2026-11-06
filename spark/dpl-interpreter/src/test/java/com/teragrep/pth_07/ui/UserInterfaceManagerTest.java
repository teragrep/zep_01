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

import com.teragrep.pth_07.ui.elements.table_dynamic.DatasetStore;
import com.teragrep.pth_07.ui.elements.table_dynamic.formats.*;
import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.display.AngularObjectRegistryListener;
import com.teragrep.zep_01.interpreter.*;
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

import java.util.ArrayList;
import java.util.List;


public final class UserInterfaceManagerTest {
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
    private final String batch1DataFile = "src/test/resources/batchedTestSourceData1.csv";
    private final Dataset<Row> batch1 = sparkSession.read().option("header",true).schema(schema).csv(batch1DataFile);

    private final String batch2DataFile = "src/test/resources/batchedTestSourceData2.csv";
    private final Dataset<Row> batch2 = sparkSession.read().option("header",true).schema(schema).csv(batch2DataFile);

    private final Dataset<Row> aggregatedBatch1 = batch1
            .groupBy("operation")
            .agg(org.apache.spark.sql.functions.max("filesModified"),org.apache.spark.sql.functions.min("filesModified"))
            .withMetadata("operation",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());

    private final Dataset<Row> aggregatedBatch2 = batch2
            .groupBy("operation")
            .agg(org.apache.spark.sql.functions.max("filesModified"),org.apache.spark.sql.functions.min("filesModified"))
            .withMetadata("operation",new MetadataBuilder().putBoolean("dpl_internal_isGroupByColumn",true).build());


    private final String noteId = "testNote";
    private final String paragraphId = "testParagraph";
    private final String interpreterGroupId = "testInterpreterGroupId";
    private final FakeInterpreterOutputListener testOutputListener = new FakeInterpreterOutputListener();
    private final InterpreterOutput testOutput = new InterpreterOutput(testOutputListener);
    private final AngularObjectRegistryListener registryListener = new FakeAngularObjectRegistryListener();
    AngularObjectRegistry registry = new AngularObjectRegistry(interpreterGroupId, registryListener);
    InterpreterContext context = InterpreterContext.builder()
            .setNoteId(noteId)
            .setParagraphId(paragraphId)
            .setInterpreterOut(testOutput)
            .setAngularObjectRegistry(registry)
            .build();


    /**
     * Every call to UserInterfaceManager.updateDataset() should result in a formatted representation of the dataset to be written to InterpreterOutput, using the configured deafault format, in this case DataTables.
     */
    @Test
    void testUpdateDatasetWithDataTablesFormat() {
        final List<AvailableFormat> availableFormatList = new ArrayList<>();
        availableFormatList.add(new DataTablesAvailableFormat());
        final Dataset<Row> emptyDataset = sparkSession.emptyDataFrame();
        final JsonObject json = Json.createObjectBuilder()
                .add("paragraphId","paragraph_1777976743753_395717996")
                .add("noteId","2MRV3E2UT")
                .add("type", "dataTables")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("draw",1)
                        .add("start",0)
                        .add("length",50)
                        .add("search",Json.createObjectBuilder()
                                .add("value","")
                                .add("regex",false)
                                .add("fixed",Json.createArrayBuilder().build())
                                .build()).build())
                .build();
        final UIOption defaultUIOption = new UIOptionImpl(json);
        final UserInterfaceManager userInterfaceManager = new UserInterfaceManager(context,emptyDataset,defaultUIOption,availableFormatList);

        Assertions.assertEquals(0,testOutput.size());
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(batch1));
        final List<InterpreterResultMessage> outputList = Assertions.assertDoesNotThrow(()->testOutputListener.outputs());

        // Data should be available in InterpreterOutput
        Assertions.assertEquals(1,testOutput.size());
        Assertions.assertEquals(1,outputList.size());

        final JsonObject expectedDataTablesBatch1Json = Json.createObjectBuilder()
                .add("data", Json.createObjectBuilder()
                        .add("data", Json.createArrayBuilder()
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","create")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",true)
                                        .add("filesModified",2))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","update")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","create")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",true)
                                        .add("filesModified",1)))
                        .add("draw",1)
                        .add("recordsTotal",5)
                        .add("recordsFiltered",5))
                .add("options", Json.createObjectBuilder()
                        .add("headers",Json.createArrayBuilder()

                                .add("_time")

                                .add("operation")

                                .add("success")

                                .add("filesModified")))
                .add("isAggregated",false)
                .add("type","dataTables")
                .build();
        final String expectedDataTablesBatch1Output = "%datatables "+expectedDataTablesBatch1Json.toString();
        Assertions.assertEquals(expectedDataTablesBatch1Output, outputList.get(0).toString());


        // second batch should be formatted properly
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(batch2));

        final JsonObject expectedDataTablesBatch2Json = Json.createObjectBuilder()
                .add("data", Json.createObjectBuilder()
                        .add("data", Json.createArrayBuilder()
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","create")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",true)
                                        .add("filesModified",2))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","update")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","create")
                                        .add("success",true)
                                        .add("filesModified",1))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",true)
                                        .add("filesModified",1))


                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-01T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",false)
                                        .add("filesModified",4))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-02T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",false)
                                        .add("filesModified",5))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-02T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",false)
                                        .add("filesModified",2))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-02T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",false)
                                        .add("filesModified",3))
                                .add(Json.createObjectBuilder()
                                        .add("_time","2025-01-02T12:00:00.000Z")
                                        .add("operation","delete")
                                        .add("success",false)
                                        .add("filesModified",1))
                        )
                        .add("draw",1)
                        .add("recordsTotal",10)
                        .add("recordsFiltered",10))
                .add("options", Json.createObjectBuilder()
                        .add("headers",Json.createArrayBuilder()
                                .add("_time")
                                .add("operation")
                                .add("success")
                                .add("filesModified")))
                .add("isAggregated",false)
                .add("type","dataTables")
                .build();
        final String expectedDataTablesBatch2Output = "%datatables "+expectedDataTablesBatch2Json.toString();
        Assertions.assertEquals(expectedDataTablesBatch2Output, outputList.get(1).toString());

    }
    /**
     * Every call to UserInterfaceManager.updateDataset() should result in a formatted representation of the dataset to be written to InterpreterOutput, using the configured deafault format, in this case uPlot.
     */
    @Test
    void testUpdateDatasetWithUPlotFormat(){
        final List<AvailableFormat> availableFormatList = new ArrayList<>();
        availableFormatList.add(new UPlotAvailableFormat());
        final Dataset<Row> emptyDataset = sparkSession.emptyDataFrame();
        final JsonObject json = Json.createObjectBuilder()
                .add("paragraphId","paragraphId")
                .add("noteId","dataTables")
                .add("type", "uPlot")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("graphType","line")
                        .build())
                .build();
        final UIOption defaultUIOption = new UIOptionImpl(json);
        final UserInterfaceManager userInterfaceManager = new UserInterfaceManager(context,emptyDataset,defaultUIOption,availableFormatList);

        Assertions.assertEquals(0,testOutput.size());
        // uPlot can only be used with aggregated, numerical data, so we use an aggregated batch
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(aggregatedBatch1));
        final List<InterpreterResultMessage> outputList = Assertions.assertDoesNotThrow(()->testOutputListener.outputs());

        // Data should be available in InterpreterOutput
        Assertions.assertEquals(1,testOutput.size());
        Assertions.assertEquals(1,outputList.size());

        final JsonObject uPlotOptionJson = Json.createObjectBuilder()
                .add("paragraphId","paragraphId")
                .add("noteId","dataTables")
                .add("type", "uPlot")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("graphType","line")
                        .build())
                .build();
        final UIOption uPlotUIOption = new UIOptionImpl(uPlotOptionJson);
        userInterfaceManager.updateUIOption(uPlotUIOption);

        final JsonObject expecteduPlotBatch1Json = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(Json.createArrayBuilder()
                                .add(0)
                                .add(1)
                                .add(2))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(2))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(1))
                )
                .add("options",Json.createObjectBuilder()
                        .add("labels",Json.createArrayBuilder()
                                .add("create")
                                .add("update")
                                .add("delete"))
                        .add("series",Json.createArrayBuilder()
                                .add("max(filesModified)")
                                .add("min(filesModified)"))
                        .add("graphType","line")
                        .add("xAxisLabel","operation")
                )
                .add("isAggregated",true)
                .add("type","uPlot")
                .build();
        final String expectedUplotBatch1Output = "%uplot "+expecteduPlotBatch1Json.toString();

        Assertions.assertEquals(expectedUplotBatch1Output,outputList.get(0).toString());

        // second batch should be formatted properly
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(aggregatedBatch2));
        final JsonObject expecteduPlotBatch2Json = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(Json.createArrayBuilder()
                                .add(0)
                                .add(1)
                                .add(2))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(5))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(1))
                )
                .add("options",Json.createObjectBuilder()
                        .add("labels",Json.createArrayBuilder()
                                .add("create")
                                .add("update")
                                .add("delete"))
                        .add("series",Json.createArrayBuilder()
                                .add("max(filesModified)")
                                .add("min(filesModified)"))
                        .add("graphType","line")
                        .add("xAxisLabel","operation")
                )
                .add("isAggregated",true)
                .add("type","uPlot")
                .build();
        final String expectedUplotBatch2Output = "%uplot "+expecteduPlotBatch2Json.toString();

        Assertions.assertEquals(expectedUplotBatch2Output,outputList.get(1).toString());
    }

    /**
     * Call to UserInterfaceManager.formatDataset() should return a formatted representation of the dataset as a String, in the requested format.
     */
    @Test
    void formatDatasetTest() {
        final List<AvailableFormat> availableFormatList = new ArrayList<>();
        availableFormatList.add(new DataTablesAvailableFormat());
        availableFormatList.add(new UPlotAvailableFormat());
        final Dataset<Row> emptyDataset = sparkSession.emptyDataFrame();

        final JsonObject defaultOptionJson = Json.createObjectBuilder()
                .add("paragraphId","paragraphId")
                .add("noteId","noteId")
                .add("type", "dataTables")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("draw",0)
                        .add("start",0)
                        .add("length",50)
                        .add("search",Json.createObjectBuilder()
                                .add("value","")
                                .add("regex",false)
                                .add("fixed",Json.createArrayBuilder().build())
                                .build()).build())
                .build();
        final UIOption defaultUIOption = new UIOptionImpl(defaultOptionJson);
        final UserInterfaceManager userInterfaceManager = new UserInterfaceManager(context,emptyDataset,defaultUIOption,availableFormatList);

        // Should be able to format to uPlot
        final JsonObject uPlotOptionJson = Json.createObjectBuilder()
                .add("paragraphId","paragraphId")
                .add("noteId","dataTables")
                .add("type", "uPlot")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("graphType","line")
                        .build())
                .build();
        final UIOption uPlotUIOption = new UIOptionImpl(uPlotOptionJson);
        Assertions.assertDoesNotThrow(()->userInterfaceManager.updateDataset(aggregatedBatch1));

        final String uPlotFormatted = Assertions.assertDoesNotThrow(()->userInterfaceManager.formatDataset(uPlotUIOption));

        final JsonObject expecteduPlotJson = Json.createObjectBuilder()
                .add("data",Json.createArrayBuilder()
                        .add(Json.createArrayBuilder()
                                .add(0)
                                .add(1)
                                .add(2))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(2))
                        .add(Json.createArrayBuilder()
                                .add(1)
                                .add(1)
                                .add(1))
                )
                .add("options",Json.createObjectBuilder()
                        .add("labels",Json.createArrayBuilder()
                                .add("create")
                                .add("update")
                                .add("delete"))
                        .add("series",Json.createArrayBuilder()
                                .add("max(filesModified)")
                                .add("min(filesModified)"))
                        .add("graphType","line")
                        .add("xAxisLabel","operation")
                )
                .add("isAggregated",true)
                .add("type","uPlot")
                .build();
        Assertions.assertEquals(expecteduPlotJson.toString(),uPlotFormatted);


        // Should be able to format to DataTables
        final JsonObject json = Json.createObjectBuilder()
                .add("paragraphId","paragraph_1777976743753_395717996")
                .add("noteId","2MRV3E2UT")
                .add("type", "dataTables")
                .add("requestOptions",Json.createObjectBuilder()
                        .add("draw",1)
                        .add("start",0)
                        .add("length",50)
                        .add("search",Json.createObjectBuilder()
                                .add("value","")
                                .add("regex",false)
                                .add("fixed",Json.createArrayBuilder().build())
                                .build()).build())
                .build();
        final UIOption dataTablesUIOption = new UIOptionImpl(json);
        final String dataTablesFormatted = Assertions.assertDoesNotThrow(()->userInterfaceManager.formatDataset(dataTablesUIOption));

        final JsonObject expectedDataTablesJson = Json.createObjectBuilder()
                .add("data", Json.createObjectBuilder()
                        .add("data", Json.createArrayBuilder()
                                .add(Json.createObjectBuilder()
                                        .add("operation","create")
                                        .add("max(filesModified)",1)
                                        .add("min(filesModified)",1))
                                .add(Json.createObjectBuilder()
                                        .add("operation","update")
                                        .add("max(filesModified)",1)
                                        .add("min(filesModified)",1))
                                .add(Json.createObjectBuilder()
                                        .add("operation","delete")
                                        .add("max(filesModified)",2)
                                        .add("min(filesModified)",1)))
                        .add("draw",1)
                        .add("recordsTotal",3)
                        .add("recordsFiltered",3))
                .add("options", Json.createObjectBuilder()
                        .add("headers",Json.createArrayBuilder()
                                .add("operation")
                                .add("max(filesModified)")
                                .add("min(filesModified)")))
                .add("isAggregated",true)
                .add("type","dataTables")
                .build();

        Assertions.assertEquals(expectedDataTablesJson.toString(),dataTablesFormatted);
    }
    @Test
    void equalsVerifier() {
        final InterpreterContext redInterpreterContext = InterpreterContext.builder().setNoteId("note1").build();
        final InterpreterContext blueInterpreterContext = InterpreterContext.builder().setNoteId("note2").build();
        final JsonObject json = Json.createObjectBuilder()
                .add("key","value").build();
        final DatasetStore redDatasetStore = new DatasetStore(batch1,new ArrayList<>(),redInterpreterContext,new UIOptionImpl(json));
        final DatasetStore blueDatasetStore = new DatasetStore(batch2,new ArrayList<>(),blueInterpreterContext,new UIOptionImpl(json));
        EqualsVerifier.forClass(UserInterfaceManager.class)
                .withPrefabValues(InterpreterContext.class, redInterpreterContext, blueInterpreterContext)
                .withPrefabValues(DatasetStore.class, redDatasetStore, blueDatasetStore)
                .verify();
        }
    }
