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

import com.teragrep.zep_01.display.AngularObjectRegistry;
import com.teragrep.zep_01.interpreter.*;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DTTableDatasetNgTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DTTableDatasetNgTest.class);
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
    private final Dataset<Row> testDs = testDataset.createDataset(49,Timestamp.from(Instant.ofEpochSecond(0)),0L,"data data","index_A","stream","host","input",String.valueOf(0),0L,"test data");


    private final StructType smallTestSchema = new StructType(
            new StructField[] {
                    new StructField("_time", DataTypes.TimestampType, false, new MetadataBuilder().build()),
                    new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
                    new StructField("_raw", DataTypes.StringType, false, new MetadataBuilder().build()),
            }
    );
    private final TestDPLData smallTestDataset = new TestDPLData(sparkSession, smallTestSchema);
    private final Dataset<Row> smallTestDs = smallTestDataset.createDataset(49,Timestamp.from(Instant.ofEpochSecond(0)),0L,"data data");

    @Test
    public void testAJAXResponse() {
        List<String> datasetAsJSON = testDs.toJSON().collectAsList();

        List<String> subList = datasetAsJSON.subList(0, 5);

        JsonArray formated = DTTableDatasetNg.dataStreamParser(subList);

        DTHeader dtHeader = new DTHeader(testSchema);
        JsonArray headers = dtHeader.json();
        JsonObject response = DTTableDatasetNg.DTNetResponse(formated, headers, 1, datasetAsJSON.size(),formated.size());

        ArrayList<String> timestamps = new ArrayList<>();
        timestamps.add("1970-01-01T00:00:49.000Z");
        timestamps.add("1970-01-01T00:00:48.000Z");
        timestamps.add("1970-01-01T00:00:47.000Z");
        timestamps.add("1970-01-01T00:00:46.000Z");
        timestamps.add("1970-01-01T00:00:45.000Z");

        JsonArrayBuilder dataBuilder = Json.createArrayBuilder();
        for (String timestamp:timestamps
             ) {
            JsonObject rowJson = Json.createObjectBuilder()
                    .add("_time",timestamp)
                    .add("id",0)
                    .add("_raw","data data")
                    .add("index","index_A")
                    .add("sourcetype","stream")
                    .add("host","host")
                    .add("source","input")
                    .add("partition","0")
                    .add("offset",0)
                    .add("origin","test data")
                    .build();
            dataBuilder.add(rowJson);
        }
        JsonArray data = dataBuilder.build();

        // Ensure that data field has the correct number of rows.
        Assertions.assertEquals(5,data.size());

        JsonObject expectedJson = Json.createObjectBuilder()
                .add("headers",headers)
                .add("data", data)
                .add("draw",1)
                .add("recordsTotal",49)
                .add("recordsFiltered",5)
                .build();

        assertEquals(expectedJson.toString()
                , response.toString()
        );
    }

    @Test
    public void testPagination(){
        // Boilerplate to create an InterpreterContext
        TestInterpreterOutputListener listener = new TestInterpreterOutputListener();
        InterpreterOutput testOutput =  new InterpreterOutput(listener);
        AngularObjectRegistry testRegistry = new AngularObjectRegistry("test", null);
        InterpreterContext context = InterpreterContext.builder().setInterpreterOut(testOutput).setAngularObjectRegistry(testRegistry).build();

        DTTableDatasetNg dtTableDatasetNg = new DTTableDatasetNg(context);

        // Simulate DPL receiving new data.
        Assertions.assertDoesNotThrow(()->{
            dtTableDatasetNg.setParagraphDataset(testDs);
        });

        // Get first 5 rows of the dataset, check values of first and last field
        JsonObject page1 = Assertions.assertDoesNotThrow(()->dtTableDatasetNg.SearchAndPaginate(0,0,5,""));
        Assertions.assertEquals(5,page1.getJsonArray("data").size());
        Assertions.assertEquals("1970-01-01T00:00:49.000Z",page1.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("1970-01-01T00:00:45.000Z",page1.getJsonArray("data").getJsonObject(4).getString("_time"));


        // Get rows 6-15 of the dataset, check values of first and last field
        JsonObject page2 = Assertions.assertDoesNotThrow(()->dtTableDatasetNg.SearchAndPaginate(0,5,10,""));
        Assertions.assertEquals(10,page2.getJsonArray("data").size());
        Assertions.assertEquals("1970-01-01T00:00:44.000Z",page2.getJsonArray("data").getJsonObject(0).getString("_time"));
        Assertions.assertEquals("1970-01-01T00:00:35.000Z",page2.getJsonArray("data").getJsonObject(9).getString("_time"));
    }

    // DTTableDatasetNG should clear the output of a Paragraph related to an InterpreterOutput when new data is received from DPL.
    // Does not include a concrete implementation of InterpreterOutputListener as it's an anonymous class within RemoteInterpreterServer, and instantiating it would require too many dependencies.
    @Test
    public void testClearParagraphResultsOnNewDataset(){

        TestInterpreterOutputListener listener = new TestInterpreterOutputListener();
        InterpreterOutput testOutput =  new InterpreterOutput(listener);

        AngularObjectRegistry testRegistry = new AngularObjectRegistry("test", null);
        InterpreterContext context = InterpreterContext.builder().setInterpreterOut(testOutput).setAngularObjectRegistry(testRegistry).build();
        DTTableDatasetNg dtTableDatasetNg = new DTTableDatasetNg(context);

        // Simulate DPL receiving new data.
        Assertions.assertDoesNotThrow(()->{
            dtTableDatasetNg.setParagraphDataset(testDs);
        });
        Assertions.assertEquals(1,listener.numberOfUpdateCalls());
        Assertions.assertEquals(1,listener.numberOfResetCalls());
    }

    /**
     * An incremented 'draw' value should be sent to the UI each time a new batch of data is received.
     * The 'draw' value should be reset to 1 every time the schema of the data changes.
     */
    @Test
    public void testIncrementDraw(){
        TestInterpreterOutputListener listener = new TestInterpreterOutputListener();
        InterpreterOutput testOutput =  new InterpreterOutput(listener);

        AngularObjectRegistry testRegistry = new AngularObjectRegistry("test", null);
        InterpreterContext context = InterpreterContext.builder().setInterpreterOut(testOutput).setAngularObjectRegistry(testRegistry).build();
        DTTableDatasetNg dtTableDatasetNg = new DTTableDatasetNg(context);

        // Simulate DPL receiving new data.
        Assertions.assertDoesNotThrow(()->{
            dtTableDatasetNg.setParagraphDataset(testDs);
        });
        List<InterpreterResultMessage> messages = Assertions.assertDoesNotThrow(()->testOutput.toInterpreterResultMessage());
        // First message should have draw value of 1
        Assertions.assertTrue(messages.get(0).getData().contains("\"draw\":1"));

        // Simulate DPL receiving another batch of new data without changing schema.
        Assertions.assertDoesNotThrow(()->{
            dtTableDatasetNg.setParagraphDataset(testDs);
        });
        List<InterpreterResultMessage> messages2 = Assertions.assertDoesNotThrow(()->testOutput.toInterpreterResultMessage());
        // Second message should have draw value of 2
        Assertions.assertTrue(messages2.get(0).getData().contains("\"draw\":2"));

        // Simulate DPL receiving yet another batch of new data but with a changed schema.
        Assertions.assertDoesNotThrow(()->{
            dtTableDatasetNg.setParagraphDataset(smallTestDs);
        });
        List<InterpreterResultMessage> messages3 = Assertions.assertDoesNotThrow(()->testOutput.toInterpreterResultMessage());
        // Third message's draw value should be reset to 1
        Assertions.assertTrue(messages3.get(0).getData().contains("\"draw\":1"));
    }

    private class TestInterpreterOutputListener implements InterpreterOutputListener{
        private int numberOfResetCalls = 0;
        private int numberOfUpdateCalls = 0;
        @Override
        public void onUpdateAll(InterpreterOutput out) {
            numberOfResetCalls++;
            // Calling this clears the paragraph's results. It will be called when we update the dataset, but should not be called upon pagination request.
        }
        @Override
        public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
            // Calling this does not clear the paragraph's results.
        }
        @Override
        public void onUpdate(int index, InterpreterResultMessageOutput out) {
            // Calling this does not clear the paragraph's results.
            numberOfUpdateCalls++;
        }

        public int numberOfUpdateCalls(){
            return numberOfUpdateCalls;
        }
        public int numberOfResetCalls(){
            return numberOfResetCalls;
        }
    }

    private class TestDPLData {
        private final SparkSession sparkSession;
        private final StructType schema;

        public TestDPLData(SparkSession sparkSession, StructType schema){
            this.sparkSession = sparkSession;
            this.schema = schema;
        }

        /**
         * Tries to generate a dataset of size 'amount' with default values corresponding to varargs 'values'
         * @param amount - desired number of rows in the dataset
         * @param values - varargs specifying the default values to fill into the dataset.
         * @return
         */

        public Dataset<Row> createDataset(int amount, Object ... values){
            final List<Row> rows = rowList(amount,values);
            return sparkSession.createDataFrame(rows, schema);
        }

        /**
         * Generates a List of Rows based on given values.
         * @param amount - Number of rows to generate
         * @param values - Default values to add for each Row. If first value given is a Timestamp, it will be incremented by one for each Row.
         * @return
         */
        private List<Row> rowList(int amount, Object ... values){
            final List<Object> valueList = Arrays.asList(values);
            final ArrayList<Row> rowArrayList = new ArrayList<>();
            while (amount > 0) {
                if(valueList.get(0) instanceof Timestamp){
                    valueList.set(0,Timestamp.from(Instant.ofEpochSecond(amount)));
                }
                final Row row = RowFactory.create(valueList.toArray());
                rowArrayList.add(row);
                amount--;
            }
            return rowArrayList;
        }
    }
}
