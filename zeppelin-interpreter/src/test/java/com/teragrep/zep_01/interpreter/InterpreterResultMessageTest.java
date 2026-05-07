package com.teragrep.zep_01.interpreter;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InterpreterResultMessageTest {
    /**
     * If the data is in DataTables format, it should contain all the expected keys and presented as a JSON object
     */
    @Test
    void testDataTablesResultAsJson() {

        final String resultJsonString = "{\"type\":\"dataTables\",\"options\":{\"headers\":[\"_time\",\"operation\",\"count\"]},\"data\":{\"data\":[{\"_time\":\"2021-02-24T02:00:00.000+02:00\",\"operation\":\"create\",\"count\":722},{\"_time\":\"2021-03-26T02:00:00.000+02:00\",\"operation\":\"create\",\"count\":693},{\"_time\":\"2021-03-27T02:00:00.000+02:00\",\"operation\":\"create\",\"count\":673}],\"draw\":1,\"recordsTotal\":10,\"recordsFiltered\":3},\"isAggregated\":false}";
        InterpreterResultMessage testMessage = new InterpreterResultMessage(InterpreterResult.Type.DATATABLES,resultJsonString);

        final JsonObject resultJson = testMessage.asJson();
        Assertions.assertEquals(false,resultJson.getBoolean("isAggregated"));
        Assertions.assertEquals(InterpreterResult.Type.DATATABLES.label,resultJson.getString("type"));
        final JsonObject expectedOptions = Json.createObjectBuilder()
                .add("headers",Json.createArrayBuilder()
                        .add("_time")
                        .add("operation")
                        .add("count")
                        .build())
                .build();

        Assertions.assertEquals(expectedOptions,resultJson.getJsonObject("options"));
        final JsonArray expectedDataArray = Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                        .add("_time","2021-02-24T02:00:00.000+02:00")
                        .add("operation","create")
                        .add("count",722)
                        .build())
                .add(Json.createObjectBuilder()
                        .add("_time","2021-03-26T02:00:00.000+02:00")
                        .add("operation","create")
                        .add("count",693)
                        .build())
                .add(Json.createObjectBuilder()
                        .add("_time","2021-03-27T02:00:00.000+02:00")
                        .add("operation","create")
                        .add("count",673)
                        .build())
                .build();
        final JsonObject expectedData = Json.createObjectBuilder()
                .add("recordsTotal",10)
                .add("recordsFiltered",3)
                .add("draw",1)
                .add("data",expectedDataArray)
                .build();
        Assertions.assertEquals(expectedData,resultJson.getJsonObject("data"));
    }

    /**
     * If the data is in uPlot format, it should contain all the expected keys and presented as a JSON object
     */
    @Test
    void testUPlotResultAsJson() {

        final String resultJsonString = "{\"type\":\"uPlot\",\"options\":{\"labels\":[\"minTemperature\",\"maxTemperature\"],\"series\":[\"bedroom\",\"kitchen\",\"balcony\"]},\"data\":[[0,1,2],[[18,25],[19,28],[10,22]]],\"isAggregated\":true}";
        InterpreterResultMessage testMessage = new InterpreterResultMessage(InterpreterResult.Type.UPLOT,resultJsonString);

        final JsonObject resultJson = testMessage.asJson();
        Assertions.assertEquals(true,resultJson.getBoolean("isAggregated"));
        Assertions.assertEquals(InterpreterResult.Type.UPLOT.label,resultJson.getString("type"));
        final JsonObject expectedOptions = Json.createObjectBuilder()
                .add("labels",Json.createArrayBuilder()
                        .add("minTemperature")
                        .add("maxTemperature")
                        .build())
                .add("series",Json.createArrayBuilder()
                        .add("bedroom")
                        .add("kitchen")
                        .add("balcony")
                        .build())
                .build();

        Assertions.assertEquals(expectedOptions,resultJson.getJsonObject("options"));
        final JsonArray expectedDataArray = Json.createArrayBuilder()
                .add(Json.createArrayBuilder()
                        .add(0)
                        .add(1)
                        .add(2))
                .add(Json.createArrayBuilder()
                        .add(Json.createArrayBuilder()
                                .add(18)
                                .add(25))
                        .add(Json.createArrayBuilder()
                                .add(19)
                                .add(28))
                        .add(Json.createArrayBuilder()
                                .add(10)
                                .add(22)))
                .build();
        Assertions.assertEquals(expectedDataArray,resultJson.getJsonArray("data"));
    }

    /**
     * If the data is in a plain text format, it should be returned as a simple string. "type" and "isAggregated" keys should be present too.
     */
    @Test
    void testPlaintextResultAsJson() {
        final String resultJsonString = "plain text result";
        InterpreterResultMessage testMessage = new InterpreterResultMessage(InterpreterResult.Type.TEXT,resultJsonString);
        final JsonObject resultJson = testMessage.asJson();
        Assertions.assertEquals(false,resultJson.getBoolean("isAggregated"));
        Assertions.assertEquals(InterpreterResult.Type.TEXT.label,resultJson.getString("type"));
        Assertions.assertEquals(resultJsonString,resultJson.getString("data"));
    }

    /**
     * Some data might be in a plain text format, but it could contain valid JSON. In this case the data should be sent as a String.
     */
    @Test
    void testPlainJSONtextResultAsJson() {
        final String resultJsonString = "{\"data\":\"plain text result\"}";
        InterpreterResultMessage testMessage = new InterpreterResultMessage(InterpreterResult.Type.TEXT,resultJsonString);
        final JsonObject resultJson = testMessage.asJson();
        Assertions.assertEquals(false,resultJson.getBoolean("isAggregated"));
        Assertions.assertEquals(InterpreterResult.Type.TEXT.label,resultJson.getString("type"));
        Assertions.assertEquals(resultJsonString,resultJson.getString("data"));
    }
}