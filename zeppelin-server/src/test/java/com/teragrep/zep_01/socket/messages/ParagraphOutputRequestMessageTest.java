package com.teragrep.zep_01.socket.messages;

import com.teragrep.zep_01.interpreter.thrift.DataTablesOptions;
import com.teragrep.zep_01.interpreter.thrift.UPlotOptions;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParagraphOutputRequestMessageTest {

    @Test
    public void uPlotOutputRequestTest(){
        String msgId = "testMsgId";
        String noteId = "testNoteId";
        String paragraphId = "testParaId";
        String visualizationLibraryName = "uPlot";

        String graphType = "bar";
        JsonObject messageJson = Json.createObjectBuilder()
                .add("msgId",msgId)
                .add("noteId",noteId)
                .add("paragraphId",paragraphId)
                .add("type",visualizationLibraryName)
                .add("requestOptions",Json.createObjectBuilder()
                        .add("graphType",graphType)
                        .build())
                .build();
        ParagraphOutputRequestMessage message = new ParagraphOutputRequestMessage(messageJson);
        // Json is valid, so no method should throw an Exception.
        Assertions.assertEquals(msgId, message.messageId());
        Assertions.assertEquals(noteId, message.noteId());
        Assertions.assertEquals(paragraphId, message.paragraphId());
        Assertions.assertEquals(visualizationLibraryName, message.visualizationLibraryName());
        UPlotOptions options = Assertions.assertDoesNotThrow(()->message.options().getUPlotOptions());
        Assertions.assertEquals(graphType,options.getGraphType());
    }

    @Test
    public void dataTablesOutputRequestTest(){
        String msgId = "testMsgId";
        String noteId = "testNoteId";
        String paragraphId = "testParaId";
        String visualizationLibraryName = "dataTables";

        String searchString = "";

        //fields for options
        int draw = 0;
        int start = 0;
        int length = 25;

        JsonObject messageJson = Json.createObjectBuilder()
                .add("msgId",msgId)
                .add("noteId",noteId)
                .add("paragraphId",paragraphId)
                .add("type",visualizationLibraryName)
                .add("requestOptions",Json.createObjectBuilder()
                        .add("draw",draw)
                        .add("start",start)
                        .add("length",length)
                        .add("order",Json.createArrayBuilder()
                                .build())
                        .add("columns",Json.createArrayBuilder()
                                .add(Json.createObjectBuilder()
                                        .add("data","_time")
                                        .add("name","")
                                        .add("searchable",true)
                                        .add("orderable",false)
                                        .add("search",Json.createObjectBuilder()
                                                .add("value","")
                                                .add("regex",false)
                                                .add("fixed",Json.createArrayBuilder()
                                                        .build())
                                                .build())
                                        .build())
                                .add(Json.createObjectBuilder()
                                        .add("data","operation")
                                        .add("name","")
                                        .add("searchable",true)
                                        .add("orderable",false)
                                        .add("search",Json.createObjectBuilder()
                                                .add("value","")
                                                .add("regex",false)
                                                .add("fixed",Json.createArrayBuilder()
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .add("search",Json.createObjectBuilder()
                                .add("value",searchString)
                                .add("regex",false)
                                .add("fixed",Json.createArrayBuilder()
                                        .build())
                                .build())
                        .build())
                .build();
        ParagraphOutputRequestMessage message = new ParagraphOutputRequestMessage(messageJson);
        // Json is valid, so no method should throw an Exception.
        Assertions.assertEquals(msgId, message.messageId());
        Assertions.assertEquals(noteId, message.noteId());
        Assertions.assertEquals(paragraphId, message.paragraphId());
        Assertions.assertEquals(visualizationLibraryName, message.visualizationLibraryName());
        DataTablesOptions options = Assertions.assertDoesNotThrow(()->message.options().getDataTablesOptions());
        Assertions.assertEquals(draw,options.getDraw());
        Assertions.assertEquals(start,options.getStart());
        Assertions.assertEquals(length,options.getLength());
        Assertions.assertEquals(searchString,options.getSearch().getValue());
    }
}