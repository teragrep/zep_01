package com.teragrep.zep_01.socket.messages;

import jakarta.json.Json;
import jakarta.json.JsonException;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParagraphOutputRequestMessageTest {

    @Test
    public void uPlotOutputRequestTest(){
        final String msgId = "testMsgId";
        final String noteId = "testNoteId";
        final String paragraphId = "testParaId";
        final String type = "uPlot";

        final String graphType = "bar";
        final JsonObject messageJson = Json.createObjectBuilder()
                .add("msgId",msgId)
                .add("data",Json.createObjectBuilder()
                        .add("noteId",noteId)
                        .add("paragraphId",paragraphId)
                        .add("type",type)
                        .add("requestOptions",Json.createObjectBuilder()
                                .add("graphType",graphType)
                                .build())
                        .build())
                .build();
        final ParagraphOutputRequestMessage message = new ParagraphOutputRequestMessage(messageJson);
        // Json is valid, so no method should throw an Exception.
        Assertions.assertEquals(msgId, message.messageId());
        Assertions.assertEquals(noteId, message.noteId());
        Assertions.assertEquals(paragraphId, message.paragraphId());
        Assertions.assertEquals(type, message.type());
        final String data = Assertions.assertDoesNotThrow(()->message.data());
        Assertions.assertTrue(data.contains(graphType));
    }

    @Test
    public void dataTablesOutputRequestTest(){
        final String msgId = "testMsgId";
        final String noteId = "testNoteId";
        final String paragraphId = "testParaId";
        final String type = "dataTables";

        final String searchString = "";

        //fields for options
        final int draw = 0;
        final int start = 0;
        final int length = 25;

        final JsonObject messageJson = Json.createObjectBuilder()
                .add("msgId",msgId)
                .add("data",Json.createObjectBuilder()
                    .add("noteId",noteId)
                    .add("paragraphId",paragraphId)
                    .add("type",type)
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
                        .build())
                .build();
        final ParagraphOutputRequestMessage message = new ParagraphOutputRequestMessage(messageJson);
        // Json is valid, so no method should throw an Exception.
        Assertions.assertEquals(msgId, message.messageId());
        Assertions.assertEquals(noteId, message.noteId());
        Assertions.assertEquals(paragraphId, message.paragraphId());
        Assertions.assertEquals(type, message.type());
        final String data = Assertions.assertDoesNotThrow(()->message.data());
        Assertions.assertTrue(data.contains(Integer.toString(draw)));
        Assertions.assertTrue(data.contains(Integer.toString(start)));
        Assertions.assertTrue(data.contains(Integer.toString(length)));
    }

    @Test
    public void invalidRequestMessageTest(){

        // Given JsonObject does not contain any of the required fields.
        final JsonObject messageJson = Json.createObjectBuilder()
                .build();
        final ParagraphOutputRequestMessage message = new ParagraphOutputRequestMessage(messageJson);
        // Should throw an error when trying to retrieve any of the parameters
        Assertions.assertThrows(JsonException.class,()-> message.paragraphId());
        Assertions.assertThrows(JsonException.class,()-> message.messageId());
        Assertions.assertThrows(JsonException.class,()-> message.noteId());
        Assertions.assertThrows(JsonException.class,()-> message.data());
        Assertions.assertThrows(JsonException.class,()-> message.type());
    }
    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(ParagraphOutputRequestMessage.class).verify();
    }
}