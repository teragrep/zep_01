package com.teragrep.zep_01.common;

import com.google.gson.Gson;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;



public class ValidatedMessageTest {
    @Test
    public void validMessageTest(){
        Assertions.assertDoesNotThrow(()->{
            Gson gson = new Gson();
            JsonObject testMessageObject = Json.createObjectBuilder()
                    .add("op","")
                    .add("data",Json.createObjectBuilder()
                            .add("noteId","note1")
                            .add("paragraphId","paragraph1")
                            .add("search",Json.createObjectBuilder()
                                    .add("value","searchString")
                                    .add("regex",false)
                                    .build())
                            .add("array",Json.createArrayBuilder()
                                    .add(Json.createObjectBuilder().
                                            add("key1",1)
                                            .add("key2",2)
                                            .build())
                                    .add(Json.createObjectBuilder().
                                            add("key3",1)
                                            .add("key4",2)
                                            .build())
                                    .add(Json.createObjectBuilder().
                                            add("key5",1)
                                            .add("key6",2)
                                            .build())
                                    .build())
                            .build())
                    .build();

            // Use Zeppelin's method of generating a Message object to test.
            Message testMessage = gson.fromJson(testMessageObject.toString(),Message.class);

            // Create a JsonObject that is used to validate the message's parameters
            JsonObject requiredKeys = Json.createObjectBuilder()
                    .add("data",Json.createObjectBuilder()
                            .add("noteId","")
                            .add("search",Json.createObjectBuilder()
                                    .add("value","")
                                    .add("regex",false)
                                    .build())
                            .add("array",Json.createArrayBuilder()
                                    .add(Json.createObjectBuilder()
                                            .add("key1",0)
                                            .build())
                                    .add(Json.createObjectBuilder()
                                            .add("key4",0)
                                            .build())
                                    .build())
                            .build())
                    .build();

            // Add the decorator to the message and validate the message's parameters
            ValidatedMessage validatedMessage = new ValidatedMessage(testMessage,requiredKeys);
            Assertions.assertTrue(validatedMessage.validate());
        });
    }
    @Test
    public void invalidMessageTest() {
        Assertions.assertDoesNotThrow(() -> {
            Gson gson = new Gson();
            JsonObject testMessageObject = Json.createObjectBuilder()
                    .add("op", "PARAGRAPH_UPDATE_OUTPUT")
                    .add("data", Json.createObjectBuilder()
                            .add("noteId", "note1")
                            .add("paragraphId", "paragraph1")
                            .add("search", Json.createObjectBuilder()
                                    .add("value", "searchString")
                                    .add("regex", false)
                                    .build())
                            .add("array", Json.createArrayBuilder()
                                    .add(Json.createObjectBuilder().
                                            add("key1", 1)
                                            .add("key2", 2)
                                            .build())
                                    .add(Json.createObjectBuilder().
                                            add("key3", 1)
                                            .add("key4", 2)
                                            .build())
                                    .build())
                            .add("draw",0)
                            .build())
                    .build();

            // Use Zeppelin's method of generating a Message object to test.
            Message testMessage = gson.fromJson(testMessageObject.toString(), Message.class);

            // Create a JsonObject that is used to validate the message's parameters. This one has a nonexistentKey, and doesn't require a NoteId to be set.
            JsonObject requiredKeys = Json.createObjectBuilder()
                    .add("data", Json.createObjectBuilder()
                            .add("draw",0)
                            .add("search", Json.createObjectBuilder()
                                    .add("value", "")
                                    .add("regex", "")
                                    .build())
                            .add("array", Json.createArrayBuilder()
                                    .add(Json.createObjectBuilder()
                                            .add("key1", "")
                                            .build())
                                    .add(Json.createObjectBuilder()
                                            .add("key4", "")
                                            .build())
                                    .build())
                            .add("nonexistentkey", "")
                            .build())
                    .build();
            // Add the decorator to the message and validate the message's parameters
            ValidatedMessage validatedMessage = new ValidatedMessage(testMessage, requiredKeys);
            Assertions.assertFalse(validatedMessage.validate());
        });
    }

    @Test
    public void validateTest(){
        Gson gson = new Gson();
        JsonObject testMessageObject = Json.createObjectBuilder()
                .add("op", "PARAGRAPH_UPDATE_OUTPUT")
                .add("data", Json.createObjectBuilder()
                        .add("noteId", "note1")
                        .add("paragraphId", "paragraph1")
                        .add("start",0)
                        .add("length",0)
                        .add("search", Json.createObjectBuilder()
                                .add("value", "searchString")
                                .add("regex", false)
                                .build())
                        .add("draw",0)
                        .build())
                .build();

        // Use Zeppelin's method of generating a Message object to test.
        Message testMessage = gson.fromJson(testMessageObject.toString(), Message.class);
        JsonObject requiredKeys = Json.createObjectBuilder()
                .add("op", "PARAGRAPH_UPDATE_OUTPUT")
                .add("data", Json.createObjectBuilder()
                    .add("noteId","")
                    .add("paragraphId","")
                    .add("start","")
                    .add("length","")
                    .add("search",Json.createObjectBuilder()
                            .add("value","")
                            .build())
                    .add("draw","")
                    .build())
                .build();
        ValidatedMessage validatedMessage = new ValidatedMessage(testMessage,requiredKeys);
        Assertions.assertTrue(validatedMessage.validate());
    }
}