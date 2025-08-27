package com.teragrep.zep_01.common;

import com.google.gson.Gson;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


public class ValidatedMessageTest {
    @Test
    public void validMessageTest(){
        Assertions.assertDoesNotThrow(()->{
            Gson gson = new Gson();
            JsonObject testMessageObject = Json.createObjectBuilder()
                    .add("op",Message.OP.PARAGRAPH_UPDATE_RESULT.toString())
                    .add("data",Json.createObjectBuilder()
                            .add("noteId","note1")
                            .add("paragraphId","paragraph1")
                            .add("start",0)
                            .add("length",25)
                            .add("search",Json.createObjectBuilder()
                                    .add("value","searchString")
                                    .add("regex",false)
                                    .build())
                            .add("draw",0)
                            .build())
                    .build();

            // Use Zeppelin's method of generating a Message object to test.
            Message testMessage = gson.fromJson(testMessageObject.toString(),Message.class);

            // Add the decorator to the message and validate the message's parameters
            ValidatedMessage validatedMessage = new ValidatedMessage(testMessage);
            Assertions.assertTrue(validatedMessage.isValid());
        });
    }
    @Test
    public void invalidMessageTest() {
        Assertions.assertDoesNotThrow(() -> {
            Gson gson = new Gson();
            // Message is missing search.value field
            JsonObject testMessageObject = Json.createObjectBuilder()
                    .add("op", "PARAGRAPH_UPDATE_OUTPUT")
                    .add("data", Json.createObjectBuilder()
                            .add("start",0)
                            .add("length",25)
                            .add("noteId", "note1")
                            .add("paragraphId", "paragraph1")
                            .add("search", Json.createObjectBuilder()
                                    .add("regex", false)
                                    .build())
                            .add("draw",0)
                            .build())
                    .build();

            // Use Zeppelin's method of generating a Message object to test.
            Message testMessage = gson.fromJson(testMessageObject.toString(), Message.class);

            // Add the decorator to the message and validate the message's parameters
            ValidatedMessage validatedMessage = new ValidatedMessage(testMessage);
            Assertions.assertFalse(validatedMessage.isValid());
        });
    }
    @Test
    public void testContract() {
        EqualsVerifier.forClass(ValidatedMessage.class).verify();
    }
}