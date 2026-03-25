package com.teragrep.zep_01.socket.messages;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParagraphOutputResponseMessageTest {

    @Test
    void asJsonTest() {
        final String noteId = "testNote";
        final String paragraphId = "testParagraph";
        final JsonObject testOutput = Json.createObjectBuilder()
                .add("type","dataTables")
                .add("data",Json.createArrayBuilder()
                        .build())
                .build();
        final ParagraphOutputResponseMessage testMessage = new ParagraphOutputResponseMessage(noteId,paragraphId,testOutput);
        final JsonObject actualJson = testMessage.asJson();
        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("noteId",noteId)
                .add("paragraphId",paragraphId)
                .add("output",testOutput)
                .build();
        Assertions.assertEquals(expectedJson,actualJson);
    }

    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(ParagraphOutputRequestMessage.class).verify();
    }
}