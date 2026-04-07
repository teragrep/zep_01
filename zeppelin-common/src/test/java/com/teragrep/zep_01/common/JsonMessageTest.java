package com.teragrep.zep_01.common;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonMessageTest {

    @Test
    public void asJsonTest() {
        final Message.OP op = Message.OP.ERROR_INFO;
        final JsonObject json = Json.createObjectBuilder().add("message","Something went wrong").build();
        final JsonMessage message = new JsonMessage(op,json);

        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("op","ERROR_INFO")
                .add("data",json)
                .add("ticket","anonymous")
                .add("principal","anonymous")
                .add("roles","")
                .build();
        Assertions.assertEquals(expectedJson, message.asJson());
    }

    @Test
    public void withMessageIdTest() {
        final Message.OP op = Message.OP.ERROR_INFO;
        final JsonObject json = Json.createObjectBuilder().add("message","Something went wrong").build();
        final String messageId = "testId";
        final SimpleMessageId id = new SimpleMessageId(messageId);
        final JsonMessage message = new JsonMessage(id,op,json);

        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("op","ERROR_INFO")
                .add("data",json)
                .add("ticket","anonymous")
                .add("principal","anonymous")
                .add("roles","")
                .add("msgId",messageId)
                .build();
        Assertions.assertEquals(expectedJson, message.asJson());
    }

    @Test
    public void jsonableAsJsonTest() {
        final Message.OP op = Message.OP.ERROR_INFO;
        final Jsonable jsonable = new JsonMessage(Message.OP.PARAGRAPH,Json.createObjectBuilder().build());
        final JsonMessage message = new JsonMessage(op,jsonable);

        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("op","ERROR_INFO")
                .add("data",jsonable.asJson())
                .add("ticket","anonymous")
                .add("principal","anonymous")
                .add("roles","")
                .build();
        Assertions.assertEquals(expectedJson, message.asJson());
    }

    @Test
    public void jsonableWithMessageIdTest() {
        final Message.OP op = Message.OP.ERROR_INFO;
        final Jsonable jsonable = new JsonMessage(Message.OP.PARAGRAPH,Json.createObjectBuilder().build());
                final String messageId = "testId";
        final SimpleMessageId id = new SimpleMessageId(messageId);
        final JsonMessage message = new JsonMessage(id,op,jsonable);

        final JsonObject expectedJson = Json.createObjectBuilder()
                .add("op","ERROR_INFO")
                .add("data",jsonable.asJson())
                .add("ticket","anonymous")
                .add("principal","anonymous")
                .add("roles","")
                .add("msgId",messageId)
                .build();
        Assertions.assertEquals(expectedJson, message.asJson());
    }
    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(JsonMessage.class).verify();
    }
}