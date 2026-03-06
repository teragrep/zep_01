package com.teragrep.zep_01.common;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonMessageTest {

    @Test
    public void asJsonTest() {
        Message.OP op = Message.OP.ERROR_INFO;
        JsonObject json = Json.createObjectBuilder().add("message","Something went wrong").build();
        JsonMessage message = new JsonMessage(op,json);

        JsonObject expectedJson = Json.createObjectBuilder()
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
        Message.OP op = Message.OP.ERROR_INFO;
        JsonObject json = Json.createObjectBuilder().add("message","Something went wrong").build();
        String messageId = "testId";
        SimpleMessageId id = new SimpleMessageId(messageId);
        JsonMessage message = new JsonMessage(id,op,json);

        JsonObject expectedJson = Json.createObjectBuilder()
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
        Message.OP op = Message.OP.ERROR_INFO;
        Jsonable jsonable = new JsonMessage(Message.OP.PARAGRAPH,Json.createObjectBuilder().build());
        JsonMessage message = new JsonMessage(op,jsonable);

        JsonObject expectedJson = Json.createObjectBuilder()
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
        Message.OP op = Message.OP.ERROR_INFO;
        Jsonable jsonable = new JsonMessage(Message.OP.PARAGRAPH,Json.createObjectBuilder().build());
                String messageId = "testId";
        SimpleMessageId id = new SimpleMessageId(messageId);
        JsonMessage message = new JsonMessage(id,op,jsonable);

        JsonObject expectedJson = Json.createObjectBuilder()
                .add("op","ERROR_INFO")
                .add("data",jsonable.asJson())
                .add("ticket","anonymous")
                .add("principal","anonymous")
                .add("roles","")
                .add("msgId",messageId)
                .build();
        Assertions.assertEquals(expectedJson, message.asJson());
    }
}