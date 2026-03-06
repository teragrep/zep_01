package com.teragrep.zep_01.common;

import jakarta.json.Json;
import jakarta.json.JsonValue;

public class SimpleMessageId implements MessageId{

    private final String id;
    public SimpleMessageId(String id){
        this.id = id;
    }
    @Override
    public JsonValue asJson() {
        return Json.createValue(id);
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
