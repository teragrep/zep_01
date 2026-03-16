package com.teragrep.zep_01.common;

import jakarta.json.Json;
import jakarta.json.JsonValue;

import java.util.Objects;

public class SimpleMessageId implements MessageId{

    private final String id;
    public SimpleMessageId(final String id){
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleMessageId that = (SimpleMessageId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
