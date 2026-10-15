package com.teragrep.zep_01.common;

import jakarta.json.Json;
import jakarta.json.JsonValue;

import java.util.Objects;

public final class MessageIdImpl implements MessageId{

    private final String id;
    public MessageIdImpl(final String id){
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
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final MessageIdImpl messageId = (MessageIdImpl) o;
            equals = Objects.equals(id, messageId.id);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
