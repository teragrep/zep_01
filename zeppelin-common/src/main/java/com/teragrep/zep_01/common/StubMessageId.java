package com.teragrep.zep_01.common;

import jakarta.json.JsonValue;

public class StubMessageId implements MessageId{
    @Override
    public boolean isStub() {
        return true;
    }

    @Override
    public JsonValue asJson() {
        throw new IllegalStateException("Cannot turn a StubMessageId into JSON!");
    }
}
