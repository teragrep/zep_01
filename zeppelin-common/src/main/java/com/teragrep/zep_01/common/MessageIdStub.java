package com.teragrep.zep_01.common;

import jakarta.json.JsonValue;

import java.util.Objects;

public final class MessageIdStub implements MessageId{

    private final boolean isStub;
    public MessageIdStub(){
        this(true);
    }

    private MessageIdStub(final boolean isStub){
        this.isStub = isStub;
    }

    @Override
    public boolean isStub() {
        return isStub;
    }

    @Override
    public JsonValue asJson() {
        throw new IllegalStateException("Cannot turn a MessageIdStub into JSON!");
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final MessageIdStub that = (MessageIdStub) o;
            equals = isStub == that.isStub;
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isStub);
    }
}
