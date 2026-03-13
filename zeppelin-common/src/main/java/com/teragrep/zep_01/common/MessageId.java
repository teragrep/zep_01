package com.teragrep.zep_01.common;

import jakarta.json.JsonValue;

public interface MessageId extends Stubable {
    public abstract JsonValue asJson();
}
