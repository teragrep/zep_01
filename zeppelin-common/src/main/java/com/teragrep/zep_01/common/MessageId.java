package com.teragrep.zep_01.common;

import com.teragrep.stb_01.Stubable;
import jakarta.json.JsonValue;

public interface MessageId extends Stubable {
    public abstract JsonValue asJson();
}
