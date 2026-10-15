package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Objects;

public final class ParagraphRuntimeInfos implements Jsonable {
    private final Map<String,ParagraphRuntimeInfo> runtimeInfoMap;

    public ParagraphRuntimeInfos(final Map<String, ParagraphRuntimeInfo> runtimeInfoMap){
        this.runtimeInfoMap = runtimeInfoMap;
    }

    @Override
    public JsonObject asJson() {
        final JsonObjectBuilder runtimeInfos = Json.createObjectBuilder();
        if(runtimeInfoMap != null){
            for (final Map.Entry<String,ParagraphRuntimeInfo> entry : runtimeInfoMap.entrySet()) {
                runtimeInfos.add(entry.getKey(),entry.getValue().asJson());
            }
        }
        return runtimeInfos.build();
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final ParagraphRuntimeInfos that = (ParagraphRuntimeInfos) o;
            equals = Objects.equals(runtimeInfoMap, that.runtimeInfoMap);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(runtimeInfoMap);
    }
}
