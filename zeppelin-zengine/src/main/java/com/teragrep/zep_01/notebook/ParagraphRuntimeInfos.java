package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Objects;

public final class ParagraphRuntimeInfos implements Jsonable {
    private final Map<String,ParagraphRuntimeInfo> runtimeInfoMap;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParagraphRuntimeInfos that = (ParagraphRuntimeInfos) o;
        return Objects.equals(runtimeInfoMap, that.runtimeInfoMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runtimeInfoMap);
    }

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
}
