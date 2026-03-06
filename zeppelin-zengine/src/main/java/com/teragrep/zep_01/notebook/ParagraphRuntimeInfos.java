package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;

public class ParagraphRuntimeInfos implements Jsonable {
    private Map<String,ParagraphRuntimeInfo> runtimeInfoMap;
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
