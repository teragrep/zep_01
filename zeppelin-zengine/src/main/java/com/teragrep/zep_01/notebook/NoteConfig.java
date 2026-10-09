package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Objects;

public final class NoteConfig implements Jsonable {
    private final Map<String,Object> configMap;

    public NoteConfig(final Map<String,Object> configMap){
        this.configMap = configMap;
    }
    @Override
    public JsonObject asJson() {
        final JsonObjectBuilder configJson = Json.createObjectBuilder();
        if(configMap.containsKey("bodyClassName")){
            configJson.add("bodyClassName",configMap.get("bodyClassName").toString());
        }
        if(configMap.containsKey("cronInput")){
            configJson.add("cronInput",configMap.get("cronInput").toString());
        }
        if(configMap.containsKey("isZeppelinNotebookCronEnable") && configMap.get("isZeppelinNotebookCronEnable") instanceof Boolean){
            configJson.add("isZeppelinNotebookCronEnable",(Boolean) configMap.get("isZeppelinNotebookCronEnable"));
        }
        if(configMap.containsKey("looknfeel")){
            configJson.add("looknfeel",configMap.get("looknfeel").toString());
        }
        if(configMap.containsKey("personalizedMode")){
            configJson.add("personalizedMode",configMap.get("personalizedMode").toString());
        }
        return configJson.build();
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final NoteConfig that = (NoteConfig) o;
            equals = Objects.equals(configMap, that.configMap);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(configMap);
    }
}
