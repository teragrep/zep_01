package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Objects;

public final class ParagraphConfig implements Jsonable {
    private final Map<String, Object> configMap;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParagraphConfig config = (ParagraphConfig) o;
        return Objects.equals(configMap, config.configMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configMap);
    }

    public ParagraphConfig(final Map<String, Object> configMap){
        this.configMap = configMap;
    }
    @Override
    public JsonObject asJson() {
        final JsonObjectBuilder configJson = Json.createObjectBuilder();
        if(configMap.containsKey("colWidth") && configMap.get("colWidth") instanceof Number){
            final long columnWidth = ((Number) configMap.get("colWidth")).longValue();
            configJson.add("colWidth",columnWidth);
        }
        if(configMap.containsKey("editorMode") && configMap.get("editorMode") instanceof String){
            final String editorMode = configMap.get("editorMode").toString();
            configJson.add("editorMode",editorMode);
        }
        if(configMap.containsKey("enabled") && configMap.get("enabled") instanceof Boolean){
            final boolean enabled = (Boolean) configMap.get("enabled");
            configJson.add("enabled",enabled);
        }
        if(configMap.containsKey("fontSize") && configMap.get("fontSize") instanceof Number){
            final long fontSize = ((Number) configMap.get("fontSize")).longValue();
            configJson.add("fontSize",fontSize);
        }
        if(configMap.containsKey("lineNumbers") && configMap.get("lineNumbers") instanceof Boolean){
            final boolean lineNumbers = (Boolean) configMap.get("lineNumbers");
            configJson.add("lineNumbers",lineNumbers);
        }
        if(configMap.containsKey("editorSetting") && configMap.get("editorSetting") instanceof Map){
            final Map<String,Object> editorSettingMap = (Map<String, Object>) configMap.get("editorSetting");
            final EditorSetting editorSetting = new EditorSetting(editorSettingMap);
            configJson.add("editorSetting",editorSetting.asJson());
        }
        return configJson.build();
    }
}
