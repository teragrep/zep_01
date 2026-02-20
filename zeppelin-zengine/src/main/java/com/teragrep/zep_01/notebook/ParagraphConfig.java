package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;

public class ParagraphConfig implements Jsonable {
    private final Map<String, Object> configMap;
    public ParagraphConfig(Map<String, Object> configMap){
        this.configMap = configMap;
    }
    @Override
    public JsonObject asJson() {
        JsonObjectBuilder configJson = Json.createObjectBuilder();
        if(configMap.containsKey("colWidth") && configMap.get("colWidth") instanceof Number){
            long columnWidth = ((Number) configMap.get("colWidth")).longValue();
            configJson.add("colWidth",columnWidth);
        }
        if(configMap.containsKey("editorMode") && configMap.get("editorMode") instanceof String){
            String editorMode = configMap.get("editorMode").toString();
            configJson.add("editorMode",editorMode);
        }
        if(configMap.containsKey("enabled") && configMap.get("enabled") instanceof Boolean){
            boolean enabled = (Boolean) configMap.get("enabled");
            configJson.add("enabled",enabled);
        }
        if(configMap.containsKey("fontSize") && configMap.get("fontSize") instanceof Number){
            long fontSize = ((Number) configMap.get("fontSize")).longValue();
            configJson.add("fontSize",fontSize);
        }
        if(configMap.containsKey("lineNumbers") && configMap.get("lineNumbers") instanceof Boolean){
            boolean lineNumbers = (Boolean) configMap.get("lineNumbers");
            configJson.add("lineNumbers",lineNumbers);
        }
        if(configMap.containsKey("editorSetting") && configMap.get("editorSetting") instanceof Map){
            Map<String,Object> editorSettingMap = (Map<String, Object>) configMap.get("editorSetting");
            EditorSetting editorSetting = new EditorSetting(editorSettingMap);
            configJson.add("editorSetting",editorSetting.asJson());
        }
        return configJson.build();
    }
}
