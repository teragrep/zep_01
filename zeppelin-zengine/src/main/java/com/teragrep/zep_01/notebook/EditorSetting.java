package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;

public class EditorSetting implements Jsonable {
    private final Map<String, Object> settingMap;
    public EditorSetting(Map<String, Object> settingMap){
        this.settingMap = settingMap;
    }
    @Override
    public JsonObject asJson() {
        JsonObjectBuilder editorSettingJson = Json.createObjectBuilder();
        if(settingMap.containsKey("language") && settingMap.get("language") instanceof String){
            String language = settingMap.get("language").toString();
            editorSettingJson.add("language",language);
        }
        if(settingMap.containsKey("completionKey") && settingMap.get("completionKey") instanceof String){
            String language = settingMap.get("completionKey").toString();
            editorSettingJson.add("completionKey",language);
        }
        if(settingMap.containsKey("completionSupport") && settingMap.get("completionSupport") instanceof Boolean){
            boolean completionSupport = (Boolean) settingMap.get("completionSupport");
            editorSettingJson.add("completionSupport",completionSupport);
        }
        if(settingMap.containsKey("editOnDoubleClick") && settingMap.get("editOnDoubleClick") instanceof Boolean){
            boolean editOnDoublClick = (Boolean) settingMap.get("editOnDoubleClick");
            editorSettingJson.add("editOnDoubleClick",editOnDoublClick);
        }
        return editorSettingJson.build();
    }
}
