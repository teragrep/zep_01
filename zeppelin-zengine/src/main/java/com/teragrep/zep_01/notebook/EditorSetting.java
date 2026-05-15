package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Objects;

public final class EditorSetting implements Jsonable {
    private final Map<String, Object> settingMap;

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final EditorSetting that = (EditorSetting) o;
        return Objects.equals(settingMap, that.settingMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settingMap);
    }

    public EditorSetting(final Map<String, Object> settingMap){
        this.settingMap = settingMap;
    }
    @Override
    public JsonObject asJson() {
        final JsonObjectBuilder editorSettingJson = Json.createObjectBuilder();
        if(settingMap.containsKey("language") && settingMap.get("language") instanceof String){
            final String language = settingMap.get("language").toString();
            editorSettingJson.add("language",language);
        }
        if(settingMap.containsKey("completionKey") && settingMap.get("completionKey") instanceof String){
            final String language = settingMap.get("completionKey").toString();
            editorSettingJson.add("completionKey",language);
        }
        if(settingMap.containsKey("completionSupport") && settingMap.get("completionSupport") instanceof Boolean){
            final boolean completionSupport = (Boolean) settingMap.get("completionSupport");
            editorSettingJson.add("completionSupport",completionSupport);
        }
        if(settingMap.containsKey("editOnDoubleClick") && settingMap.get("editOnDoubleClick") instanceof Boolean){
            final boolean editOnDoublClick = (Boolean) settingMap.get("editOnDoubleClick");
            editorSettingJson.add("editOnDoubleClick",editOnDoublClick);
        }
        return editorSettingJson.build();
    }
}
