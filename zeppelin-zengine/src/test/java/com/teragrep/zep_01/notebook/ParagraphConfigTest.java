package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ParagraphConfigTest {

    @Test
    void asJson() {
        int colWidth = 12;
        boolean enabled = true;
        int fontSize = 12;
        boolean lineNumbers = true;
        String editorMode = "mode";

        String language = "test";
        String completionKey = "TAB";
        boolean completionSupport = true;
        boolean editOnDoubleClick = true;

        Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("language",language);
        editorSettings.put("completionKey",completionKey);
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("colWidth",colWidth);
        configMap.put("enabled",enabled);
        configMap.put("fontSize",fontSize);
        configMap.put("lineNumbers",lineNumbers);
        configMap.put("editorSetting",editorSettings);
        configMap.put("editorMode",editorMode);
        ParagraphConfig config = new ParagraphConfig(configMap);
        JsonObject json = config.asJson();

        Assertions.assertEquals(colWidth,json.getInt("colWidth"));
        Assertions.assertEquals(enabled,json.getBoolean("enabled"));
        Assertions.assertEquals(fontSize,json.getInt("fontSize"));
        Assertions.assertEquals(lineNumbers,json.getBoolean("lineNumbers"));
        Assertions.assertEquals(editorMode,json.getString("editorMode"));

        JsonObject editorSettingJson = json.getJsonObject("editorSetting");
        Assertions.assertEquals(language, editorSettingJson.getString("language"));
        Assertions.assertEquals(completionKey, editorSettingJson.getString("completionKey"));
        Assertions.assertEquals(completionSupport, editorSettingJson.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, editorSettingJson.getBoolean("editOnDoubleClick"));
    }
}