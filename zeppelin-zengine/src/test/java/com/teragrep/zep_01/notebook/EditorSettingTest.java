package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


class EditorSettingTest {

    @Test
    void asJson() {
        String language = "test";
        String completionKey = "TAB";
        boolean completionSupport = true;
        boolean editOnDoubleClick = true;

        Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("language",language);
        editorSettings.put("completionKey",completionKey);
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        EditorSetting editorSetting = new EditorSetting(editorSettings);
        JsonObject json = editorSetting.asJson();

        Assertions.assertEquals(language, json.getString("language"));
        Assertions.assertEquals(completionKey, json.getString("completionKey"));
        Assertions.assertEquals(completionSupport, json.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, json.getBoolean("editOnDoubleClick"));
    }
}