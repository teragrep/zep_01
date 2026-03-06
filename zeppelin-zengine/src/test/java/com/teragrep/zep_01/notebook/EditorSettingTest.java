package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


class EditorSettingTest {

    @Test
    void asJson() {
        final String language = "test";
        final String completionKey = "TAB";
        final boolean completionSupport = true;
        final boolean editOnDoubleClick = true;

        final Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("language",language);
        editorSettings.put("completionKey",completionKey);
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        final EditorSetting editorSetting = new EditorSetting(editorSettings);
        final JsonObject json = editorSetting.asJson();

        Assertions.assertEquals(language, json.getString("language"));
        Assertions.assertEquals(completionKey, json.getString("completionKey"));
        Assertions.assertEquals(completionSupport, json.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, json.getBoolean("editOnDoubleClick"));
    }
}