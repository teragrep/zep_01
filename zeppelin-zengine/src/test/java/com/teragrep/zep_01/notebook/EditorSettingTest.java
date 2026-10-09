package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public final class EditorSettingTest {

    @Test
    void testAsJson() {
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

        Assertions.assertEquals(4, json.size());
        Assertions.assertEquals(language, json.getString("language"));
        Assertions.assertEquals(completionKey, json.getString("completionKey"));
        Assertions.assertEquals(completionSupport, json.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, json.getBoolean("editOnDoubleClick"));
    }

    @Test
    void testMissingKeys() {
        final boolean completionSupport = true;
        final boolean editOnDoubleClick = true;

        final Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        final EditorSetting editorSetting = new EditorSetting(editorSettings);
        final JsonObject json = editorSetting.asJson();

        // Missing keys should be omitted, valid keys should exist
        Assertions.assertEquals(2, json.size());
        Assertions.assertEquals(completionSupport, json.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, json.getBoolean("editOnDoubleClick"));
    }

    @Test
    void testInvalidValues() {
        final String language = "test";
        final String completionKey = "TAB";
        final String invalidCompletionSupport = "true";
        final long invalidEditOnDoubleClick = 100l;

        final Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("language",language);
        editorSettings.put("completionKey",completionKey);
        editorSettings.put("completionSupport",invalidCompletionSupport);
        editorSettings.put("editOnDoubleClick",invalidEditOnDoubleClick);

        final EditorSetting editorSetting = new EditorSetting(editorSettings);
        final JsonObject json = editorSetting.asJson();

        // Keys with invalid values should be omitted, keys with valid values should exist
        Assertions.assertEquals(2, json.size());
        Assertions.assertEquals(language, json.getString("language"));
        Assertions.assertEquals(completionKey, json.getString("completionKey"));
        Assertions.assertFalse(json.containsKey("completionSupport"));
        Assertions.assertFalse(json.containsKey("editOnDoubleClick"));
    }

    @Test
    void testInvalidKey() {
        final String language = "test";
        final String completionKey = "TAB";
        final boolean completionSupport = true;
        final boolean editOnDoubleClick = true;

        final Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("invalidLanguage",language);
        editorSettings.put("invalidCompletionKey",completionKey);
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        final EditorSetting editorSetting = new EditorSetting(editorSettings);
        final JsonObject json = editorSetting.asJson();

        // Invalid keys should be omitted, valid keys should exist
        Assertions.assertEquals(2, json.size());
        Assertions.assertFalse(json.containsKey("language"));
        Assertions.assertFalse(json.containsKey("completionKey"));
        Assertions.assertEquals(completionSupport, json.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, json.getBoolean("editOnDoubleClick"));
    }

    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(EditorSetting.class).verify();
    }
}