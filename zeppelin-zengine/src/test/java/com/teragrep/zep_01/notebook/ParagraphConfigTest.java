package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public final class ParagraphConfigTest {

    @Test
    public void testAsJson() {
        final int colWidth = 12;
        final boolean enabled = true;
        final int fontSize = 12;
        final boolean lineNumbers = true;
        final String editorMode = "mode";

        final String language = "test";
        final String completionKey = "TAB";
        final boolean completionSupport = true;
        final boolean editOnDoubleClick = true;

        final Map<String,Object> editorSettings = new HashMap<>();
        editorSettings.put("language",language);
        editorSettings.put("completionKey",completionKey);
        editorSettings.put("completionSupport",completionSupport);
        editorSettings.put("editOnDoubleClick",editOnDoubleClick);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("colWidth",colWidth);
        configMap.put("enabled",enabled);
        configMap.put("fontSize",fontSize);
        configMap.put("lineNumbers",lineNumbers);
        configMap.put("editorSetting",editorSettings);
        configMap.put("editorMode",editorMode);
        final ParagraphConfig config = new ParagraphConfig(configMap);
        final JsonObject json = config.asJson();

        Assertions.assertEquals(colWidth,json.getInt("colWidth"));
        Assertions.assertEquals(enabled,json.getBoolean("enabled"));
        Assertions.assertEquals(fontSize,json.getInt("fontSize"));
        Assertions.assertEquals(lineNumbers,json.getBoolean("lineNumbers"));
        Assertions.assertEquals(editorMode,json.getString("editorMode"));

        final JsonObject editorSettingJson = json.getJsonObject("editorSetting");
        Assertions.assertEquals(language, editorSettingJson.getString("language"));
        Assertions.assertEquals(completionKey, editorSettingJson.getString("completionKey"));
        Assertions.assertEquals(completionSupport, editorSettingJson.getBoolean("completionSupport"));
        Assertions.assertEquals(editOnDoubleClick, editorSettingJson.getBoolean("editOnDoubleClick"));
    }

    @Test
    public void testMissingKeys() {
        final boolean enabled = true;
        final int fontSize = 12;
        final boolean lineNumbers = true;

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("enabled",enabled);
        configMap.put("fontSize",fontSize);
        configMap.put("lineNumbers",lineNumbers);
        final ParagraphConfig config = new ParagraphConfig(configMap);
        final JsonObject json = config.asJson();

        // Missing keys should be omitted, valid keys should still exist
        Assertions.assertEquals(3, json.size());
        Assertions.assertEquals(enabled,json.getBoolean("enabled"));
        Assertions.assertEquals(fontSize,json.getInt("fontSize"));
        Assertions.assertEquals(lineNumbers,json.getBoolean("lineNumbers"));
    }

    @Test
    public void testInvalidKeys() {
        final int colWidth = 12;
        final boolean enabled = true;
        final int fontSize = 12;
        final boolean lineNumbers = true;
        final String editorMode = "mode";

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("colWidth",colWidth);
        configMap.put("enabled",enabled);
        configMap.put("fontSize",fontSize);
        configMap.put("invalidLineNumbers",lineNumbers);
        configMap.put("invalidEditorMode",editorMode);
        final ParagraphConfig config = new ParagraphConfig(configMap);
        final JsonObject json = config.asJson();

        // Invalid keys should be omitted, valid keys should still exist
        Assertions.assertEquals(3, json.size());
        Assertions.assertEquals(colWidth,json.getInt("colWidth"));
        Assertions.assertEquals(enabled,json.getBoolean("enabled"));
        Assertions.assertEquals(fontSize,json.getInt("fontSize"));
        Assertions.assertFalse(json.containsKey("lineNumbers"));
        Assertions.assertFalse(json.containsKey("editorMode"));
    }

    @Test
    public void testInvalidValues() {
        final String invalidColumnWidth = "seventeen";
        final long invalidEnabled = 100l;
        final boolean invalidFontSize = false;
        final boolean lineNumbers = true;
        final String editorMode = "mode";


        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("colWidth",invalidColumnWidth);
        configMap.put("enabled",invalidEnabled);
        configMap.put("fontSize",invalidFontSize);
        configMap.put("lineNumbers",lineNumbers);
        configMap.put("editorMode",editorMode);
        final ParagraphConfig config = new ParagraphConfig(configMap);
        final JsonObject json = config.asJson();

        // Keys with invalid values should be omitted, keys with valid values should still exist
        Assertions.assertEquals(2, json.size());
        Assertions.assertFalse(json.containsKey("colWidth"));
        Assertions.assertFalse(json.containsKey("enabled"));
        Assertions.assertFalse(json.containsKey("fontSize"));
        Assertions.assertEquals(lineNumbers,json.getBoolean("lineNumbers"));
        Assertions.assertEquals(editorMode,json.getString("editorMode"));
    }

    @Test
    void equalsVerifier() {
        EqualsVerifier.forClass(ParagraphConfig.class).verify();
    }
}