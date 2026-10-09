package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;


public final class NoteConfigTest {

    @Test
    public void testAsJson(){
        final String bodyClassName = "testName";
        final String cronInput = "testCron";
        final boolean cronEnabled = true;
        final String looknfeel = "testLookNFeel";
        final String personalizedMode = "testPersonalizedMode";
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("bodyClassName", bodyClassName);
        configMap.put("cronInput", cronInput);
        configMap.put("isZeppelinNotebookCronEnable", cronEnabled);
        configMap.put("looknfeel", looknfeel);
        configMap.put("personalizedMode", personalizedMode);

        final NoteConfig config = new NoteConfig(configMap);
        final JsonObject json = config.asJson();
        Assertions.assertEquals(5, json.size());
        Assertions.assertEquals(bodyClassName,json.getString("bodyClassName"));
        Assertions.assertEquals(cronInput,json.getString("cronInput"));
        Assertions.assertEquals(cronEnabled,json.getBoolean("isZeppelinNotebookCronEnable"));
        Assertions.assertEquals(looknfeel,json.getString("looknfeel"));
        Assertions.assertEquals(personalizedMode,json.getString("personalizedMode"));
    }

    @Test
    public void testMissingKeys(){
        final String cronInput = "testCron";
        final boolean cronEnabled = true;

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("cronInput", cronInput);
        configMap.put("isZeppelinNotebookCronEnable", cronEnabled);

        final NoteConfig config = new NoteConfig(configMap);
        final JsonObject json = config.asJson();
        Assertions.assertEquals(cronInput,json.getString("cronInput"));
        Assertions.assertEquals(cronEnabled,json.getBoolean("isZeppelinNotebookCronEnable"));

        // Missing keys should be omitted, valid keys should exist
        Assertions.assertEquals(2,json.size());
        Assertions.assertEquals(cronInput,json.getString("cronInput"));
        Assertions.assertEquals(cronEnabled,json.getBoolean("isZeppelinNotebookCronEnable"));
    }

    @Test
    public void testInvalidValues(){
        final boolean bodyClassName = true;
        final long cronInput = 100l;
        final int invalidCronEnabled = 500;
        final String looknfeel = "testLookNFeel";
        final String personalizedMode = "testPersonalizedMode";
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("bodyClassName", bodyClassName);
        configMap.put("cronInput", cronInput);
        configMap.put("isZeppelinNotebookCronEnable", invalidCronEnabled);
        configMap.put("looknfeel", looknfeel);
        configMap.put("personalizedMode", personalizedMode);

        // Keys with invalid values should be omitted, keys with valid values should exist
        final NoteConfig config = new NoteConfig(configMap);
        final JsonObject json = config.asJson();
        Assertions.assertEquals(4, json.size());
        Assertions.assertEquals("true",json.getString("bodyClassName"));
        Assertions.assertEquals("100",json.getString("cronInput"));
        Assertions.assertFalse(json.containsKey("isZeppelinNotebookCronEnable"));
        Assertions.assertEquals(looknfeel,json.getString("looknfeel"));
        Assertions.assertEquals(personalizedMode,json.getString("personalizedMode"));

    }

    @Test
    public void testInvalidKeys(){
        final String bodyClassName = "testName";
        final String cronInput = "testCron";
        final boolean cronEnabled = true;
        final String looknfeel = "testLookNFeel";
        final String personalizedMode = "testPersonalizedMode";
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("invalidBodyClassName", bodyClassName);
        configMap.put("invalidCronInput", cronInput);
        configMap.put("invalidIsZeppelinNotebookCronEnable", cronEnabled);
        configMap.put("looknfeel", looknfeel);
        configMap.put("personalizedMode", personalizedMode);

        // Invalid keys should be omitted, valid keys should exist
        final NoteConfig config = new NoteConfig(configMap);
        final JsonObject json = config.asJson();
        Assertions.assertEquals(2, json.size());
        Assertions.assertEquals(looknfeel,json.getString("looknfeel"));
        Assertions.assertEquals(personalizedMode,json.getString("personalizedMode"));
    }

    @Test
    public void equalsVerifier() {
        EqualsVerifier.forClass(NoteConfig.class).verify();
    }
}