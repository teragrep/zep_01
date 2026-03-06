package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;


public class NoteConfigTest {

    @Test
    public void asJson(){
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
}