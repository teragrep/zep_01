package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;


public class NoteConfigTest {

    @Test
    public void asJson(){
        String bodyClassName = "testName";
        String cronInput = "testCron";
        boolean cronEnabled = true;
        String looknfeel = "testLookNFeel";
        String personalizedMode = "testPersonalizedMode";
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("bodyClassName", bodyClassName);
        configMap.put("cronInput", cronInput);
        configMap.put("isZeppelinNotebookCronEnable", cronEnabled);
        configMap.put("looknfeel", looknfeel);
        configMap.put("personalizedMode", personalizedMode);

        NoteConfig config = new NoteConfig(configMap);
        JsonObject json = config.asJson();
        Assertions.assertEquals(5, json.size());
        Assertions.assertEquals(bodyClassName,json.getString("bodyClassName"));
        Assertions.assertEquals(cronInput,json.getString("cronInput"));
        Assertions.assertEquals(cronEnabled,json.getBoolean("isZeppelinNotebookCronEnable"));
        Assertions.assertEquals(looknfeel,json.getString("looknfeel"));
        Assertions.assertEquals(personalizedMode,json.getString("personalizedMode"));
    }
}