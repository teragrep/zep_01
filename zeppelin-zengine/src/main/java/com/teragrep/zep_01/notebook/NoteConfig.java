package com.teragrep.zep_01.notebook;

import com.teragrep.zep_01.common.Jsonable;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;

public class NoteConfig implements Jsonable {
    private final Map<String,Object> configMap;
    public NoteConfig(Map<String,Object> configMap){
        this.configMap = configMap;
    }
    @Override
    public JsonObject asJson() {
        JsonObjectBuilder configJson = Json.createObjectBuilder();
        if(configMap.containsKey("bodyClassName") && configMap.get("bodyClassName") instanceof String){
            configJson.add("bodyClassName",(String) configMap.get("bodyClassName"));
        }
        if(configMap.containsKey("cronInput") && configMap.get("cronInput") instanceof String){
            configJson.add("cronInput",(String) configMap.get("cronInput"));
        }
        if(configMap.containsKey("isZeppelinNotebookCronEnable") && configMap.get("isZeppelinNotebookCronEnable") instanceof Boolean){
            configJson.add("isZeppelinNotebookCronEnable",(Boolean) configMap.get("isZeppelinNotebookCronEnable"));
        }
        if(configMap.containsKey("looknfeel") && configMap.get("looknfeel") instanceof String){
            configJson.add("looknfeel",(String) configMap.get("looknfeel"));
        }
        if(configMap.containsKey("personalizedMode") && configMap.get("personalizedMode") instanceof String){
            configJson.add("personalizedMode",(String) configMap.get("personalizedMode"));
        }
        return configJson.build();
    }
}
