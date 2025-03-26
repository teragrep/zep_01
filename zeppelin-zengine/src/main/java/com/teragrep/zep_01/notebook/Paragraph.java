package com.teragrep.zep_01.notebook;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;
import com.teragrep.zep_01.interpreter.InterpreterSetting;
import com.teragrep.zep_01.interpreter.ManagedInterpreterGroup;

import java.util.Map;

public final class Paragraph {
    private final String id;
    private final String title;
    private final Result result;
    private final Script script;

    public Paragraph(String id, String title, Result result, Script script){
        this.id = id;
        this.title = title;
        this.result = result;
        this.script = script;
    }

    public Result run() throws Exception {
        try{
            return script.execute();
        }catch (Exception exception){
            throw new Exception("Failed to run paragraph "+id+"!",exception);
        }
    }

    public JsonObject json(){
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id",id);
        builder.add("title", title != null? title : "");
        builder.add("script",script.json());
        builder.add("results",result.json());
        //compatibility fields, UI updates required to remove these//
        InterpreterSetting interpreterSetting = ((ManagedInterpreterGroup)
                script().interpreter().getInterpreterGroup()).getInterpreterSetting();
        Map<String, Object> config = interpreterSetting.getConfig(script().interpreter().getClassName());
        builder.add("settings",JsonValue.EMPTY_JSON_OBJECT);
        builder.add("text",script.text());
        builder.add("config",Json.createObjectBuilder(config));
        // end //
        return builder.build();
    }
    public String title(){
        return title;
    }
    public String id(){
        return id;
    }

    public Script script(){
        return script;
    }

    public Result result(){
        return result;
    }
}
