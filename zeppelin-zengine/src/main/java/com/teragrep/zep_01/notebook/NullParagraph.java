package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import com.teragrep.zep_01.conf.ZeppelinConfiguration;
import com.teragrep.zep_01.interpreter.InterpreterResult;

import java.util.ArrayList;

public final class NullParagraph {
    private final NullResult result;
    private final NullScript script;

    public NullParagraph(){
        this.result = new NullResult();
        this.script = new NullScript();
    }

    public Result run() throws Exception {
        throw new UnsupportedOperationException("Cannot run NullParagraph!");
    }

    public JsonObject json(){
        throw new UnsupportedOperationException("NullParagraph can't have an JsonObject!");
    }

    public Paragraph fromJson(JsonObject json){
        String id = json.getString("id");
        String title = json.containsKey("title") ? json.getString("title") : "";
        Result result = json.containsKey("results") ? this.result.load(json.getJsonObject("results")) : new Result(InterpreterResult.Code.SUCCESS,new ArrayList<>());
        Script script = json.containsKey("script") ? this.script.load(json.getJsonObject("script"),id) : this.script.legacyLoad(json.containsKey("text") ? json.getString("text") : "", ZeppelinConfiguration.create().getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT),id);
        return new Paragraph(id,title,result,script);
    }
    public String name(){
        throw new UnsupportedOperationException("NullParagraph can't have a name!");
    }
    public String id(){
        throw new UnsupportedOperationException("NullParagraph can't have an ID!");
    }

    public Script script(){
        throw new UnsupportedOperationException("NullParagraph can't have a Script!");
    }

    public Result result(){
        throw new UnsupportedOperationException("NullParagraph can't have a result!");
    }
}
