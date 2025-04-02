package com.teragrep.zep_01.notebook;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import com.teragrep.zep_01.interpreter.*;

import java.util.ArrayList;
import java.util.List;

public final class NullResult{

    public NullResult(){
    }

    public List<InterpreterResultMessage> messages(){
        throw new UnsupportedOperationException("NullResult can't have messages!");
    }
    public InterpreterResult.Code code(){
        throw new UnsupportedOperationException("NullResult can't have a Code!");
    }

    public JsonObject json(){
        throw new UnsupportedOperationException("NullResult can't be turned into JSON!");
    }

    public Result load(JsonObject json){
        InterpreterResult.Code code = InterpreterResult.Code.valueOf(json.getString("code"));
        JsonArray messagesJson = json.getJsonArray("msg");
        ArrayList<InterpreterResultMessage> messages = new ArrayList<>();
        for (JsonObject messageJson:messagesJson.getValuesAs(JsonObject.class)
             ) {
            messages.add(new InterpreterResultMessage(InterpreterResult.Type.valueOf(messageJson.getString("type")),messageJson.getString("data")));
        }
        return new Result(code,messages);
    }
}
