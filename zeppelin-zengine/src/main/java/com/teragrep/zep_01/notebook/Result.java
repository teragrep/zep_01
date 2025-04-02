package com.teragrep.zep_01.notebook;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.interpreter.InterpreterResultMessage;

import java.util.List;

public final class Result implements Stubable{
    private final List<InterpreterResultMessage> messages;
    private final InterpreterResult.Code code;

    public Result(InterpreterResult.Code code, List<InterpreterResultMessage> messages){
        this.messages = messages;
        this.code = code;
    }
    public List<InterpreterResultMessage> messages(){
        return messages;
    }
    public InterpreterResult.Code code(){
        return code;
    }
    public JsonObject json(){
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("code",code.name());
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (InterpreterResultMessage message:messages) {
            arrayBuilder.add(message.json());
        }
        builder.add("msg",arrayBuilder.build());
        return builder.build();
    }

    @Override
    public boolean isStub() {
        return false;
    }
    //@Override
    //public Copyable copy(String id) {
    //    ArrayList<InterpreterResultMessage> copyMessages = new ArrayList<InterpreterResultMessage>();
    //    copyMessages.addAll(messages);
    //    return new Result(code,copyMessages);
    //}
}
