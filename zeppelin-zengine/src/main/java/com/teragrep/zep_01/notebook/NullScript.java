package com.teragrep.zep_01.notebook;

import jakarta.json.JsonObject;
import com.teragrep.zep_01.interpreter.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class NullScript{

    private static Logger LOGGER = LoggerFactory.getLogger(NullScript.class);

    public NullScript(){
    }

    public String text(){
        throw new UnsupportedOperationException("NullScript can't have a text!");
    }
    public Interpreter interpreter(){
        throw new UnsupportedOperationException("NullScript can't have an interpreter!");
    }
    public Result execute() throws InterpreterException {
        throw new UnsupportedOperationException("Cannot execute a NullScript!");
    }

    public JsonObject json(){
        throw new UnsupportedOperationException("Cannot turn NullScript into JSON!");
    }

    public InterpreterResult getReturn() {
        throw new UnsupportedOperationException("NullScript cannot have a return!");
    }

    public int progress() {
        throw new UnsupportedOperationException("NullScript cannot have a progress!");
    }

    public Map<String, Object> info() {
        throw new UnsupportedOperationException("NullScript cannot have an info!");
    }

    protected InterpreterResult jobRun() throws Throwable {
        throw new UnsupportedOperationException("Cannot run a NullScript!");
    }

    protected boolean jobAbort() {
        throw new UnsupportedOperationException("Cannot abort a NullScript!");
    }

    public void setResult(InterpreterResult result) {
        throw new UnsupportedOperationException("Cannot set a result to NullScript!");
    }

    public Script load(JsonObject json, String paragraphId){
        try{
            String text = json.getString("text");
            String intpText = ParagraphTextParser.parse(text).getIntpText();
            String scriptText = ParagraphTextParser.parse(text).getScriptText();
            InterpreterFactory factory = InterpreterFactory.getInstance();
            ExecutionContext context = new ExecutionContext("anonymous",null,json.getJsonObject("interpreter").getString("defaultInterpreterGroup"));
            Interpreter interpreter = factory.getInterpreter(intpText,context);
            InterpreterContext interpreterContext = InterpreterContext.builder().setReplName(intpText)
                    .setParagraphText(scriptText)
                    .setParagraphId(paragraphId)
                    .build();
            return new Script(text,interpreter,interpreterContext);
        } catch (InterpreterNotFoundException exception){
            throw new UnsupportedOperationException(exception);
        }
    }

    // TODO: InterpreterContext must have paragraphId, so it must be passed to legacyLoad. remove this when you have fixed interpreters pls
    public Script legacyLoad(String text, String defaultInterpreterGroup, String paragraphId){
        try{
            InterpreterFactory factory = InterpreterFactory.getInstance();
            ExecutionContext context = new ExecutionContext("anonymous",null,defaultInterpreterGroup);
            String intpText = ParagraphTextParser.parse(text).getIntpText();
            String scriptText = ParagraphTextParser.parse(text).getScriptText();
            Interpreter interpreter = factory.getInterpreter(intpText,context);
            //Interpreter interpreter = factory.getInterpreter(text.substring(text.indexOf("%"),text.indexOf("\n")),context);
            InterpreterContext interpreterContext = InterpreterContext.builder().setReplName(intpText)
                    .setParagraphText(scriptText)
                    .setParagraphId(paragraphId)
                    .build();
            return new Script(text,interpreter,interpreterContext);
        } catch (InterpreterNotFoundException exception){
            throw new UnsupportedOperationException(exception);
        }

    }
}
