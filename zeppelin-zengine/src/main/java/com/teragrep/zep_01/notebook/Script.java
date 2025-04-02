package com.teragrep.zep_01.notebook;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;
import com.teragrep.zep_01.scheduler.JobWithProgressPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public final class Script extends JobWithProgressPoller<InterpreterResult> implements Stubable {

    private static Logger LOGGER = LoggerFactory.getLogger(Script.class);
    private final Interpreter interpreter;
    private final String text;
    // Compatibility fields, other components need refactoring before we can remove these.
    private final InterpreterContext interpreterContext;
    // End compatibility fields

    public Script(String text, Interpreter interpreter, InterpreterContext interpreterContext){
        super("job_" + System.currentTimeMillis(),null);
        this.text = text;
        this.interpreter = interpreter;
        this.interpreterContext = interpreterContext;
    }

    public String text(){
        return text;
    }
    public Interpreter interpreter(){
        return interpreter;
    }
    public Result execute() throws InterpreterException {
        InterpreterResult result = interpreter.interpret(ParagraphTextParser.parse(text).getScriptText(),interpreterContext);
        return new Result(result.code(),result.message());
    }

    public JsonObject json(){
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("text",text);
        builder.add("interpreter",interpreter.json());
        return builder.build();
    }

    public InterpreterContext interpreterContext(){
        return interpreterContext;
    }

    // Overrides from legacy Job class
    @Override
    public InterpreterResult getReturn() {
        throw new UnsupportedOperationException("getReturn is no longer supported, use Paragraph.result() instead");
    }

    @Override
    public int progress() {
        try{
            return interpreter.getProgress(interpreterContext);
        }catch (InterpreterException exception){
            throw new RuntimeException("Failed to get execution progress from Interpreter!",exception);
        }
    }

    @Override
    public Map<String, Object> info() {
        throw new UnsupportedOperationException("info() is no longer supported.");
    }

    @Override
    protected InterpreterResult jobRun() throws Throwable {
        Result result = execute();
        return new InterpreterResult(result.code(),result.messages());
    }

    @Override
    protected boolean jobAbort() {
        try {
            interpreter.cancel(interpreterContext);
        } catch (InterpreterException e) {
            throw new RuntimeException("Failed to abort Script execution!",e);
        }

        return true;
    }

    @Override
    public void setResult(InterpreterResult result) {
        throw new UnsupportedOperationException("setResult is no longer supported!");
    }

    @Override
    public boolean isStub() {
        return false;
    }
}
