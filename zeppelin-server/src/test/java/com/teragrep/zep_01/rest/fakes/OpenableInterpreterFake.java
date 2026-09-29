package com.teragrep.zep_01.rest.fakes;

import com.teragrep.zep_01.interpreter.Interpreter;
import com.teragrep.zep_01.interpreter.InterpreterContext;
import com.teragrep.zep_01.interpreter.InterpreterException;
import com.teragrep.zep_01.interpreter.InterpreterResult;

import java.util.Properties;

/**
 * Fake interpreter that can report whether it has been opened
 */
public final class OpenableInterpreterFake extends Interpreter {
    private boolean isOpened;

    public OpenableInterpreterFake(Properties properties) {
        super(properties);
        isOpened = false;
    }

    @Override
    public void open() throws InterpreterException {
        isOpened = true;
    }

    @Override
    public void close() throws InterpreterException {
        isOpened = false;
    }

    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) throws InterpreterException {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS,st);
    }

    @Override
    public void cancel(InterpreterContext context) throws InterpreterException {
    }

    @Override
    public FormType getFormType() throws InterpreterException {
        return FormType.NONE;
    }

    @Override
    public int getProgress(InterpreterContext context) throws InterpreterException {
        return 100;
    }

    public boolean isOpened(){
        return isOpened;
    }
}
