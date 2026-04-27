package com.teragrep.zep_01.common.exception;

public class IncompatibleValueException extends Throwable{
    public IncompatibleValueException(){
        super();
    }
    public IncompatibleValueException(final String message){
        super(message);
    }
    public IncompatibleValueException(final String message, final Throwable cause){
        super(message, cause);
    }
}
