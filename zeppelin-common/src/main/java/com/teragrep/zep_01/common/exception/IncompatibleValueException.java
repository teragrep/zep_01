package com.teragrep.zep_01.common.exception;

public class IncompatibleValueException extends Throwable{
    public IncompatibleValueException(){
        super();
    }
    public IncompatibleValueException(String message){
        super(message);
    }
    public IncompatibleValueException(String message, Throwable cause){
        super(message, cause);
    }
}
