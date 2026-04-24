package com.teragrep.zep_01.display;

public class DynamicFormException extends Exception {
    public DynamicFormException(){
        super();
    }
    public DynamicFormException(String message){
        super(message);
    }
    public DynamicFormException(String message, Throwable cause){
        super(message, cause);
    }
}
