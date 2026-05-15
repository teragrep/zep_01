package com.teragrep.zep_01.regex;

public class RegexInterpreterException extends Exception {

    public RegexInterpreterException(String message) {
        super(message);
    }

    public RegexInterpreterException(String message, Throwable cause) {
        super(message, cause);
    }

    public RegexInterpreterException(Throwable cause) {
        super(cause);
    }
}
