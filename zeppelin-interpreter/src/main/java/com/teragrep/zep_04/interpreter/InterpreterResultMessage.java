package com.teragrep.zep_04.interpreter;

import java.io.Serializable;

public interface InterpreterResultMessage extends Serializable {

    Type getType();

    String getData();

    String toString();

}
