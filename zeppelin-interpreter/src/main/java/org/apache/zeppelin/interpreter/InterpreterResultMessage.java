package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.interpreter.xref.Type;

import java.io.Serializable;

public interface InterpreterResultMessage extends Serializable {

    Type getType();

    String getData();

    String toString();

}
