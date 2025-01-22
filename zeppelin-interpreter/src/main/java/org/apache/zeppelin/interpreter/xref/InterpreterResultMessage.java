package org.apache.zeppelin.interpreter.xref;

import java.io.Serializable;

public interface InterpreterResultMessage extends Serializable {

    Type getType();

    String getData();

    String toString();

}
