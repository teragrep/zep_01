package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.common.JsonSerializable;
import org.apache.zeppelin.interpreter.xref.Code;
import org.apache.zeppelin.interpreter.xref.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.xref.Type;

import java.io.Serializable;
import java.util.List;

public interface InterpreterResult extends Serializable, JsonSerializable {

    void add(String msg);

    void add(Type type, String data);

    void add(InterpreterResultMessage interpreterResultMessage);

    Code code();

    List<InterpreterResultMessage> message();

    String toString();

}
