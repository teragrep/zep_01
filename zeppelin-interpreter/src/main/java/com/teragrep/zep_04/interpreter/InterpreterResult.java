package com.teragrep.zep_04.interpreter;

import org.apache.zeppelin.common.JsonSerializable;

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
