package org.apache.zeppelin.interpreter.xref.remote;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.thrift.InterpreterRPCException;

public interface RemoteFunction<R, T> {

    R call(T client) throws InterpreterRPCException, TException;

}
