package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.interpreter.xref.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.xref.Type;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public interface InterpreterResultMessageOutput extends Closeable, Flushable {

    void setEnableTableAppend(boolean enableTableAppend);

    Type getType();

    void setType(Type type);

    void clear();

    void clear(boolean sendUpdateToFrontend);

    void write(int b) throws IOException;

    void write(byte[] b) throws IOException;

    void write(byte[] b, int off, int len) throws IOException;

    void write(File file) throws IOException;

    void write(String string) throws IOException;

    void write(URL url) throws IOException;

    void setResourceSearchPaths(List<String> resourceSearchPaths);

    void writeResource(String resourceName) throws IOException;

    byte[] toByteArray() throws IOException;

    InterpreterResultMessage toInterpreterResultMessage() throws IOException;

    void flush() throws IOException;

    boolean isAppendSupported();

    @Override
    void close() throws IOException;

    String toString();

}
