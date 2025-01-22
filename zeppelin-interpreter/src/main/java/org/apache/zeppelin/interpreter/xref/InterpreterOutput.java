package org.apache.zeppelin.interpreter.xref;

import org.apache.zeppelin.interpreter.InterpreterOutputListener;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutputListener;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public interface InterpreterOutput extends Closeable, Flushable {

    void setEnableTableAppend(boolean enableTableAppend);

    void setType(Type type) throws IOException;

    void addInterpreterOutListener(InterpreterOutputListener outputListener);

    InterpreterResultMessageOutputListener createInterpreterResultMessageOutputListener(
            int index
    );

    InterpreterResultMessageOutput getCurrentOutput();

    InterpreterResultMessageOutput getOutputAt(int index);

    int size();

    void clear();

    void clear(boolean sendUpdateToFrontend);

    void write(int b) throws IOException;

    void write(byte[] b) throws IOException;

    void write(byte[] b, int off, int len) throws IOException;

    void write(File file) throws IOException;

    void write(String string) throws IOException;

    void write(URL url) throws IOException;

    void addResourceSearchPath(String path);

    void writeResource(String resourceName) throws IOException;

    List<InterpreterResultMessage> toInterpreterResultMessage() throws IOException;

    void flush() throws IOException;

    byte[] toByteArray() throws IOException;

    @Override
    String toString();

    @Override
    void close() throws IOException;

    long getLastWriteTimestamp();

}
