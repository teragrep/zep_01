package com.teragrep.zep_04.interpreter;

import java.io.*;
import java.net.URL;
import java.util.List;

public abstract class InterpreterOutput extends OutputStream implements Closeable, Flushable {

    abstract public void setEnableTableAppend(boolean enableTableAppend);

    abstract public void setType(Type type) throws IOException;

    abstract public void addInterpreterOutListener(InterpreterOutputListener outputListener);

    abstract public InterpreterResultMessageOutputListener createInterpreterResultMessageOutputListener(
            int index
    );

    abstract public InterpreterResultMessageOutput getCurrentOutput();

    abstract public InterpreterResultMessageOutput getOutputAt(int index);

    abstract public int size();

    abstract public void clear();

    abstract public void clear(boolean sendUpdateToFrontend);

    abstract public void write(int b) throws IOException;

    abstract public void write(byte[] b) throws IOException;

    abstract public void write(byte[] b, int off, int len) throws IOException;

    abstract public void write(File file) throws IOException;

    abstract public void write(String string) throws IOException;

    abstract public void write(URL url) throws IOException;

    abstract public void addResourceSearchPath(String path);

    abstract public void writeResource(String resourceName) throws IOException;

    abstract public List<InterpreterResultMessage> toInterpreterResultMessage() throws IOException;

    abstract public void flush() throws IOException;

    abstract public byte[] toByteArray() throws IOException;

    @Override
    abstract public String toString();

    @Override
    abstract public void close() throws IOException;

    abstract public long getLastWriteTimestamp();

}
