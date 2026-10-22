package com.teragrep.zep_01.socket;

import org.eclipse.jetty.websocket.api.WebSocketListener;

import java.io.IOException;

public interface NotebookSocket extends WebSocketListener {

    public abstract void send(String serializeMessage) throws IOException;

    public abstract String getUser();

    public abstract void setUser(String user);

    public abstract boolean watcherPermitted();
}
