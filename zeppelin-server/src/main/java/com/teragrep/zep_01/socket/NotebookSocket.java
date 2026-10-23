package com.teragrep.zep_01.socket;

import org.eclipse.jetty.websocket.api.WebSocketListener;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Responsibilities:
 * - Receives messages from websocket client via {@link WebSocketListener}
 * - Sends messages to websocket client
 * - Provides access to the HttpServletRequest that was used to create the websocket connection
 * - Sets a username for the connection
 **/

public interface NotebookSocket extends WebSocketListener {

    public abstract void send(String serializeMessage) throws IOException;

    public abstract String getUser();

    public abstract void setUser(String user);

    public abstract HttpServletRequest getRequest();
}
