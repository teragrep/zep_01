package com.teragrep.zep_01.socket;

import org.eclipse.jetty.websocket.api.WebSocketListener;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Represents a Websocket connectionn to a single client
 * Responsibilities:
 * - Receives messages from websocket client via {@link WebSocketListener}
 * - Sends messages to websocket client
 * - Provides access to the {@link HttpServletRequest} that was used to create the websocket connection
 * - Provides access to a {@link Username} that is associated with this connection
 **/

public interface NotebookSocket extends WebSocketListener {

    /**
     * Send a string payload to the remote client.
     * @param serializeMessage String payload to send
     * @throws IOException If an immediate Exception occurs that prevents transmitting
     */
    public abstract void send(String serializeMessage) throws IOException;

    public abstract Username username();

    public abstract HttpServletRequest getRequest();
}
