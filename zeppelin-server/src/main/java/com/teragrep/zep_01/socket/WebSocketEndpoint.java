package com.teragrep.zep_01.socket;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Represents an outgoing WebSocket connection to a single client.
 * Responsibilities:
 * - Sends messages to a websocket client
 * - Provides access to the {@link HttpServletRequest} that was used to create the websocket connection
 * - Provides access to a {@link Username} that is associated with this connection
 * - Sets a {@link Username} for this connection.
 **/

public interface WebSocketEndpoint {

    /**
     * Send a string payload to the remote client.
     * @param serializeMessage String payload to send
     * @throws IOException If an immediate Exception occurs that prevents transmitting
     */
    public abstract void send(String serializeMessage) throws IOException;

    public abstract Username username();

    public abstract void rename(Username username);

    public abstract HttpServletRequest getRequest();
}
