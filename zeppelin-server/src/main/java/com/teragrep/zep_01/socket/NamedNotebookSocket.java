package com.teragrep.zep_01.socket;

import org.eclipse.jetty.websocket.api.Session;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Objects;

public final class NamedNotebookSocket implements NotebookSocket {

    private final NotebookSocket origin;
    private final Username username;

    public NamedNotebookSocket(final NotebookSocket origin, final String username){
        this(origin, new UsernameImpl(username));
    }

    public NamedNotebookSocket(final NotebookSocket origin, final Username username){
        this.origin = origin;
        this.username = username;
    }

    @Override
    public Username username() {
        return username;
    }

    @Override
    public void send(final String serializeMessage) throws IOException {
        origin.send(serializeMessage);
    }

    @Override
    public HttpServletRequest getRequest() {
        return origin.getRequest();
    }

    @Override
    public void onWebSocketBinary(final byte[] payload, final int offset, final int len) {
        origin.onWebSocketBinary(payload, offset, len);
    }

    @Override
    public void onWebSocketText(final String message) {
        origin.onWebSocketText(message);
    }

    @Override
    public void onWebSocketClose(final int statusCode, final String reason) {
        origin.onWebSocketClose(statusCode, reason);
    }

    @Override
    public void onWebSocketConnect(final Session session) {
        origin.onWebSocketConnect(session);
    }

    @Override
    public void onWebSocketError(final Throwable cause) {
        origin.onWebSocketError(cause);
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final NamedNotebookSocket that = (NamedNotebookSocket) o;
            equals = Objects.equals(origin, that.origin) && Objects.equals(username, that.username);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin, username);
    }
}
