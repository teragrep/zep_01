package com.teragrep.zep_01.socket;

public final class FakeNotebookSocketListener implements NotebookSocketListener {
    @Override
    public void onClose(final WebSocketEndpoint socket, final int code, final String message) {
        // no-op
    }

    @Override
    public void onOpen(final WebSocketEndpoint socket) {
        // no-op
    }

    @Override
    public void onMessage(final WebSocketEndpoint socket, final String message) {
        // no-op
    }
}
