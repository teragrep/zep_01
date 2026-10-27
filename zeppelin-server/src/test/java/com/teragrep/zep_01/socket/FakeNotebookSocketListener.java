package com.teragrep.zep_01.socket;

public class FakeNotebookSocketListener implements NotebookSocketListener {
    @Override
    public void onClose(final NotebookSocket socket, final int code, final String message) {
        // no-op
    }

    @Override
    public void onOpen(final NotebookSocket socket) {
        // no-op
    }

    @Override
    public void onMessage(final NotebookSocket socket, final String message) {
        // no-op
    }
}
