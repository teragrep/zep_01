package com.teragrep.zep_01.socket.fakes;

import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.socket.NotebookSocket;
import com.teragrep.zep_01.socket.NotebookSocketListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a single connection to a NotebookServer. Captures all messages the NotebookServer sends to the user as a list of Strings
 */

// Have to extend NotebookSocket instead of implementing an Interface as we rely on send(), which isn't declared in an Interface, but a concrete class
public class FakeNotebookSocket extends NotebookSocket {

    List<String> sentMessages = new ArrayList<>();

    public FakeNotebookSocket(NotebookSocketListener listener) {
        super(new FakeHttpServletRequest(), "", listener);
    }

    @Override
    public void send(String serializeMessage){
        sentMessages.add(serializeMessage);
    }

    /**
     * Get a snapshot of the currently received messages
     * @return Unmodifiable list of the messages received at the time of the method call
     */
    public List<String> sentMessages(){
        return Collections.unmodifiableList(new ArrayList<>(sentMessages));
    }
}