package com.teragrep.zep_01.socket.fakes;

import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.socket.NotebookSocket;
import com.teragrep.zep_01.socket.NotebookSocketListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Represents a single connection to a NotebookServer. Captures Messages that have a matchin OpCode to the given filter
 * TODO: this should be refactored into a decorator for a FakeNotebookSocket that accepts all incoming messages. This is pending extraction of NotebookSocket into an interface instead of concrete class
 */

// Have to extend NotebookSocket instead of implementing an Interface as we rely on send(), which isn't declared in an Interface, but a concrete class
public final class FilteringFakeNotebookSocket extends NotebookSocket {

    private final List<Message> sentMessages;
    private final Set<Message.OP> filteredOps;

    public FilteringFakeNotebookSocket(NotebookSocketListener listener, Set<Message.OP> filter) {
        super(new FakeHttpServletRequest(), "", listener);
        this.sentMessages = new ArrayList<>();
        this.filteredOps = filter;
    }

    @Override
    public void send(String serializeMessage){
        final Message message = Message.fromJson(serializeMessage);
        if(filteredOps.contains(message.op) || filteredOps.isEmpty()){
            sentMessages.add(Message.fromJson(serializeMessage));
        }
    }

    /**
     * Get a snapshot of the currently received messages
     * @return Unmodifiable list of the messages received at the time of the method call
     */
    public List<Message> sentMessages(){
        return Collections.unmodifiableList(new ArrayList<>(sentMessages));
    }
}