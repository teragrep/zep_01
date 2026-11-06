package com.teragrep.zep_01.socket.fakes;

import com.teragrep.zep_01.common.Message;
import com.teragrep.zep_01.socket.*;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * Represents a single connection to a NotebookServer. Captures Messages that have a matchin OpCode to the given filter
 */

// Have to extend NotebookSocket instead of implementing an Interface as we rely on send(), which isn't declared in an Interface, but a concrete class
public final class FilteringFakeNotebookSocket implements WebSocketEndpoint {

    private final List<Message> sentMessages;
    private final Set<Message.OP> filteredOps;

    public FilteringFakeNotebookSocket(Set<Message.OP> filter) {
        this.sentMessages = new ArrayList<>();
        this.filteredOps = Collections.unmodifiableSet(new HashSet<>(filter));
    }

    @Override
    public void send(String serializeMessage){
        final Message message = Message.fromJson(serializeMessage);
        if(filteredOps.contains(message.op)){
            sentMessages.add(message);
        }
    }

    @Override
    public Username username() {
        return new UsernameAnonymous();
    }

    @Override
    public void rename(final Username username) {
        //no-op
    }

    @Override
    public HttpServletRequest getRequest() {
        return new FakeHttpServletRequest();
    }

    /**
     * Get a snapshot of the currently received messages
     * @return Unmodifiable list of the messages received at the time of the method call
     */
    public List<Message> sentMessages(){
        return Collections.unmodifiableList(new ArrayList<>(sentMessages));
    }
}