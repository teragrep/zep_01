package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.lang.reflect.Proxy;

class NotebookSocketImplTest {

    @Test
    public void testContract(){
        EqualsVerifier.forClass(NotebookSocketImpl.class).withIgnoredFields("username","connection").verify();
    }
    // calling send() before a call to onWebSocketConnect() is received should result in an IOException
    @Test
    public void testCallingSendWithUninitializedConnectionThrowsIOException(){
        NotebookSocketImpl socket = new NotebookSocketImpl(null,new FakeNotebookSocketListener());
        Throwable error = Assertions.assertThrows(IOException.class,()->{socket.send("lol");});
        Assertions.assertEquals("Tried to send a message to a Websocket connection with no established Session!",error.getMessage());
    }
}