package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NamedNotebookSocketTest {

    @Test
    public void testContract(){
        EqualsVerifier.forClass(NamedNotebookSocket.class).verify();
    }
}