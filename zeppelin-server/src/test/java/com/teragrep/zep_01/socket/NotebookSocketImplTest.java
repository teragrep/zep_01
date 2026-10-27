package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class NotebookSocketImplTest {

    @Test
    public void testContract(){
        EqualsVerifier.forClass(NotebookSocketImpl.class).verify();
    }
}