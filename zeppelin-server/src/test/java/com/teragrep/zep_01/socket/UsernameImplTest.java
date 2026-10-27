package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UsernameImplTest {

    @Test
    public void testContract(){
        EqualsVerifier.forClass(UsernameImpl.class).verify();
    }

}