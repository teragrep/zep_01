package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UsernameAnonymousTest {

    @Test
    public void testIsAnonymous(){
        final UsernameAnonymous username = new UsernameAnonymous();
        Assertions.assertTrue(username.isAnonymous());
    }
    @Test
    public void testAsString(){
        final String expectedName = "anonymous";
        final UsernameAnonymous username = new UsernameAnonymous();
        Assertions.assertEquals(expectedName, username.asString());
    }

    @Test
    public void testContract(){
        EqualsVerifier.forClass(UsernameAnonymous.class).verify();
    }

}