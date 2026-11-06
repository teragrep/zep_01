package com.teragrep.zep_01.socket;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UsernameImplTest {
    @Test
    public void testAsString(){
        final String expectedName = "/Ev1|И@m£";
        final UsernameImpl username = new UsernameImpl(expectedName);
        Assertions.assertEquals(expectedName, username.asString());
    }

    @Test
    public void testNotAnonymous(){
        final String name = "/Ev1|И@m£";
        final UsernameImpl username = new UsernameImpl(name);
        Assertions.assertFalse(username.isAnonymous());
    }

    @Test
    public void testContract(){
        EqualsVerifier.forClass(UsernameImpl.class).verify();
    }

}