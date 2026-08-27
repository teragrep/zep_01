package com.teragrep.zep_01.notebook.repo;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class RevisionTest {

    @Test
    public void testIsEmpty(){
        final String id = "id";
        final String message = "message";
        final int time = 178300000;
        final Revision revision = new Revision(id,message,time);
        Assertions.assertFalse(revision.isEmpty());

        final String emptyId = "";
        final String emptyMessage = "";
        final int emptyTime = 0;
        final Revision emptyRevision = new Revision(emptyId,emptyMessage,emptyTime);
        Assertions.assertTrue(emptyRevision.isEmpty());
    }
    @Test
    public void testContract() {
        EqualsVerifier.forClass(Revision.class).verify();
    }
}