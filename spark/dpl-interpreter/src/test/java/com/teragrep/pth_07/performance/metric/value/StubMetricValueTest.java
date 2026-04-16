package com.teragrep.pth_07.performance.metric.value;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StubMetricValueTest {

    @Test
    public void testValue(){
        StubMetricValue stub = new StubMetricValue();
        Assertions.assertThrows(UnsupportedOperationException.class, ()-> stub.value());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(StubMetricValue.class).verify();
    }
}