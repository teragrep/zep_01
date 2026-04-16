package com.teragrep.pth_07.performance.metric.value;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class DoubleMetricValueTest {

    @Test
    public void testContract() {
        EqualsVerifier.forClass(DoubleMetricValue.class).verify();
    }
}