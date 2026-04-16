package com.teragrep.pth_07.performance.metric.value;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

class LongMetricValueTest {

    @Test
    public void testContract() {
        EqualsVerifier.forClass(LongMetricValue.class).verify();
    }
}