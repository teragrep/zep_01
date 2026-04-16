package com.teragrep.pth_07.performance.metric.value;

import com.teragrep.pth_07.performance.metric.BytesPerSecond;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DoubleMetricValueTest {

    @Test
    public void testContract() {
        EqualsVerifier.forClass(DoubleMetricValue.class).verify();
    }
}