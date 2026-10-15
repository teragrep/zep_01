package com.teragrep.zep_01.common;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.regex.Pattern;

class DefaultFormatDateTest {

    @Test
    void testFormat() {
        final Date date = Date.from(Instant.ofEpochSecond(1000000000L));
        final DefaultFormatDate defaultFormat = new DefaultFormatDate(date);
        final String formatted = defaultFormat.asFormattedString();

        // Formatted time should always contain 24 characters in format yyyy-mm-ddThh:mm:ss+timezone
        final Pattern regexPattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[+-]\\d{4}");
        Assertions.assertTrue(regexPattern.matcher(formatted).matches());
    }

    @Test
    public void testContract(){
        EqualsVerifier.forClass(DefaultFormatDate.class).verify();
    }
}