package com.teragrep.zep_01.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public final class DefaultFormatDate {
    private final SimpleDateFormat format;
    private final Date date;

    public DefaultFormatDate(Date date){
        this(date, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"));
    }

    private DefaultFormatDate(Date date, SimpleDateFormat format){
        this.date = date;
        this.format = format;
    }
    public String asFormattedString(){
        return format.format(date);
    };

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final DefaultFormatDate that = (DefaultFormatDate) o;
            equals = Objects.equals(format, that.format) && Objects.equals(date, that.date);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, date);
    }
}
