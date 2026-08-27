package com.teragrep.zep_01.notebook.repo;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public final class Revision {
/**
 * Represents the 'Revision' a point in life of the notebook
 */

    private final String id;
    private final String message;
    private final int time;

    public Revision() {
        this(StringUtils.EMPTY, StringUtils.EMPTY, 0);
    }

    public Revision(final String revId, final String message, final int time) {
        this.id = revId;
        this.message = message;
        this.time = time;
    }

    public boolean isEmpty() {
        return (id.isEmpty() && message.isEmpty() && time == 0);
    }

    public String id(){
        return id;
    }

    public String message(){
        return message;
    }

    public int time(){
        return time;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Revision revision = (Revision) o;
        return time == revision.time && Objects.equals(id, revision.id) && Objects.equals(message, revision.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, message, time);
    }
}
