package com.teragrep.zep_01.socket;

import java.util.Objects;

public final class UsernameImpl implements Username {

    private final String username;

    public UsernameImpl(final String username){
        this.username = username;
    }

    @Override
    public String asString() {
        return username;
    }

    @Override
    public boolean isStub() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final UsernameImpl username1 = (UsernameImpl) o;
            equals = Objects.equals(username, username1.username);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(username);
    }
}
