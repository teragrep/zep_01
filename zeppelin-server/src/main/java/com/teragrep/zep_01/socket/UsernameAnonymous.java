package com.teragrep.zep_01.socket;

import java.util.Objects;

public final class UsernameAnonymous implements Username {

    private final String name;
    private final boolean isAnonymous;

    public UsernameAnonymous(){
        this("anonymous",true);
    }
    private UsernameAnonymous(final String name, final boolean isAnonymous){
        this.name = name;
        this.isAnonymous = isAnonymous;
    }
    @Override
    public String asString() {
        return name;
    }

    @Override
    public boolean isAnonymous() {
        return isAnonymous;
    }

    @Override
    public boolean equals(final Object o) {
        final boolean equals;
        if (this == o) {
            equals = true;
        } else if (o == null || getClass() != o.getClass()) {
            equals = false;
        } else {
            final UsernameAnonymous that = (UsernameAnonymous) o;
            equals = isAnonymous == that.isAnonymous && Objects.equals(name, that.name);
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, isAnonymous);
    }
}
