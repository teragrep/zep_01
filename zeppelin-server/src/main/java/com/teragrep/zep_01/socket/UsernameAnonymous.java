package com.teragrep.zep_01.socket;


public final class UsernameAnonymous implements Username {

    @Override
    public String asString() {
        return "anonymous";
    }

    @Override
    public boolean isAnonymous() {
        return true;
    }

}
