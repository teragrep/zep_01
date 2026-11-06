package com.teragrep.zep_01.socket;

/**
 * Represents the name of a connected user.
 * Responsibilities
 * - Returns the name of the user as a string
 * - Indicates whether the connected user is considered anonymous or not
 */

public interface Username {
    public abstract String asString();
    public abstract boolean isAnonymous();
}
