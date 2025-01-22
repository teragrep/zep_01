package org.apache.zeppelin.interpreter.xref.user;

import org.apache.zeppelin.user.UsernamePassword;

public interface UserCredentials {

    UsernamePassword getUsernamePassword(String entity);

    void putUsernamePassword(String entity, UsernamePassword up);

    void removeUsernamePassword(String entity);

    boolean existUsernamePassword(String entity);

}
