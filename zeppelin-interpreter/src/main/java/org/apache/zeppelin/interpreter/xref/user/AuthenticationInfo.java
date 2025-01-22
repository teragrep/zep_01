package org.apache.zeppelin.interpreter.xref.user;

import org.apache.zeppelin.common.JsonSerializable;

import java.util.List;
import java.util.Set;

public interface AuthenticationInfo extends JsonSerializable {

    String getUser();

    void setUser(String user);

    Set<String> getRoles();

    void setRoles(Set<String> roles);

    List<String> getUsersAndRoles();

    String getTicket();

    void setTicket(String ticket);

    UserCredentials getUserCredentials();

    void setUserCredentials(UserCredentials userCredentials);

    boolean isAnonymous();

}
