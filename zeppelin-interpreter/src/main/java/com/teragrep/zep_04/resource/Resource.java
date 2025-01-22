package com.teragrep.zep_04.resource;

import org.apache.zeppelin.common.JsonSerializable;

import java.lang.reflect.Type;
import java.util.ArrayList;

public interface Resource extends JsonSerializable {

    ResourceId getResourceId();

    String getClassName();

    Object get();

    <T> T get(Class<T> clazz);

    boolean isSerializable();

    boolean isRemote();

    boolean isLocal();

    Object invokeMethod(String methodName);

    Resource invokeMethod(String methodName, String returnResourceName);

    Object invokeMethod(String methodName, Object[] params) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, ArrayList params
    ) throws ClassNotFoundException;

    Resource invokeMethod(String methodName, Object[] params, String returnResourceName) throws ClassNotFoundException;

    Resource invokeMethod(
            String methodName, ArrayList params, String returnResourceName
    ) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, String[] paramTypes, Object[] params
    ) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, ArrayList<String> paramTypes, ArrayList params
    ) throws ClassNotFoundException;

    Resource invokeMethod(
            String methodName, String[] paramTypes, Object[] params, String returnResourceName
    ) throws ClassNotFoundException;

    Resource invokeMethod(
            String methodName, ArrayList<String> paramTypes, ArrayList params, String returnResourceName
    ) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, Type[] types, Object[] params
    ) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, Type[] types, Object[] params, String returnResourceName
    ) throws ClassNotFoundException;

    Object invokeMethod(
            String methodName, Class[] paramTypes, Object[] params
    );

    Resource invokeMethod(
            String methodName, Class[] paramTypes, Object[] params, String returnResourceName
    );

}
