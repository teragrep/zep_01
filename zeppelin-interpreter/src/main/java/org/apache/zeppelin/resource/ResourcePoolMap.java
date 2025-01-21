package org.apache.zeppelin.resource;

import org.apache.zeppelin.interpreter.xref.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/*
 * Used by StringSubstitutor to receive data from resourcePool
 */
public class ResourcePoolMap implements Map<String, String> {
    private static final String exceptionMessage = "Only get is supported.";
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourcePoolMap.class);
    private final ResourcePool resourcePool;
    public ResourcePoolMap(ResourcePool resourcePool) {
        this.resourcePool = resourcePool;
    }

    @Override
    public String get(Object key)  {
        LOGGER.debug("Querying for key for StringSubstitution: " + key.toString());
        Resource resource = resourcePool.get(key.toString());
        LOGGER.debug("Received value " + resource.get() + " for key " + key.toString());
        if(resource.get() == null) {
            throw new IllegalArgumentException("Resource " + key + " is null");
        }
        return resource.get().toString();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public boolean containsKey(Object o) {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public String put(String s, String s2) {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public String remove(Object o) {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> map) {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException(exceptionMessage);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException(exceptionMessage);
    }
}
