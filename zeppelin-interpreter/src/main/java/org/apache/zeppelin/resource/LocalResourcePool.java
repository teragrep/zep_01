/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.resource;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * ResourcePool
 */
public class LocalResourcePool implements ResourcePool {
  private final String resourcePoolId;
  private final Map<ResourceId, Resource> resources = Collections.synchronizedMap(
      new HashMap<ResourceId, Resource>());
  private static final int maximumObjectSize = ZeppelinConfiguration.create().getResourcePoolMaximumObjectSize();
  private static final int maximumResourcesCount = ZeppelinConfiguration.create().getResourcePoolMaximumObjectCount();
  private static final int maximumKeyLength = 255; // Hardcoded and unconfigurable by choice, this should never be reached
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalResourcePool.class);
  /**
   * @param id unique id
   */
  public LocalResourcePool(String id) {
    resourcePoolId = id;
  }

  /**
   * Get unique id of this resource pool
   *
   * @return
   */
  @Override
  public String id() {
    return resourcePoolId;
  }

  /**
   * Get resource
   *
   * @return null if resource not found
   */
  @Override
  public Resource get(String name) {
    ResourceId resourceId = new ResourceId(resourcePoolId, name);
    return resources.get(resourceId);
  }

  @Override
  public Resource get(String noteId, String paragraphId, String name) {
    ResourceId resourceId = new ResourceId(resourcePoolId, noteId, paragraphId, name);
    return resources.get(resourceId);
  }

  @Override
  public ResourceSetImpl getAll() {
    return new ResourceSetImpl(resources.values());
  }

  /**
   * Put resource into the pull
   *
   * @param
   * @param object object to put into the resource
   */
  @Override
  public void put(String name, Object object) {
    checkSize(name, object);
    ResourceId resourceId = new ResourceId(resourcePoolId, name);

    Resource resource = new Resource(this, resourceId, object);
    resources.put(resourceId, resource);
  }

  @Override
  public void put(String noteId, String paragraphId, String name, Object object) {
    checkSize(name, object);
    ResourceId resourceId = new ResourceId(resourcePoolId, noteId, paragraphId, name);

    Resource resource = new Resource(this, resourceId, object);
    resources.put(resourceId, resource);
  }

  @Override
  public Resource remove(String name) {
    return resources.remove(new ResourceId(resourcePoolId, name));
  }

  @Override
  public Resource remove(String noteId, String paragraphId, String name) {
    return resources.remove(new ResourceId(resourcePoolId, noteId, paragraphId, name));
  }

  // Check if we are approaching resource storage limits
  private void checkSize(String name, Object object) {
    int objectSize = object.toString().length();
    if (name.length() > maximumKeyLength) {
      throw new ResourcePoolManipulationException("Resource '" + name.substring(0, 16) + "...' can't be added: Name is too long (size " + name.length() + ", maximum " + maximumKeyLength + ")");
    }
    LOGGER.debug("Registering object " + name + " with size of " + objectSize);
    if(objectSize > maximumObjectSize) {
      throw new ResourcePoolManipulationException("Resource " + name + " can't be added: Resource is too big (size " + objectSize + ", maximum " + maximumObjectSize + ")\n" +
              "Edit zeppelin.resourcepool.max.object.size in zeppelin-site.xml to increase the limits.");
    }
    if(resources.size() >= maximumResourcesCount) {
      throw new ResourcePoolManipulationException("Resource " + name + " can't be added: Too many resources stored, maximum " + maximumResourcesCount + " objects)\n" +
              "Edit zeppelin.resourcepool.max.object.count in zeppelin-site.xml to increase the limits.");
    }
  }
}
