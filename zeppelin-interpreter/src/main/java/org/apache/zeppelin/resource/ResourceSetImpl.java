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

import com.google.gson.Gson;

import java.util.Collection;
import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * List of resources
 */
public class ResourceSetImpl extends LinkedList<Resource> implements ResourceSet {

  private static final Gson gson = new Gson();

  public ResourceSetImpl(Collection<Resource> resources) {
    super(resources);
  }

  public ResourceSetImpl() {
    super();
  }

  @Override
  public ResourceSet filterByNameRegex(String regex) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (Pattern.matches(regex, r.getResourceId().getName())) {
        result.add(r);
      }
    }
    return result;
  }

  @Override
  public ResourceSet filterByName(String name) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (r.getResourceId().getName().equals(name)) {
        result.add(r);
      }
    }
    return result;
  }

  @Override
  public ResourceSet filterByClassnameRegex(String regex) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (Pattern.matches(regex, r.getClassName())) {
        result.add(r);
      }
    }
    return result;
  }

  @Override
  public ResourceSet filterByClassname(String className) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (r.getClassName().equals(className)) {
        result.add(r);
      }
    }
    return result;
  }

  @Override
  public ResourceSetImpl filterByNoteId(String noteId) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (equals(r.getResourceId().getNoteId(), noteId)) {
        result.add(r);
      }
    }
    return result;
  }

  @Override
  public ResourceSetImpl filterByParagraphId(String paragraphId) {
    ResourceSetImpl result = new ResourceSetImpl();
    for (Resource r : this) {
      if (equals(r.getResourceId().getParagraphId(), paragraphId)) {
        result.add(r);
      }
    }
    return result;
  }

  private boolean equals(String a, String b) {
    if (a == null && b == null) {
      return true;
    } else if (a != null && b != null) {
      return a.equals(b);
    } else {
      return false;
    }
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static ResourceSet fromJson(String json) {
    return gson.fromJson(json, ResourceSetImpl.class);
  }
}
