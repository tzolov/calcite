/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.geode.stream;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 */
public class GeodeRegionChangeListener extends CacheListenerAdapter implements Declarable {

  private Queue<Object> queue = new ArrayDeque<>();

  @Override public void afterUpdate(EntryEvent event) {
    Object eKey = event.getKey();
    Object eVal = event.getNewValue();
    queue.add(eVal);
  }

  @Override public void afterCreate(EntryEvent event) {
    Object eKey = event.getKey();
    Object eVal = event.getNewValue();
    queue.add(eVal);
  }

  public void clear() {
    queue.clear();
  }

  public void put(Object o) {
    queue.add(o);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public Object poll() {
    return queue.poll();
  }
}

// End GeodeRegionChangeListener.java
