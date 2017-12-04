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

import org.apache.calcite.adapter.geode.util.GeodeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.util.CacheListenerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enumerator that reads from a Geode Regions.
 */
class GeodeStreamEnumerator extends CacheListenerAdapter implements Enumerator<Object[]>,
    Declarable {

  protected static final Logger LOGGER =
      LoggerFactory.getLogger(GeodeStreamEnumerator.class.getName());

  public static final boolean IS_STREAM = true;

  private Object current;
  private List<RelDataTypeField> fieldTypes;
  private GeodeRegionChangeListener regionChangeListener;
  private AtomicBoolean cancelFlag;

  /**
   * Creates a GeodeEnumerator.
   */
  GeodeStreamEnumerator(GeodeRegionChangeListener regionChangeListener, AtomicBoolean cancelFlag,
      RelDataType relDataType) {
    this.regionChangeListener = regionChangeListener;
    this.cancelFlag = cancelFlag;
    this.current = null;

    this.fieldTypes = relDataType.getFieldList();
  }

  /**
   * Produce the next row from the results
   *
   * @return A rel row from the results
   */
  @Override public Object[] current() {
    return (Object[]) GeodeUtils.convertToRowValues(fieldTypes, current, IS_STREAM);
  }

  @Override public boolean moveNext() {
    if (cancelFlag.get()) {
      return false;
    }

    while (this.regionChangeListener.isEmpty() && !cancelFlag.get()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    current = regionChangeListener.poll();
    if (current == null) {
      LOGGER.warn("current is Null. Should have been caught by the regionChangeListener.isEmpty()");
      return false;
    }

    return true;
  }

  @Override public void reset() {
    this.regionChangeListener.clear();
  }

  @Override public void close() {
    // Nothing to do here
    this.regionChangeListener.clear();
  }
}
// End GeodeStreamEnumerator.java
