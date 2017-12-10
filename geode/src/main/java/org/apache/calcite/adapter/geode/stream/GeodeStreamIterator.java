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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class GeodeStreamIterator implements Iterator<Object[]> {

  public static final boolean IS_STREAM = true;
  private final List<RelDataTypeField> fieldTypes;
  private final GeodeRegionChangeListener regionChangeListener;
  private final AtomicBoolean cancelFlag;
  private final RelDataType relDataType;

  public GeodeStreamIterator(GeodeRegionChangeListener regionChangeListener, AtomicBoolean
      cancelFlag, RelDataType relDataType) {
    this.regionChangeListener = regionChangeListener;
    this.cancelFlag = cancelFlag;
    this.relDataType = relDataType;
    this.fieldTypes = relDataType.getFieldList();
  }

  @Override public boolean hasNext() {
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

    return true;
  }

  @Override public Object[] next() {
    Object[] next = (Object[]) GeodeUtils.convertToRowValues(fieldTypes,
        this.regionChangeListener.poll(), IS_STREAM);
    return next;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException();
  }
}

// End GeodeStreamIterator.java
