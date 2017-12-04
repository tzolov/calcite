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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.geode.cache.client.ClientCache;

import com.google.common.collect.ImmutableList;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class GeodeStreamScannableTable extends AbstractTable implements ScannableTable,
    StreamableTable {

  private final RelDataType relDataType;
  private GeodeRegionChangeListener regionListener;
  private String regionName;
  private ClientCache clientCache;

  public GeodeStreamScannableTable(String regionName, RelDataType relDataType,
      ClientCache clientCache, GeodeRegionChangeListener regionListener) {
    super();

    this.regionName = regionName;
    this.clientCache = clientCache;
    this.relDataType = relDataType;
    this.regionListener = regionListener;
  }

  @Override public Statistic getStatistic() {
    return Statistics.of(100d,
        ImmutableList.<ImmutableBitSet>of(),
        RelCollations.createSingleton(0));
  }

  @Override public String toString() {
    return "GeodeSimpleScannableTable";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return relDataType;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new GeodeStreamEnumerator(regionListener, cancelFlag, relDataType);
      }
    };
  }

  @Override public Table stream() {
    return this;
  }
}
// End GeodeStreamScannableTable.java
