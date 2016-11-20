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
package org.apache.calcite.adapter.geode.simple;

import static org.apache.calcite.adapter.geode.util.GeodeUtils.convertToRowValues;

import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.geode.util.JavaTypeFactoryExtImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.geode.cache.client.ClientCache;

/**
 * Created by tzoloc on 5/4/16.
 */
public class GeodeScannableTable extends AbstractTable implements ScannableTable {

  private final RelDataType relDataType;
  private String regionName;
  //private Class<?> regionValueClass;
  private ClientCache clientCache;

  public GeodeScannableTable(String regionName, Class<?> regionValueClass,
                             ClientCache clientCache) {
    super();

    this.regionName = regionName;
    //this.regionValueClass = regionValueClass;
    this.clientCache = clientCache;
    this.relDataType = new JavaTypeFactoryExtImpl().createStructType(regionValueClass);
  }

  @Override public String toString() {
    return "GeodeScannableTable";
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return relDataType;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {

    final List<String> fieldNames = relDataType.getFieldNames();

    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new GeodeEnumerator<Object[]>(clientCache, regionName) {
          @Override public Object[] convert(Object obj) {
            Object values = convertToRowValues(relDataType.getFieldList(), obj);
            if (values instanceof Object[]) {
              return (Object[]) values;
            }
            return new Object[]{values};
          }
        };
      }
    };
  }
}
// End GeodeScannableTable.java
