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
package org.apache.calcite.adapter.geode.util;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;

/**
 * Created by tzoloc on 5/11/16.
 */
public class GeodeUtils {

  private GeodeUtils() {
  }

  /**
   * Create Geode client instance connected to locator and configured to support PDX instances.
   * Note if an old instance exists, it will be destroyed and re-created.
   *
   * @param locatorHost               Locator's host address
   * @param locatorPort               Loator's port
   * @param autoSerializerPackagePath package name of the Domain calsses loaded in the regions
   * @return Returns a Gode {@link ClientCache} instance connected to Geode cluster
   */
  public static ClientCache createClientCache(String locatorHost, int locatorPort,
                                              String autoSerializerPackagePath, boolean readSerialized) {
    try {
      ClientCache existing = ClientCacheFactory.getAnyInstance();
      existing.close();
    } catch (CacheClosedException cce) {
      //Do nothing if there is no existing instance
    }

    ClientCache clientCache = new ClientCacheFactory()
        .addPoolLocator(locatorHost, locatorPort)
        .setPdxSerializer(new ReflectionBasedAutoSerializer(autoSerializerPackagePath))
        .setPdxReadSerialized(readSerialized).setPdxPersistent(false).create();

    return clientCache;
  }

  /**
   * Obtain a proxy pointing to an existing Region on the server
   *
   * @param clientCache {@link ClientCache} instance to interact with the Geode server
   * @param regionName  Name of the region to create proxy for.
   * @return Returns are Region proxy to a remote (on the Server) regions.
   */
  public static Region createRegionProxy(ClientCache clientCache, String regionName) {
    return clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
  }

  /**
   * Convert Geode object into Row tuple
   *
   * @param relDataTypeFields - table relation types
   * @param geodeResultObject - Object value returned by Geode query
   * @return List of objects values corresponding to the relDataTypeFields
   */
  public static Object convertToRowValues(
      List<RelDataTypeField> relDataTypeFields, Object geodeResultObject) {

    Object values;

    if (geodeResultObject instanceof Struct) {
      values = handleStructEntry(relDataTypeFields, geodeResultObject);
    } else if (geodeResultObject instanceof PdxInstance) {
      values = handlePdxInstanceEntry(relDataTypeFields, geodeResultObject);
    } else {
      values = handleJavaObjectEntry(relDataTypeFields, geodeResultObject);
    }

    return values;
  }

  private static Object handleStructEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    Struct struct = (Struct) obj;

    if (relDataTypeFields.size() == 1) {
      return struct.get(relDataTypeFields.get(0).getName());
    }

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      values[index++] = struct.get(relDataTypeField.getName());
    }

    return values;
  }

  private static Object handlePdxInstanceEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    PdxInstance pdxEntry = (PdxInstance) obj;

    if (relDataTypeFields.size() == 1) {
      return pdxEntry.getField(relDataTypeFields.get(0).getName());
    }

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      values[index++] =
          pdxEntry.getField(relDataTypeField.getName());
    }
    return values;
  }

  private static Object handleJavaObjectEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {

    Class<?> clazz = obj.getClass();
    if (relDataTypeFields.size() == 1) {
      try {
        Field javaField = clazz.getDeclaredField(relDataTypeFields.get(0).getName());
        javaField.setAccessible(true);
        return javaField.get(obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      try {
        Field javaField = clazz.getDeclaredField(relDataTypeField.getName());
        javaField.setAccessible(true);
        values[index++] = javaField.get(obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return values;
  }

  public static RelDataType createRelDataType(Object regionEntry) {
    JavaTypeFactoryExtImpl typeFactory = new JavaTypeFactoryExtImpl();
    if (regionEntry instanceof PdxInstance) {
      return typeFactory.createPdxType((PdxInstance) regionEntry);
    } else {
      return typeFactory.createStructType(regionEntry.getClass());
    }
  }

}
// End GeodeUtils.java
