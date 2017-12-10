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

import org.apache.calcite.adapter.geode.stream.GeodeRegionChangeListener;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Util;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class GeodeUtils {

  protected static final Logger LOGGER = LoggerFactory.getLogger(GeodeUtils.class.getName());

  /**
   * Cache for the client proxy regions created in the current ClientCache.
   */
  private static Map<String, Region> regionMap = new ConcurrentHashMap();

  private static String currentLocatorHost = "";
  private static int currentLocatorPort = -1;

  private GeodeUtils() {
  }

  /**
   * Create Geode client instance connected to locator and configured to support PDX instances.
   * Note if an old instance exists, it will be destroyed and re-created.
   *
   * @param locatorHost               Locator's host address
   * @param locatorPort               Locator's port
   * @param autoSerializerPackagePath package name of the Domain classes loaded in the regions
   * @return Returns a Gode {@link ClientCache} instance connected to Geode cluster
   */
  public static synchronized ClientCache createClientCache(
      String locatorHost, int locatorPort,
      String autoSerializerPackagePath,
      boolean readSerialized,
      boolean subscriptionEnabled) {
    if (locatorPort != currentLocatorPort
        || !StringUtils.equalsIgnoreCase(currentLocatorHost, locatorHost)) {
      LOGGER.info("Close existing ClientCache ["
          + currentLocatorHost + ":" + currentLocatorPort + "] for new Locator connection at: ["
          + locatorHost + ":" + locatorPort + "]");
      currentLocatorHost = locatorHost;
      currentLocatorPort = locatorPort;
      closeClientCache();
    }

    try {
      // If exists returns the existing client cache. This requires that the pre-created
      // client proxy regions can also be resolved from the regionMap
      return ClientCacheFactory.getAnyInstance();
    } catch (CacheClosedException cce) {
      //Do nothing if there is no existing instance
    }

    ClientCache clientCache = new ClientCacheFactory()
        .addPoolLocator(locatorHost, locatorPort)
        .setPdxSerializer(new ReflectionBasedAutoSerializer(autoSerializerPackagePath))
        .setPdxReadSerialized(readSerialized)
        .setPoolSubscriptionEnabled(subscriptionEnabled)
        .setPdxPersistent(false)
        .create();

    return clientCache;
  }

  public static synchronized void closeClientCache() {
    try {
      ClientCacheFactory.getAnyInstance().close();
    } catch (CacheClosedException cce) {
      //Do nothing if there is no existing instance
    }
    regionMap.clear();
  }

  /**
   * Obtain a proxy pointing to an existing Region on the server
   *
   * @param clientCache {@link ClientCache} instance to interact with the Geode server
   * @param regionName  Name of the region to create proxy for.
   * @return Returns are Region proxy to a remote (on the Server) regions.
   */
  public static synchronized Region createRegionProxy(ClientCache clientCache, String regionName) {
    Region region = regionMap.get(regionName);
    if (region == null) {
      region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(regionName);
      regionMap.put(regionName, region);
    }
    return region;
  }

  public static synchronized Region createRegionProxy(
      ClientCache clientCache, String regionName, GeodeRegionChangeListener regionChangeListener) {

    Region region = regionMap.get(regionName);
    if (region == null) {
      region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .addCacheListener(regionChangeListener)
          .create(regionName);
      regionMap.put(regionName, region);
    }
    return region;
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

    return convertToRowValues(relDataTypeFields, geodeResultObject, false);
  }

  public static Object convertToRowValues(
      List<RelDataTypeField> relDataTypeFields, Object geodeResultObject, boolean stream) {

    Object values;

    if (geodeResultObject instanceof Struct) {
      values = handleStructEntry(relDataTypeFields, geodeResultObject, stream);
    } else if (geodeResultObject instanceof PdxInstance) {
      values = handlePdxInstanceEntry(relDataTypeFields, geodeResultObject, stream);
    } else {
      values = handleJavaObjectEntry(relDataTypeFields, geodeResultObject);
    }

    return values;
  }

  private static JavaTypeFactoryExtImpl javaTypeFactory = new JavaTypeFactoryExtImpl();

  private static Object handleStructEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {
    return handleStructEntry(relDataTypeFields, obj, false);
  }

  private static Object handleStructEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj, boolean stream) {

    Struct struct = (Struct) obj;

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;

    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      Type javaType = javaTypeFactory.getJavaClass(relDataTypeField.getType());
      Object rawValue = null;
//      if (stream && index == 0) {
//        values[0] = System.currentTimeMillis();
//      } else {

      try {
        rawValue = struct.get(relDataTypeField.getName());
      } catch (IllegalArgumentException e) {
        rawValue = "<error>";
        System.err.println("Could find field : " + relDataTypeField.getName());
        e.printStackTrace();
      }
//      }
      values[index++] = convert(rawValue, (Class) javaType);
    }

    if (values.length == 1) {
      return values[0];
    }

    return values;
  }

  private static Object handlePdxInstanceEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj, boolean stream) {

    PdxInstance pdxEntry = (PdxInstance) obj;

    Object[] values = new Object[relDataTypeFields.size()];

    int index = 0;
    for (RelDataTypeField relDataTypeField : relDataTypeFields) {
      Type javaType = javaTypeFactory.getJavaClass(relDataTypeField.getType());
      Object rawValue;
//      if (stream && index == 0) {
//        rawValue = System.currentTimeMillis();
//      } else {
      rawValue = pdxEntry.getField(relDataTypeField.getName());
//      }
      values[index++] = convert(rawValue, (Class) javaType);
    }

    if (values.length == 1) {
      return values[0];
    }

    return values;
  }

  private static Object handlePdxInstanceEntry(
      List<RelDataTypeField> relDataTypeFields, Object obj) {
    return handlePdxInstanceEntry(relDataTypeFields, obj, false);
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

  private static Object convert(Object o, Class clazz) {
    if (o == null) {
      return null;
    }
    Primitive primitive = Primitive.of(clazz);
    if (primitive != null) {
      clazz = primitive.boxClass;
    } else {
      primitive = Primitive.ofBox(clazz);
    }
    if (clazz == null) {
      //!!! This is in case of nested Objects!!!
      if (o instanceof PdxInstance) {
        return Util.toString(
            ((PdxInstance) o).getFieldNames(), "PDX[", ",", "]");
      }
      return o.toString();
    }
    if (clazz.isInstance(o)) {
      return o;
    }
    if (o instanceof Date && primitive != null) {
      o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
    }
    if (o instanceof Number && primitive != null) {
      return primitive.number((Number) o);
    }
    return o;
  }

  // Create Relational Type by inferring a Geode entry or response instance.
  public static RelDataType createRelDataType(Object regionEntry) {
    return createRelDataType(regionEntry, false);
  }

  public static RelDataType createRelDataType(Object regionEntry, boolean stream) {
    JavaTypeFactoryExtImpl typeFactory = new JavaTypeFactoryExtImpl();
    if (regionEntry instanceof PdxInstance) {
      return typeFactory.createPdxType((PdxInstance) regionEntry, stream);
    } else {
      return typeFactory.createStructType(regionEntry.getClass(), stream);
    }
  }

}
// End GeodeUtils.java
