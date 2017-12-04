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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 */
public class JsonLoader {

  private ClientCache cache;
  private String rootPackage;
  private Region region;
  private ObjectMapper mapper;
  private Random random;

  public JsonLoader(ClientCache cache, String regionName, String rootPackage) {
    this.cache = cache;
    this.rootPackage = rootPackage;
    this.region = cache.getRegion(regionName);
    this.mapper = new ObjectMapper();
    this.random = new Random();
  }


  public void load(String jsonPath) throws IOException {

    InputStreamReader isr = new InputStreamReader(new FileInputStream(jsonPath), "UTF-8");
    try (BufferedReader br = new BufferedReader(isr)) {
      int key = 0;
      for (String line; (line = br.readLine()) != null;) {
        Map jsonMap = mapper.readValue(line, Map.class);
        PdxInstance pdxInstance = mapToPdx(rootPackage, jsonMap);

        region.put(key++, pdxInstance);
      }
    }
  }

  public void generateBookMasterEntries(int count, int startKey, long waitInterval)
      throws IOException {


    for (int i = 0; i < count; i++) {
      Map jsonMap = new HashMap();
      jsonMap.put("itemNumber", random.nextInt(10) + 1);
      jsonMap.put("retailCost", random.nextDouble() * 20);
      jsonMap.put("description", "Description " + random.nextInt(1000));
      jsonMap.put("yearPublished", random.nextInt(10) + 2000);
      jsonMap.put("author", "Author " + random.nextInt(1000));
      jsonMap.put("title", "Title " + random.nextInt(1000));

      PdxInstance pdxInstance = mapToPdx(rootPackage, jsonMap);

      try {
        Thread.sleep(waitInterval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      region.put(startKey + i, pdxInstance);
    }
  }

  private PdxInstance mapToPdx(String packageName, Map<String, Object> map) {
    PdxInstanceFactory pdxBuilder = cache.createPdxInstanceFactory(packageName);

    for (String name : map.keySet()) {
      Object value = map.get(name);

      if (value instanceof Map) {
        pdxBuilder.writeObject(name, mapToPdx(packageName + "." + name, (Map) value));
      } else {
        pdxBuilder.writeObject(name, value);
      }
    }

    return pdxBuilder.create();
  }

  public static void printJson(Object value) {
    try {
      System.out.println(new ObjectMapper().writeValueAsString(value));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }
}

// End JsonLoader.java
