/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.partitioner.bucket;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.astraea.common.csv.CsvWriter;

public class BucketPartitioner implements Partitioner {
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
  private final ConcurrentSkipListMap<String, AtomicInteger> keysRecordCounterMap =
      new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<String, AtomicInteger> bucketRecordCounterMap =
      new ConcurrentSkipListMap<>();
  private final AtomicInteger init = new AtomicInteger(-1);
  private final Map<String, Integer> keyCorrespondingPartitionInWarmUp = new ConcurrentHashMap<>();
  private Map<String, Integer> keyCorrespondingPartition;
  private long startTimeStamp;
  private boolean complete = false;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return overTenSecond()
        ? bucketPartition(topic, key, cluster)
        : warmerPartition(topic, key, keyBytes, cluster);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    startTimeStamp = System.currentTimeMillis();
  }

  @Override
  public void close() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
    Date date = new Date(System.currentTimeMillis());
    String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    String title = path + formatter.format(date) + ".csv";
    System.out.println(title);
    try (CsvWriter csvWriter =
        CsvWriter.builder(org.astraea.common.Utils.packException(() -> new FileWriter(title)))
            .build()) {
      csvWriter.rawAppend(List.of("Key Corresponding in the Warm Up"));
      csvWriter.rawAppend(List.of(""));

      keyCorrespondingPartitionInWarmUp.forEach(
          (key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));

      csvWriter.rawAppend(List.of("Record number in the Warm Up"));
      csvWriter.rawAppend(List.of(""));
      csvWriter.rawAppend(List.of("Key", "Count"));
      keysRecordCounterMap.forEach(
          (key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
      csvWriter.rawAppend(List.of());
      csvWriter.rawAppend(List.of("Key Corresponding Partition"));
      keyCorrespondingPartition.forEach(
          (key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
      csvWriter.rawAppend(List.of());
      csvWriter.rawAppend(List.of("Record number after the Warm Up"));
      bucketRecordCounterMap.forEach(
          (key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
    }
  }

  private boolean overTenSecond() {
    if (!complete && System.currentTimeMillis() - startTimeStamp > 10000) {
      complete = true;
    }
    return complete;
  }

  private int bucketPartition(String topic, Object key, Cluster cluster) {
    if (init.get() < 0) {
      synchronized (init) {
        if (init.get() < 0) {
          var availablePartitionsForTopic =
              cluster.availablePartitionsForTopic(topic).stream()
                  .map(PartitionInfo::partition)
                  .collect(Collectors.toList());
          var keyCost =
              new ArrayList<>(
                  keysRecordCounterMap.entrySet().stream()
                      .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()))
                      .entrySet());
          keyCost.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

          var keys = keyCost.iterator();
          var countCost =
              new ArrayList<>(
                  availablePartitionsForTopic.stream()
                      .collect(Collectors.toMap(pa -> pa, ignore -> 0))
                      .entrySet());
          var countCostIterator = countCost.iterator();

          HashMap<String, Integer> keyPartition = new HashMap<>();

          while (keys.hasNext()) {
            if (countCostIterator.hasNext()) {
              var keyNext = keys.next();
              Map.Entry<Integer, Integer> partitionNext = countCostIterator.next();
              partitionNext.setValue(partitionNext.getValue() + keyNext.getValue());
              keyPartition.put(keyNext.getKey(), partitionNext.getKey());
            } else {
              countCost.sort(Map.Entry.comparingByValue());
              var keyNext = keys.next();
              Map.Entry<Integer, Integer> head = countCost.get(0);
              head.setValue(head.getValue() + keyNext.getValue());
              keyPartition.put(keyNext.getKey(), head.getKey());
            }
          }
          keyCorrespondingPartition = Collections.unmodifiableMap(keyPartition);
          init.incrementAndGet();
        }
      }
    }
    bucketRecordCounterMap
        .computeIfAbsent(nullProcess(key), k -> new AtomicInteger(0))
        .incrementAndGet();
    return keyCorrespondingPartition.get(nullProcess(key));
  }

  private int warmerPartition(String topic, Object key, byte[] serializedKey, Cluster cluster) {
    keysRecordCounterMap
        .computeIfAbsent(nullProcess(key), k -> new AtomicInteger(0))
        .incrementAndGet();
    int target =
        Utils.toPositive(serializedKey == null ? 0 : murmur2(serializedKey))
            % cluster.partitionsForTopic(topic).size();
    keyCorrespondingPartitionInWarmUp.putIfAbsent(nullProcess(key), target);
    return target;
  }

  private String nullProcess(Object key) {
    return String.valueOf(key) == null ? "0" : String.valueOf(key);
  }

  public static int murmur2(final byte[] data) {
    int length = data.length;
    int seed = 0x9747b28c;
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    final int m = 0x5bd1e995;
    final int r = 24;

    // Initialize the hash to a random value
    int h = seed ^ length;
    int length4 = length / 4;

    for (int i = 0; i < length4; i++) {
      final int i4 = i * 4;
      int k =
          (data[i4 + 0] & 0xff)
              + ((data[i4 + 1] & 0xff) << 8)
              + ((data[i4 + 2] & 0xff) << 16)
              + ((data[i4 + 3] & 0xff) << 24);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // Handle the last few bytes of the input array
    switch (length % 4) {
      case 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16;
      case 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8;
      case 1:
        h ^= data[length & ~3] & 0xff;
        h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}
