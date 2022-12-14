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

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.astraea.common.Configuration;
import org.astraea.common.csv.CsvWriter;
import org.astraea.fs.FileSystem;

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

public class BucketPartitioner implements Partitioner {
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();
  private final ConcurrentSkipListMap<String, AtomicInteger> keysRecordCounterMap =
      new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<String, AtomicInteger> bucketRecordCounterMap =
          new ConcurrentSkipListMap<>();
  private final AtomicInteger init = new AtomicInteger(-1);
  private Map<String, Integer> keyCorrespondingPartition;
  private long startTimeStamp;
  private boolean complete = false;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return overTenSecond()
        ? bucketPartition(topic, key, cluster)
        : warmerPartition(topic, key, cluster);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    startTimeStamp = System.currentTimeMillis();
  }

  @Override
  public void close() {
    try(FileSystem local = FileSystem.of("local", Configuration.EMPTY)) {
      SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
      Date date = new Date(System.currentTimeMillis());
      String title = formatter.format(date) + ".csv";
      local.mkdir(title);
      try (CsvWriter csvWriter = CsvWriter.builder(org.astraea.common.Utils.packException(() -> new FileWriter(title))).build()){
          csvWriter.rawAppend(List.of("Record number in the Warm Up"));
          csvWriter.rawAppend(List.of(""));
          csvWriter.rawAppend(List.of("Key","Count"));
          keysRecordCounterMap.forEach((key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
          csvWriter.rawAppend(List.of());
          csvWriter.rawAppend(List.of("Key Corresponding Partition"));
          keyCorrespondingPartition.forEach((key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
          csvWriter.rawAppend(List.of());
          csvWriter.rawAppend(List.of("Record number after the Warm Up"));
          bucketRecordCounterMap.forEach((key, value) -> csvWriter.rawAppend(List.of(key, String.valueOf(value))));
        }
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
            }
             else {
              countCost.sort(Map.Entry.comparingByValue());
              var keyNext = keys.next();
              Map.Entry<Integer, Integer> head = countCost.get(0);
              head.setValue(head.getValue()+keyNext.getValue());
              keyPartition.put(keyNext.getKey(), head.getKey());
            }
          }
          keyCorrespondingPartition = Collections.unmodifiableMap(keyPartition);
          init.incrementAndGet();
        }
      }
    }
    bucketRecordCounterMap.computeIfAbsent(String.valueOf(key), k -> new AtomicInteger(0))
            .incrementAndGet();
    return keyCorrespondingPartition.get(String.valueOf(key));
  }

  private int warmerPartition(String topic, Object key, Cluster cluster) {
    keysRecordCounterMap
        .computeIfAbsent(String.valueOf(key), k -> new AtomicInteger(0))
        .incrementAndGet();

    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    int nextValue = nextValue(topic);
    List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
    if (!availablePartitions.isEmpty()) {
      int part = Utils.toPositive(nextValue) % availablePartitions.size();
      return availablePartitions.get(part).partition();
    } else {
      // no partitions are available, give a non-available partition
      return Utils.toPositive(nextValue) % numPartitions;
    }
  }

  private int nextValue(String topic) {
    AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> new AtomicInteger(0));
    return counter.getAndIncrement();
  }
}
