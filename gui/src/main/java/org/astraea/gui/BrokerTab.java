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
package org.astraea.gui;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.scene.control.Tab;
import org.astraea.common.DataSize;
import org.astraea.common.LinkedHashMap;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.TopicPartition;

public class BrokerTab {

  private static List<LinkedHashMap<String, String>> result(Stream<Broker> brokers) {
    return brokers
        .map(
            broker ->
                LinkedHashMap.of(
                    "hostname",
                    broker.host(),
                    "id",
                    String.valueOf(broker.id()),
                    "port",
                    String.valueOf(broker.port()),
                    "controller",
                    String.valueOf(broker.isController()),
                    "topics",
                    String.valueOf(
                        broker.folders().stream()
                            .flatMap(
                                d ->
                                    d.partitionSizes().keySet().stream().map(TopicPartition::topic))
                            .distinct()
                            .count()),
                    "partitions",
                    String.valueOf(
                        broker.folders().stream()
                            .flatMap(d -> d.partitionSizes().keySet().stream())
                            .distinct()
                            .count()),
                    "leaders",
                    String.valueOf(broker.topicPartitionLeaders().size()),
                    "size",
                    DataSize.Byte.of(
                            broker.folders().stream()
                                .mapToLong(
                                    d ->
                                        d.partitionSizes().values().stream()
                                            .mapToLong(v -> v)
                                            .sum())
                                .sum())
                        .toString()))
        .collect(Collectors.toList());
  }

  public static Tab of(Context context) {
    var pane =
        Utils.searchToTable(
            "broker id/host/port (space means all brokers):",
            (word, console) ->
                context
                    .optionalAdmin()
                    .map(
                        admin ->
                            result(
                                admin.brokers().stream()
                                    .filter(
                                        nodeInfo ->
                                            word.isEmpty()
                                                || String.valueOf(nodeInfo.id()).contains(word)
                                                || nodeInfo.host().contains(word)
                                                || String.valueOf(nodeInfo.port()).contains(word))))
                    .orElse(List.of()));
    var tab = new Tab("broker");
    tab.setContent(pane);
    return tab;
  }
}