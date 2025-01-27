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
package org.astraea.common.metrics.collector;

import java.util.Collection;
import java.util.Map;
import org.astraea.common.metrics.HasBeanObject;

@FunctionalInterface
public interface MetricSensor {

  /**
   * @param identity broker id or producer/consumer id
   * @param beans a collection of {@link HasBeanObject}
   * @return The collection of "HasBeanObject" generated after the custom statistical method of
   *     CostFunction.
   */
  Map<Integer, Collection<? extends HasBeanObject>> record(
      int identity, Collection<? extends HasBeanObject> beans);
}
