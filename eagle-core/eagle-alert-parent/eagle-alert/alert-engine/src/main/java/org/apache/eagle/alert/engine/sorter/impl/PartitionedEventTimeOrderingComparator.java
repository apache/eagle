/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.sorter.impl;

import org.apache.eagle.alert.engine.model.PartitionedEvent;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * TODO: Stable sorting algorithm for better performance to avoid event resorting with same timestamp?.
 */
public class PartitionedEventTimeOrderingComparator implements Comparator<PartitionedEvent>, Serializable {
    public static final PartitionedEventTimeOrderingComparator INSTANCE = new PartitionedEventTimeOrderingComparator();
    private static final long serialVersionUID = 7692921764576899926L;

    @Override
    public int compare(PartitionedEvent o1, PartitionedEvent o2) {
        if (Objects.equals(o1, o2)) {
            return 0;
        } else {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 != null && o2 == null) {
                return 1;
            } else if (o1 == null) {
                return -1;
            }
            // Unstable Sorting Algorithm
            if (o1.getTimestamp() <= o2.getTimestamp()) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}