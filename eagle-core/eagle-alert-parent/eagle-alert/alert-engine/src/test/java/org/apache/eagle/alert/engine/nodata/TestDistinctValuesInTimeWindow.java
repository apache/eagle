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
package org.apache.eagle.alert.engine.nodata;

import org.apache.eagle.alert.engine.evaluator.nodata.DistinctValuesInTimeWindow;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Since 6/28/16.
 */
public class TestDistinctValuesInTimeWindow {
    @Test
    public void test() {
        DistinctValuesInTimeWindow window = new DistinctValuesInTimeWindow(60 * 1000);
        window.send("1", 0);
        window.send("2", 1000);
        window.send("3", 1000);
        window.send("1", 30000);
        window.send("2", 50000);
        window.send("1", 62000);
        Map<Object, Long> values = window.distinctValues();
        System.out.println(values);
    }

    @Test
    public void testSort() {
        SortedMap<DistinctValuesInTimeWindow.ValueAndTime, DistinctValuesInTimeWindow.ValueAndTime> timeSortedMap =
            new TreeMap<>(new DistinctValuesInTimeWindow.ValueAndTimeComparator());
        DistinctValuesInTimeWindow.ValueAndTime vt1 = new DistinctValuesInTimeWindow.ValueAndTime("1", 0);
        timeSortedMap.put(vt1, vt1);
        DistinctValuesInTimeWindow.ValueAndTime vt2 = new DistinctValuesInTimeWindow.ValueAndTime("2", 1000);
        timeSortedMap.put(vt2, vt2);
        DistinctValuesInTimeWindow.ValueAndTime vt3 = new DistinctValuesInTimeWindow.ValueAndTime("3", 1000);
        timeSortedMap.put(vt3, vt3);
        timeSortedMap.remove(new DistinctValuesInTimeWindow.ValueAndTime("1", 0));
        DistinctValuesInTimeWindow.ValueAndTime vt4 = new DistinctValuesInTimeWindow.ValueAndTime("1", 30000);
        timeSortedMap.put(vt4, vt4);
        Iterator<?> it = timeSortedMap.entrySet().iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        timeSortedMap.remove(new DistinctValuesInTimeWindow.ValueAndTime("2", 1000));
        DistinctValuesInTimeWindow.ValueAndTime vt5 = new DistinctValuesInTimeWindow.ValueAndTime("2", 50000);
        timeSortedMap.put(vt5, vt5);
        DistinctValuesInTimeWindow.ValueAndTime vt6 = new DistinctValuesInTimeWindow.ValueAndTime("1", 62000);
        timeSortedMap.put(vt6, vt6);
        it = timeSortedMap.entrySet().iterator();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
    }
}
