/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.perf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.FilenameUtils;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Since 5/13/16.
 */
public class TestSerDeserPer {
    Object[] data = null;

    @Before
    public void before() {
        int max = 100;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < max; i++) {
            sb.append("a");
        }
        data = new Object[] {sb.toString()};
    }

    private String getTmpPath() {
        return System.getProperty("java.io.tmpdir");
    }

    @Test
    public void testSerDeserPerf() throws Exception {
        Kryo kryo = new Kryo();
        String outputPath = FilenameUtils.concat(getTmpPath(), "file.bin");
        Output output = new Output(new FileOutputStream(outputPath));
        for (int i = 0; i < 1000; i++) {
            kryo.writeObject(output, constructPE());
        }
        output.close();
        Input input = new Input(new FileInputStream(outputPath));
        PartitionedEvent someObject = kryo.readObject(input, PartitionedEvent.class);
        input.close();
        Assert.assertTrue(someObject.getData().length == 1);
    }

    private PartitionedEvent constructPE() {
        StreamEvent e = new StreamEvent();
        e.setStreamId("testStreamId");
        e.setTimestamp(1463159382000L);
        e.setData(data);
        StreamPartition sp = new StreamPartition();
        List<String> col = new ArrayList<>();
        col.add("host");
        sp.setColumns(col);
        StreamSortSpec sortSpec = new StreamSortSpec();
        sortSpec.setWindowMargin(30000);
        sortSpec.setWindowPeriod("PT1M");
        sp.setSortSpec(sortSpec);
        sp.setStreamId("testStreamId");
        sp.setType(StreamPartition.Type.GROUPBY);
        PartitionedEvent pe = new PartitionedEvent();
        pe.setEvent(e);
        pe.setPartition(sp);
        pe.setPartitionKey(1000);
        return pe;
    }

    @Test
    public void testSerDeserPerf2() throws Exception {
        Kryo kryo = new Kryo();
        String outputPath = FilenameUtils.concat(getTmpPath(), "file2.bin");
        Output output = new Output(new FileOutputStream(outputPath));
        for (int i = 0; i < 1000; i++) {
            kryo.writeObject(output, constructNewPE());
        }
        output.close();
        Input input = new Input(new FileInputStream(outputPath));
        NewPartitionedEvent someObject = kryo.readObject(input, NewPartitionedEvent.class);
        input.close();
        Assert.assertTrue(someObject.getData().length == 1);
    }

    private NewPartitionedEvent constructNewPE() {
        NewPartitionedEvent pe = new NewPartitionedEvent();
        pe.setStreamId("testStreamId");
        pe.setTimestamp(1463159382000L);
        pe.setData(data);

        pe.setType(StreamPartition.Type.GROUPBY);
        List<String> col = new ArrayList<>();
        col.add("host");
        pe.setColumns(col);
        pe.setPartitionKey(1000);

        pe.setWindowMargin(30000);
        pe.setWindowPeriod("PT1M");
        return pe;
    }

    @Test
    public void testSerDeserPerf3() throws Exception {
        Kryo kryo = new Kryo();
        String outputPath = FilenameUtils.concat(getTmpPath(), "file3.bin");
        Output output = new Output(new FileOutputStream(outputPath));
        for (int i = 0; i < 1000; i++) {
            kryo.writeObject(output, constructNewPE2());
        }
        output.close();
        Input input = new Input(new FileInputStream(outputPath));
        NewPartitionedEvent2 someObject = kryo.readObject(input, NewPartitionedEvent2.class);
        input.close();
        Assert.assertTrue(someObject.getData().length == 1);
    }

    private NewPartitionedEvent2 constructNewPE2() {
        NewPartitionedEvent2 pe = new NewPartitionedEvent2();
        pe.setStreamId(100);
        pe.setTimestamp(1463159382000L);
        pe.setData(data);

        pe.setType(1);
        int[] col = new int[1];
        col[0] = 1;
        pe.setColumns(col);
        pe.setPartitionKey(1000);

        pe.setWindowMargin(30000);
        pe.setWindowPeriod(60);
        return pe;
    }

    public static class NewPartitionedEvent implements Serializable {
        private static final long serialVersionUID = -3840016190614238593L;
        // basic
        private String streamId;
        private long timestamp;
        private Object[] data;

        // stream partition
        private StreamPartition.Type type;
        private List<String> columns = new ArrayList<>();
        private long partitionKey;

        // sort spec
        private String windowPeriod = "";
        private long windowMargin = 30 * 1000;

        public NewPartitionedEvent() {
        }

        public String getStreamId() {
            return streamId;
        }

        public void setStreamId(String streamId) {
            this.streamId = streamId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Object[] getData() {
            return data;
        }

        public void setData(Object[] data) {
            this.data = data;
        }

        public StreamPartition.Type getType() {
            return type;
        }

        public void setType(StreamPartition.Type type) {
            this.type = type;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public long getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(long partitionKey) {
            this.partitionKey = partitionKey;
        }

        public String getWindowPeriod() {
            return windowPeriod;
        }

        public void setWindowPeriod(String windowPeriod) {
            this.windowPeriod = windowPeriod;
        }

        public long getWindowMargin() {
            return windowMargin;
        }

        public void setWindowMargin(long windowMargin) {
            this.windowMargin = windowMargin;
        }
    }

    public static class NewPartitionedEvent2 implements Serializable {
        private static final long serialVersionUID = -3840016190614238593L;
        // basic
        private int streamId;
        private long timestamp;
        private Object[] data;

        // stream partition
        private int type;
        private int[] columns;
        private long partitionKey;

        // sort spec
        private long windowPeriod;
        private long windowMargin = 30 * 1000;

        public NewPartitionedEvent2() {
        }

        public int getStreamId() {
            return streamId;
        }

        public void setStreamId(int streamId) {
            this.streamId = streamId;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public int[] getColumns() {
            return columns;
        }

        public void setColumns(int[] columns) {
            this.columns = columns;
        }

        public long getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(long partitionKey) {
            this.partitionKey = partitionKey;
        }

        public long getWindowPeriod() {
            return windowPeriod;
        }

        public void setWindowPeriod(long windowPeriod) {
            this.windowPeriod = windowPeriod;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public Object[] getData() {
            return data;
        }

        public void setData(Object[] data) {
            this.data = data;
        }

        public long getWindowMargin() {
            return windowMargin;
        }

        public void setWindowMargin(long windowMargin) {
            this.windowMargin = windowMargin;
        }
    }
}
