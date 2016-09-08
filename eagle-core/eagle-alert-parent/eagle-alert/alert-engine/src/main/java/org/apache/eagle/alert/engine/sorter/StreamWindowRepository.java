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
package org.apache.eagle.alert.engine.sorter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.eagle.alert.engine.sorter.impl.StreamSortedWindowInMapDB;
import org.apache.eagle.alert.engine.sorter.impl.StreamSortedWindowOnHeap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * ===== Benchmark Result Report =====<br/><br/>
 * <p>
 * Num. Operation   Type                            Time<br/>
 * ---- ---------   ----                            ----<br/>
 * 1000	FlushTime	DIRECT_MEMORY            	:	55<br/>
 * 1000	FlushTime	FILE_RAF                 	:	63<br/>
 * 1000	FlushTime	MEMORY                   	:	146<br/>
 * 1000	FlushTime	ONHEAP                   	:	17<br/>
 * 1000	InsertTime	DIRECT_MEMORY           	:	68<br/>
 * 1000	InsertTime	FILE_RAF                	:	223<br/>
 * 1000	InsertTime	MEMORY                  	:	273<br/>
 * 1000	InsertTime	ONHEAP                  	:	20<br/>
 * 10000	FlushTime	DIRECT_MEMORY           	:	551<br/>
 * 10000	FlushTime	FILE_RAF                	:	668<br/>
 * 10000	FlushTime	MEMORY                  	:	643<br/>
 * 10000	FlushTime	ONHEAP                  	:	5<br/>
 * 10000	InsertTime	DIRECT_MEMORY          	:	446<br/>
 * 10000	InsertTime	FILE_RAF               	:	2095<br/>
 * 10000	InsertTime	MEMORY                 	:	784<br/>
 * 10000	InsertTime	ONHEAP                 	:	29<br/>
 * 100000	FlushTime	DIRECT_MEMORY          	:	6139<br/>
 * 100000	FlushTime	FILE_RAF               	:	6237<br/>
 * 100000	FlushTime	MEMORY                 	:	6238<br/>
 * 100000	FlushTime	ONHEAP                 	:	18<br/>
 * 100000	InsertTime	DIRECT_MEMORY         	:	4499<br/>
 * 100000	InsertTime	FILE_RAF              	:	22343<br/>
 * 100000	InsertTime	MEMORY                	:	4962<br/>
 * 100000	InsertTime	ONHEAP                	:	107<br/>
 * 1000000	FlushTime	DIRECT_MEMORY         	:	61356<br/>
 * 1000000	FlushTime	FILE_RAF              	:	63025<br/>
 * 1000000	FlushTime	MEMORY                	:	61380<br/>
 * 1000000	FlushTime	ONHEAP                	:	47<br/>
 * 1000000	InsertTime	DIRECT_MEMORY        	:	43637<br/>
 * 1000000	InsertTime	FILE_RAF             	:	464481<br/>
 * 1000000	InsertTime	MEMORY               	:	44367<br/>
 * 1000000	InsertTime	ONHEAP               	:	2040<br/>
 *
 * @see StreamSortedWindowOnHeap
 * @see org.mapdb.DBMaker
 */
public class StreamWindowRepository {
    public enum StorageType {
        /**
         * Creates new in-memory database which stores all data on heap without serialization.
         * This mode should be very fast, but data will affect Garbage PartitionedEventCollector the same way as traditional Java Collections.
         */
        ONHEAP,

        /**
         * Creates new in-memory database. Changes are lost after JVM exits.
         * This option serializes data into {@code byte[]},
         * so they are not affected by Garbage PartitionedEventCollector.
         */
        MEMORY,

        /**
         * <p>
         * Creates new in-memory database. Changes are lost after JVM exits.
         * </p><p>
         * This will use {@code DirectByteBuffer} outside of HEAP, so Garbage Collector is not affected
         * You should increase ammount of direct memory with
         * {@code -XX:MaxDirectMemorySize=10G} JVM param
         * </p>
         */
        DIRECT_MEMORY,

        /**
         * By default use File.createTempFile("streamwindows","temp")
         */
        FILE_RAF
    }

    private final static Logger LOG = LoggerFactory.getLogger(StreamWindowRepository.class);
    private final Map<StorageType, DB> dbPool;

    private StreamWindowRepository() {
        dbPool = new HashMap<>();
    }

    private static StreamWindowRepository repository;

    /**
     * Close automatically when JVM exists
     *
     * @return StreamWindowRepository singletonInstance
     */
    public static StreamWindowRepository getSingletonInstance() {
        synchronized (StreamWindowRepository.class) {
            if (repository == null) {
                repository = new StreamWindowRepository();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        repository.close();
                    }
                });
            }
            return repository;
        }
    }

    private DB createMapDB(StorageType storageType) {
        synchronized (dbPool) {
            if (!dbPool.containsKey(storageType)) {
                DB db;
                switch (storageType) {
                    case ONHEAP:
                        db = DBMaker.heapDB().closeOnJvmShutdown().make();
                        LOG.info("Create ONHEAP mapdb");
                        break;
                    case MEMORY:
                        db = DBMaker.memoryDB().closeOnJvmShutdown().make();
                        LOG.info("Create MEMORY mapdb");
                        break;
                    case DIRECT_MEMORY:
                        db = DBMaker.memoryDirectDB().closeOnJvmShutdown().make();
                        LOG.info("Create DIRECT_MEMORY mapdb");
                        break;
                    case FILE_RAF:
                        try {
                            File file = File.createTempFile("window-", ".map");
                            file.delete();
                            file.deleteOnExit();
                            Preconditions.checkNotNull(file, "file is null");
                            db = DBMaker.fileDB(file).deleteFilesAfterClose().make();
                            LOG.info("Created FILE_RAF map file at {}", file.getAbsolutePath());
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal storage type: " + storageType);
                }
                dbPool.put(storageType, db);
                return db;
            }
            return dbPool.get(storageType);
        }
    }

    public StreamWindow createWindow(long start, long end, long margin, StorageType type) {
        StreamWindow ret;
        switch (type) {
            case ONHEAP:
                ret = new StreamSortedWindowOnHeap(start, end, margin);
                break;
            default:
                ret = new StreamSortedWindowInMapDB(
                    start, end, margin,
                    createMapDB(type),
                    UUID.randomUUID().toString()
                );
                break;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created new {}, type: {}", ret, type);
        }
        return ret;
    }

    public StreamWindow createWindow(long start, long end, long margin, StreamWindowStrategy strategy) {
        return strategy.createWindow(start, end, margin, this);
    }

    public StreamWindow createWindow(long start, long end, long margin) {
        return OnHeapStrategy.INSTANCE.createWindow(start, end, margin, this);
    }

    public void close() {
        for (Map.Entry<StorageType, DB> entry : dbPool.entrySet()) {
            entry.getValue().close();
        }
        dbPool.clear();
    }

    public interface StreamWindowStrategy {
        /**
         * @param start
         * @param end
         * @param margin
         * @return
         */
        StreamWindow createWindow(long start, long end, long margin, StreamWindowRepository repository);
    }

    public static class OnHeapStrategy implements StreamWindowStrategy {
        public static final OnHeapStrategy INSTANCE = new OnHeapStrategy();

        @Override
        public StreamWindow createWindow(long start, long end, long margin, StreamWindowRepository repository) {
            return repository.createWindow(start, end, margin, StorageType.ONHEAP);
        }
    }

    public static class WindowSizeStrategy implements StreamWindowStrategy {
        private final static long ONE_HOUR = 3600 * 1000;
        private final static long FIVE_HOURS = 5 * 3600 * 1000;
        private final long onheapWindowSizeLimit;
        private final long offheapWindowSizeLimit;

        public static WindowSizeStrategy INSTANCE = new WindowSizeStrategy(ONE_HOUR, FIVE_HOURS);

        public WindowSizeStrategy(long onheapWindowSizeLimit, long offheapWindowSizeLimit) {
            this.offheapWindowSizeLimit = offheapWindowSizeLimit;
            this.onheapWindowSizeLimit = onheapWindowSizeLimit;

            if (this.offheapWindowSizeLimit < this.onheapWindowSizeLimit) {
                throw new IllegalStateException("offheapWindowSizeLimit " + this.offheapWindowSizeLimit + " < onheapWindowSizeLimit " + this.onheapWindowSizeLimit);
            }
        }

        @Override
        public StreamWindow createWindow(long start, long end, long margin, StreamWindowRepository repository) {
            long windowLength = end - start;
            if (windowLength <= onheapWindowSizeLimit) {
                return repository.createWindow(start, end, margin, StreamWindowRepository.StorageType.ONHEAP);
            } else if (windowLength > onheapWindowSizeLimit & windowLength <= offheapWindowSizeLimit) {
                return repository.createWindow(start, end, margin, StreamWindowRepository.StorageType.DIRECT_MEMORY);
            } else {
                return repository.createWindow(start, end, margin, StreamWindowRepository.StorageType.FILE_RAF);
            }
        }
    }
}