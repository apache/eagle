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

import java.io.Closeable;
import java.util.Collection;

/**
 * TODO: Reuse existing expired window to avoid recreating new windows again and again
 * <p>
 * Single stream window manager
 */
public interface StreamWindowManager extends StreamTimeClockListener, Closeable {

    /**
     * @param initialTime
     * @return
     */
    StreamWindow addNewWindow(long initialTime);

    /**
     * @param window
     */
    void removeWindow(StreamWindow window);

    /**
     * @param window
     * @return
     */
    boolean hasWindow(StreamWindow window);

    /**
     * @param timestamp time
     * @return whether window exists for time
     */
    boolean hasWindowFor(long timestamp);

    /**
     * @return Internal collection for performance optimization
     */
    Collection<StreamWindow> getWindows();

    /**
     * @param timestamp
     * @return
     */
    StreamWindow getWindowFor(long timestamp);

    boolean reject(long timestamp);
}