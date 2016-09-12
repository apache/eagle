/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.hadoop.queue.exceptions;

public class HadoopQueueFetcherException extends Exception {

    private static final long serialVersionUID = -2425311876734366496L;

    /**
     * Default constructor of FeederException.
     */
    public HadoopQueueFetcherException() {
        super();
    }

    /**
     * Constructor of FeederException.
     *
     * @param message error message
     */
    public HadoopQueueFetcherException(String message) {
        super(message);
    }

    /**
     * Constructor of FeederException.
     *
     * @param message error message
     * @param cause   the cause of the exception
     */
    public HadoopQueueFetcherException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor of FeederException.
     *
     * @param cause the cause of the exception
     */
    public HadoopQueueFetcherException(Throwable cause) {
        super(cause);
    }
}
