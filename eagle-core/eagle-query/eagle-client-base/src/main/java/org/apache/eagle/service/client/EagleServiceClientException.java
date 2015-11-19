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
package org.apache.eagle.service.client;

/**
 * Default Eagle service client exception class
 */
public class EagleServiceClientException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1710851110199812779L;

	/**
	 * Default constructor of EagleServiceClientException
	 */
    public EagleServiceClientException() {
        super();
    }

    /**
     * Constructor of EagleServiceClientException
     * 
     * @param message error message
     */
    public EagleServiceClientException(String message) {
        super(message);
    }

    /**
     * Constructor of EagleServiceClientException
     * 
     * @param message error message
     * @param cause the cause of the exception
     * 
     */
    public EagleServiceClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor of EagleServiceClientException
     * 
     * @param cause the cause of the exception
     */
    public EagleServiceClientException(Throwable cause) {
        super(cause);
    }
}
