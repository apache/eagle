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
package org.apache.eagle.query;

import java.util.List;

/**
 * @since : 10/30/14,2014
 */
public interface GenericQuery {
	/**
	 * Throw all exceptions to http server
	 *
     * @param <T> result entity type
	 * @return result entities list
	 *
     * @throws Exception
	 */
	<T> List<T> result() throws Exception;

	/**
	 * Get last/largest timestamp on all rows
	 *
	 * @return last timestamp
	 */
	long getLastTimestamp();

	/**
	 * Get first timestamp on all rows
	 */
	long getFirstTimeStamp();
}