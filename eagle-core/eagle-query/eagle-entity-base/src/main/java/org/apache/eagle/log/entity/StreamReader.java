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
package org.apache.eagle.log.entity;

import java.util.ArrayList;
import java.util.List;

public abstract class StreamReader {
	protected List<EntityCreationListener> _listeners = new ArrayList<EntityCreationListener>();

	/**
	 * Listener can be only notified after it is added to listener list
	 * @param listener
	 */
	public synchronized void register(EntityCreationListener listener){
		_listeners.add(listener);
	}
	
	/**
	 * Listener can not get notification once after it is removed from listener list
	 * @param listener
	 */
	public synchronized void unregister(EntityCreationListener listener){
		_listeners.remove(listener);
	}
	
	public abstract void readAsStream() throws Exception;
	
	/**
	 * Get scanned last entity timestamp
	 * 
	 * @return
	 */
	public abstract long getLastTimestamp();
	
	/**
	 * Get scanned first entity timestamp
	 * @return
	 */
	public abstract long getFirstTimestamp();
}