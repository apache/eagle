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
package org.apache.eagle.common;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CircularArrayListSortedSet<E> {

	private final CircularArrayList<E> list;
    private final Comparator<? super E> comparator;

	public CircularArrayListSortedSet(E[] array) {
		this.list = new CircularArrayList<E>(array);
		this.comparator = null;
	}
	
	public CircularArrayListSortedSet(E[] array, Comparator<? super E> comparator) {
		this.list = new CircularArrayList<E>(array);
		this.comparator = comparator;
	}
	
    public int capacity() {
        return list.capacity();
    }
    
    public int head() {
    	return list.head();
    }
    
    public int tail() {
    	return list.tail();
    }
    
    public boolean isFull() {
    	return list.isFull();
    }
  
    public void clear() {
    	list.clear();
    }
    
    public int size() {
    	return list.size();
    }
  
    public E get(int i) {
        return list.get(i);
    }
    
    @SuppressWarnings("unchecked")
	public int binarySearch(E e) {
    	if (comparator != null) {
    		return Collections.binarySearch(list, e, comparator);
    	} else {
    		return Collections.binarySearch((List<? extends Comparable<? super E>>)list, e);
    	}
    }
    
    public int replace(E e) {
    	int index = binarySearch(e);
    	if (index < 0) {
    		return -1;
    	}
    	list.set(index, e);
    	return index;
    }
  
    public int insert(E e) {
    	int index = binarySearch(e);
    	if (index > 0) {
    		return -1;
    	}
    	index = 0 - index - 1;
    	list.add(index, e);
    	return index;
    }
  
    public E remove(int i) {
    	return list.remove(i);
    }
    
    public int remove(E e) {
    	final int index = binarySearch(e);
    	if (index > 0) {
        	list.remove(index);
        	return index;
    	}
    	return -1;
    }
}
