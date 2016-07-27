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

import java.util.AbstractList;
import java.util.RandomAccess;

/**
 * Circular array implementation
 *
 * @param <E>
 */
public class CircularArrayList<E> extends AbstractList<E> implements RandomAccess {
  
    private final E[] buf; // a List implementing RandomAccess
    private int head = 0;
    private int tail = 0;
    private boolean full = false;
  
    public CircularArrayList(E[] array) {
        buf = array;
        full = (buf.length == 0);
    }
  
    public int capacity() {
        return buf.length;
    }
    
    public int head() {
    	return head;
    }
    
    public int tail() {
    	return tail;
    }
    
    public boolean isFull() {
    	return full;
    }
    
    @Override
    public void clear() {
        head = 0;
        tail = 0;
        full = false;
        for (int i = 0; i < buf.length; ++i) {
        	buf[i] = null;
        }
    }

    private int wrapIndex(int i) {
        int m = i % buf.length;
        if (m < 0) { // java modulus can be negative
            throw new IndexOutOfBoundsException();
        }
        return m;
    }
  
    // This method is O(n) but will never be called if the
    // CircularArrayList is used in its typical/intended role.
    private void shiftBlock(int startIndex, int endIndex) {
        assert (endIndex > startIndex);
        for (int i = endIndex - 1; i >= startIndex; i--) {
            set(i + 1, get(i));
        }
    }
    
    public int find(E e) {
    	final int size = size();
    	for (int i = 0; i < size; ++i) {
    		if (e.equals(get(i))) {
    			return i;
    		}
    	}
    	return -1;
    }
  
    @Override
    public int size() {
    	if (full) {
    		return buf.length;
    	}
        return tail - head + (tail < head ? buf.length : 0);
    }
  
    @Override
    public E get(int i) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf[wrapIndex(head + i)];
    }
  
    @Override
    public E set(int i, E e) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf[wrapIndex(head + i)] =  e;
    }
  
    @Override
    public void add(int i, E e) {
        int s = size();
        if (s == buf.length) {
            throw new IllegalStateException("Cannot add element."
                    + " CircularArrayList is filled to capacity.");
        }
        full = (s + 1 == buf.length);
        if (i < 0 || i > s) {
            throw new IndexOutOfBoundsException();
        }
        tail = wrapIndex(tail + 1);
        if (i < s) {
            shiftBlock(i, s);
        }
        set(i, e);
    }
  
    @Override
    public E remove(int i) {
        int s = size();
        if (i < 0 || i >= s) {
            throw new IndexOutOfBoundsException();
        }
        final E e = get(i);
        if (i > 0) {
            shiftBlock(0, i);
        }
    	buf[head] = null;
        head = wrapIndex(head + 1);
        full = false;
        return e;
    }
}