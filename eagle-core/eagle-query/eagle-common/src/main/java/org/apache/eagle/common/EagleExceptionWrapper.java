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

public class EagleExceptionWrapper {
	private final static int MAX_DEPTH = 10;
	
	public static String wrap(Exception ex){
		return wrap(ex, EagleExceptionWrapper.MAX_DEPTH);
	}
	
	public static String wrap(Exception ex, int maxdepth){
		int d = maxdepth;
		if(d <= 0)
			d = EagleExceptionWrapper.MAX_DEPTH;
		int index = 0;
		StringBuffer sb = new StringBuffer();
		sb.append(ex);
		sb.append(System.getProperty("line.separator"));
		for(StackTraceElement element : ex.getStackTrace()){
			sb.append(element.toString());
			sb.append(System.getProperty("line.separator"));
			if(++index >= d)
				break;
		}
		return sb.toString();
	}
}
