/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public final class StringUtils {
	
   public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
   private StringUtils(){
	   
   }
   
   public static String convertMapToString(Map<String,String> tags){
	  StringBuilder tagBuilder = new StringBuilder();
	  Iterator<Entry<String, String>> iter = tags.entrySet().iterator();
	  while (iter.hasNext()) {
         Map.Entry<String,String> entry = (Map.Entry<String,String>) iter.next();    	 
   	     tagBuilder.append(entry.getKey() + ":" + entry.getValue());
   	     if(iter.hasNext()){
   	    	tagBuilder.append(",");
	   	 }
   	  }    
	  return tagBuilder.toString();    	
   }   
} 
