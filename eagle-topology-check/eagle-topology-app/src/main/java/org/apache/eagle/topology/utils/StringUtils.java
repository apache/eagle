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
