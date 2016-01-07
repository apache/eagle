/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.gc.parser.full;

import org.apache.eagle.gc.parser.GCType;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.GCEventParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * "Concurrent mode failure can be a separate line after ParaNew Promotion Failure
 (concurrent mode failure): 89131378K->75055239K(97517568K), 430.8303930 secs] 91834503K->75055239K(100348736K), [CMS Perm : 54559K->54414K(83968K)], 431.5362150 secs] [Times: user=574.23 sys=0.00, real=431.47 secs]
 */
 
public class ConcurrentModeFailureParser implements GCEventParser {

	private Pattern cmfPattern = Pattern.compile("\\(concurrent mode failure\\): ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), (?:[\\.0-9]+) secs\\] ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\), \\[CMS Perm : ([0-9]+)K->([0-9]+)K\\(([0-9]+)K\\)\\], ([\\.0-9]+) secs\\] \\[Times: user=(?:[\\.0-9]+) sys=(?:[\\.0-9]+), real=([\\.0-9]+) secs\\]");
	
	public ConcurrentModeFailureParser(){
	}
	
	@Override
	public GCPausedEvent parse(String line) throws Exception{
		GCPausedEvent bean = null;
		boolean matched;
		Matcher m = cmfPattern.matcher(line);
		matched = m.find();
		if(matched){
			bean = new GCPausedEvent();
			bean.setEventType(GCType.FullGC.name());
			bean.setTenuredAreaGCed(true);
			bean.setTenuredUsedHeapK(Integer.parseInt(m.group(1)));
			bean.setTenuredTotalHeapK(Integer.parseInt(m.group(2)));
			bean.setPermAreaGCed(true);
			bean.setPermUsedHeapK(Integer.parseInt(m.group(7)));
			bean.setPermTotalHeapK(Integer.parseInt(m.group(9)));

			bean.setPausedGCTimeSec(Double.parseDouble(m.group(10)));
			bean.setLogLine(line);
		}
		return bean;
	}
}
