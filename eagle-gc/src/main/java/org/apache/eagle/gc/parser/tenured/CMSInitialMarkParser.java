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

package org.apache.eagle.gc.parser.tenured;

import org.apache.eagle.gc.parser.GCType;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.DateTimeParser;
import org.apache.eagle.gc.parser.GCEventParser;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CMSInitialMarkParser implements GCEventParser {
	/***
	 * 2014-06-04T22:47:31.218-0700: 1582.012: [GC [1 CMS-initial-mark: 78942227K(97517568K)] 79264643K(100348736K), 0.2334170 secs] [Times: user=0.23 sys=0.00, real=0.24 secs]
	 */
	Pattern cmsInitialMarkPattern = Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC \\[1 CMS-initial-mark: ([0-9]+)K\\(([0-9]+)K\\)\\] (?:[0-9]+)K\\((?:[0-9]+)K\\), ([\\.0-9]+) secs\\] \\[Times: user=([\\.0-9]+) sys=([\\.0-9]+), real=([\\.0-9]+) secs\\]");

	public CMSInitialMarkParser(){
	}
	
	@Override
	public GCPausedEvent parse(String line) throws Exception{
		GCPausedEvent bean = null;
		boolean matched = false;
		Matcher m = cmsInitialMarkPattern.matcher(line);
		matched = m.find();
		if(matched){
			bean = new GCPausedEvent();
			// date time portion
			String dateTimeString = m.group(1);
			Date d = DateTimeParser.parseDateTimeString(dateTimeString);
			bean.setTimestamp(d.getTime());
			bean.setEventType(GCType.TenuredGC.name());

			// tenured memory
			bean.setTenuredUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setTenuredTotalHeapK(Integer.parseInt(m.group(3)));
			bean.setPausedGCTimeSec(Double.parseDouble(m.group(4)));
			
			bean.setLogLine(line);
		}
		return bean;
	}
}
