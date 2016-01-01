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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CMSRemarkParser implements GCEventParser {

	/**
	 * 2014-06-04T22:49:50.603-0700: 1721.397: [GC[YG occupancy: 2777944 K (2831168 K)]1721.398: [Rescan (parallel) , 0.1706730 secs]1721.568: [weak refs processing, 0.0156130 secs] [1 CMS-remark: 83730081K(97517568K)] 86508026K(100348736K), 0.1868130 secs] [Times: user=3.04 sys=0.01, real=0.18 secs]
	 */

	List<Pattern> cmsRemarkPatterns = new ArrayList<Pattern>();
	
	public CMSRemarkParser() {
		cmsRemarkPatterns.add(Pattern.compile("^([^\\s]+): (?:[\\.0-9]+): \\[GC.*\\[1 CMS-remark: ([0-9]+)K\\(([0-9]+)K\\)\\] (?:[0-9]+)K\\((?:[0-9]+)K\\), ([\\.0-9]+) secs\\] \\[Times: user=([\\.0-9]+) sys=([\\.0-9]+), real=([\\.0-9]+) secs\\]"));
	}

	@Override
	public GCPausedEvent parse(String line) throws Exception{
		GCPausedEvent bean = null;
		boolean matched = false;
		Matcher m = null;
		for (Pattern pattern : cmsRemarkPatterns) {
			m = pattern.matcher(line);
			matched = m.find();
			if (matched) break;
		}
		if(matched){
			bean = new GCPausedEvent();
			// date time portion
			String dateTimeString = m.group(1);
			Date d = DateTimeParser.parseDateTimeString(dateTimeString);
			bean.setTimestamp(d.getTime());
			bean.setEventType(GCType.TenuredGC.name());

			bean.setTenuredAreaGCed(true);
			bean.setTenuredUsedHeapK(Integer.parseInt(m.group(2)));
			bean.setTenuredTotalHeapK(Integer.parseInt(m.group(3)));

			bean.setPausedGCTimeSec(Double.parseDouble(m.group(4)));
			bean.setLogLine(line);
		}
		return bean;
	}
}
