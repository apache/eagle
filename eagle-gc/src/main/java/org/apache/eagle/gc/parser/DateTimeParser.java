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

package org.apache.eagle.gc.parser;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeParser {
	/**
	 * sample 2014-06-04T22:21:19.158-0700
	 * see http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html#year
	 * @param dateTimeString
	 * @return
	 */
	public static Date parseDateTimeString(String dateTimeString) throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		Date d = sdf.parse(dateTimeString);
		return d;
	}
}
