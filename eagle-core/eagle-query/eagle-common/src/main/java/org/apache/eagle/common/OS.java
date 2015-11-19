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

public class OS {

	private final static String os = System.getProperty("os.name")
			.toLowerCase();

	public static boolean isWindows() {
		return (os.indexOf("win") >= 0);
	}

	public static boolean isMac() {
		return (os.indexOf("mac") >= 0);
	}

	public static boolean isUnix() {
		return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os
				.indexOf("aix") > 0);
	}

	public static boolean isSolaris() {
		return (os.indexOf("sunos") >= 0);
	}

}
