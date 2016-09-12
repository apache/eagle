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

import org.apache.commons.net.util.Base64;

/**
 * wrap base64 encoding and decoding, so reduce the confuse of using many Base64 methods. 
 */
public class EagleBase64Wrapper {
	public static String encodeByteArray2URLSafeString(byte[] bytes){
		return Base64.encodeBase64URLSafeString(bytes);
	}
	
	public static byte[] decode(String input){
		return Base64.decodeBase64(input);
	}
}
