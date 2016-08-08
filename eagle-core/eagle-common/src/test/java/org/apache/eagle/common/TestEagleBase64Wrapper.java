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

import org.junit.Test;

public class TestEagleBase64Wrapper {
	@Test
	public void test(){
		byte[] b = EagleBase64Wrapper.decode("BgVz-6vkdM8AAbGAf__-trtos5aqSGPod4Q1GwA268vF50iNBgmpmAxLXKkGbxkREWcmOzT3YIx3hDUb");
		byte[] c = EagleBase64Wrapper.decode("BgVz-6vkdM8AAbGAf__-trtos5aqSGPod4Q1G6pLeJcAATVuADbry8XnSI0GCamYDEtcqQZvGRERZyY7NPdgjHeENRs");
		
		System.out.println(new String(b));
		System.out.println(new String(c));
		
		int hash = "jobType".hashCode();
		byte b1 = (byte)((hash >> 24) & 0xff);
		byte b2 = (byte)(((hash << 8) >> 24) & 0xff);
		byte b3 = (byte)(((hash << 16) >> 24) & 0xff);
		byte b4 = (byte)(((hash << 24) >> 24) & 0xff);
		
		System.out.println(b1 + "," + b2 + "," + b3 + "," + b4);
	}
}
