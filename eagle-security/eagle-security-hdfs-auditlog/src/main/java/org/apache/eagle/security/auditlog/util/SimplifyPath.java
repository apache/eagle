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
package org.apache.eagle.security.auditlog.util;

import java.util.ArrayList;
import java.util.List;

public class SimplifyPath {
	public String build(String path) {
        if (path == null || path.length() == 0) {
            return null;
        }
        List<String> list = new ArrayList<String>();
        String[] pathsplit = path.split("/");
        for (String p : pathsplit) {
            if (p.equals("..") && !list.isEmpty()) {
                list.remove(list.size()-1);
            } else if (!p.equals(".") && !p.equals("..") && p.length() > 0) {
                list.add(p);
            }
        }
        if (list.isEmpty()) {
        	return "/";
        }

        StringBuilder sb = new StringBuilder();
        // "a/b/c" => "./a/b/c"
        char c = path.charAt(0);
        if (c != '.' && c != '/') {
        	sb.append(".");
        }
        
        for (String s : list) {
            sb.append("/");
            sb.append(s);
        }
        return sb.toString();
    }
}
