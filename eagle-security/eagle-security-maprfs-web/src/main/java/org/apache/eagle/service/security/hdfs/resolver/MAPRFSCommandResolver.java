/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.security.hdfs.resolver;

import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class MAPRFSCommandResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {
    private final static Logger LOG = LoggerFactory.getLogger(MAPRFSCommandResolver.class);

    private final static String [] cmdStrs = {"CHGRP", "CHOWN", "CHPERM","CREATE", "DELETE",
            "DISABLEAUDIT", "ENABLEAUDIT", "GETATTR","LOOKUP", "MKDIR", "READ", "READDIR",
            "RENAME", "RMDIR", "SETATTR", "WRITE"};

    private final static String MAPRFS_CMD_RESOLVE_FORMAT_HINT = String.format("maprfs command must be in {%s}", StringUtils.join(cmdStrs, ","));

    private final static List<String> commands = Arrays.asList(cmdStrs);

    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        String query = request.getQuery().trim();
        List<String> res = new ArrayList<>();
        for(String cmd : commands) {
            Pattern pattern = Pattern.compile("^" + query, Pattern.CASE_INSENSITIVE);
            if(pattern.matcher(cmd).find()) {
                res.add(cmd);
            }
        }
        if(res.size() == 0) {
            return commands;
        }
        return res;
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
        String query = request.getQuery();
        boolean matched = Pattern.matches("[a-zA-Z]+", query);
        if (query == null || !matched) {
            throw new BadAttributeResolveRequestException(MAPRFS_CMD_RESOLVE_FORMAT_HINT);
        }
    }

    @Override
    public Class<GenericAttributeResolveRequest> getRequestClass() {
        return GenericAttributeResolveRequest.class;
    }
}
