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

package org.apache.eagle.service.security.hdfs.resolver;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.service.alert.resolver.AttributeResolvable;
import org.apache.eagle.service.alert.resolver.AttributeResolveException;
import org.apache.eagle.service.alert.resolver.BadAttributeResolveRequestException;
import org.apache.eagle.service.alert.resolver.GenericAttributeResolveRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;


public class MAPRStatusCodeResolver implements AttributeResolvable<GenericAttributeResolveRequest,String> {
    private final static Logger LOG = LoggerFactory.getLogger(MAPRStatusCodeResolver.class);

    private final static String [] statusCodes = {"SUCCESS","EPERM","ENOENT","EINTR","EIO","ENXIO","E2BIG","ENOEXEC","EBADF",
            "ECHILD","EAGAIN","ENOMEM","EACCES","EFAULT","ENOTBLK","EBUSY","EEXIST","EXDEV","ENODEV","ENOTDIR","EISDIR","EINVAL",
            "ENFILE","EMFILE","ENOTTY","ETXTBSY","EFBIG","ENOSPC","ESPIPE","EROFS","EMLINK","EPIPE","EDOM","ERANGE","EDEADLK","ENAMETOOLONG",
            "ENOLCK","ENOSYS","ENOTEMPTY","ELOOP","EWOULDBLOCK","ENOMSG","EIDRM","ECHRNG","EL2NSYNC","EL3HLT","EL3RST","ELNRNG","EUNATCH",
            "ENOCSI","EL2HLT","EBADE","EBADR","EXFULL","ENOANO","EBADRQC","EBADSLT","EDEADLOCK","EBFONT","ENOSTR","ENODATA","ETIME","ENOSR",
            "ENONET","ENOPKG","EREMOTE","ENOLINK","EADV","ESRMNT","ECOMM","EPROTO","EMULTIHOP","EDOTDOT","EBADMSG","EOVERFLOW","ENOTUNIQ",
            "EBADFD","EREMCHG","ELIBACC","ELIBBAD","ELIBSCN","ELIBMAX","ELIBEXEC","EILSEQ","ERESTART","ESTRPIPE","EUSERS","ENOTSOCK",
            "EDESTADDRREQ","EMSGSIZE","EPROTOTYPE","ENOPROTOOPT","EPROTONOSUPPORT","ESOCKTNOSUPPORT","EOPNOTSUPP","EPFNOSUPPORT","EAFNOSUPPORT",
            "EADDRINUSE","EADDRNOTAVAIL","ENETDOWN","ENETUNREACH","ENETRESET","ECONNABORTED","ECONNRESET","ENOBUFS","EISCONN","ENOTCONN",
            "ESHUTDOWN","ETOOMANYREFS","ETIMEDOUT","ECONNREFUSED","EHOSTDOWN","EHOSTUNREACH","EALREADY","EINPROGRESS","ESTALE","EUCLEAN","ENOTNAM",
            "ENAVAIL","EISNAM","EREMOTEIO","EDQUOT","ENOMEDIUM","EMEDIUMTYPE","ECANCELED","ENOKEY","EKEYEXPIRED","EKEYREVOKED","EKEYREJECTED"};
    private Map<String, String> statusCodeMap = new HashMap<String, String>();

    private final static String MAPRFS_STATUS_CODE_RESOLVE_FORMAT_HINT = String.format("Status code must be in {%s}", StringUtils.join(statusCodes, ","));

    private final static List<String> statusList = Arrays.asList(statusCodes);


    @Inject
    public MAPRStatusCodeResolver(){
        //construct hashmap for status code query
        for(int i = 0; i < statusCodes.length; i++){
            statusCodeMap.put(statusCodes[i],String.valueOf(i));
        }

    }


    //conver human readable status code to id
    public String getStatusCodeID(String code){
        String id = "STATUS CODE ID NOT FOUND";
        if(statusCodeMap.containsKey(code)) {
            id = statusCodeMap.get(code);
        }
        return id;
    }

    @Override
    public List<String> resolve(GenericAttributeResolveRequest request) throws AttributeResolveException {
        String query = request.getQuery().trim();
        List<String> res = new ArrayList<>();
        for(String status : statusList) {
            Pattern pattern = Pattern.compile("^" + query, Pattern.CASE_INSENSITIVE);
            if(pattern.matcher(status).find()) {
                res.add(status);
            }
        }
        if(res.size() == 0) {
            return statusList;
        }
        return res;
    }

    @Override
    public void validateRequest(GenericAttributeResolveRequest request) throws BadAttributeResolveRequestException {
        String query = request.getQuery();
        boolean matched = Pattern.matches("[a-zA-Z]+", query);
        if (query == null || !matched) {
            throw new BadAttributeResolveRequestException(MAPRFS_STATUS_CODE_RESOLVE_FORMAT_HINT);
        }
    }

    @Override
    public Class<GenericAttributeResolveRequest> getRequestClass() {
        return GenericAttributeResolveRequest.class;
    }
}