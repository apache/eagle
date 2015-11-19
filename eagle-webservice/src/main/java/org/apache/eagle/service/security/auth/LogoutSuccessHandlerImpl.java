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
package org.apache.eagle.service.security.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @since 10/5/15
 */
public class LogoutSuccessHandlerImpl extends SimpleUrlLogoutSuccessHandler {
    @Override
    public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) throws IOException, ServletException {
        PrintWriter writer = response.getWriter();
        ObjectMapper mapper = new ObjectMapper();
        if(authentication!=null && authentication.isAuthenticated()) {
            response.setStatus(HttpServletResponse.SC_OK);
            writer.write(mapper.writeValueAsString(new AuthenticationResult(true, "Successfully logout session for user \"" + authentication.getName()+"\"")));
        }else{
            response.setStatus(HttpServletResponse.SC_OK);
            writer.write(mapper.writeValueAsString(new AuthenticationResult(true, "Session is not authenticated")));
        }
        writer.flush();
        writer.close();
    }
}