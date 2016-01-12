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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.dsl.dao;

import org.apache.eagle.stream.dsl.entity.AppCommandEntity;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;


public class TestAppEntityDaoImpl {
    private final static Logger LOG = LoggerFactory.getLogger(TestAppEntityDaoImpl.class);

    @Test
    public void testSearch(){
        AppEntityDaoImpl dao = new AppEntityDaoImpl("localhost", 9098, "admin", "secret");
        String query = "AppCommandService[@status=\"UNKNOWN\"]{*}";
        List<AppCommandEntity> result = dao.search(query, Integer.MAX_VALUE).getObj();
        assertTrue(result != null);
        LOG.debug(result.toString());
    }
}