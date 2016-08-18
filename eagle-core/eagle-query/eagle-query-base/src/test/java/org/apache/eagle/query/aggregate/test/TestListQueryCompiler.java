/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.query.aggregate.test;

import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.query.parser.EagleQueryParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestListQueryCompiler {
    private final static Logger LOG = LoggerFactory.getLogger(TestListQueryCompiler.class);
    @Test
    public void test() throws Exception {
        try {
            try {
                String queryWithCondition = "JobExecutionService[@site=\"apollo\"]{*}";
                ListQueryCompiler compiler1 = new ListQueryCompiler(queryWithCondition);
            } catch (EagleQueryParseException ex) {}
            try {
                String queryWithSquareBrackets = "JobExecutionService[@jobId=\"[A9BB756F29494F3C8281117FE58A4632/361C947C782F44748BFD840CE2FDBFB8] com.ebay.scalding.coview.CoviewsBasedOnSojornerData[site=2, runDate=2016-08-17, nDays=7}]/(4/5)\"]{*}";
                ListQueryCompiler compiler1 = new ListQueryCompiler(queryWithSquareBrackets);
            } catch (EagleQueryParseException ex) {}
            try {
                String queryWithoutCondition = "JobExecutionService[]{*}";
                ListQueryCompiler compiler1 = new ListQueryCompiler(queryWithoutCondition);
            } catch (EagleQueryParseException ex) {}
        } catch (IllegalArgumentException e) {
            LOG.error(e.getMessage());
            Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
    }
}
