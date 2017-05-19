/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.analyzer;

import org.apache.eagle.jpm.analyzer.publisher.Result;
import org.junit.Assert;
import org.junit.Test;

public class ResultTest {

    @Test
    public void testResultLevel() {
        String info = "info";
        Assert.assertFalse(Result.ResultLevel.contains(info));

        String INFO = "INFO";
        Assert.assertTrue(Result.ResultLevel.contains(INFO));
        Assert.assertTrue(Result.ResultLevel.fromString(INFO).equals(Result.ResultLevel.INFO));

        Assert.assertTrue(Result.ResultLevel.INFO.ordinal() == 1);
        Assert.assertTrue(Result.ResultLevel.CRITICAL.ordinal() > Result.ResultLevel.INFO.ordinal());
    }
}
