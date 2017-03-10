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

package org.apache.eagle.jpm.util.resourcefetch.ha;

import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.connection.InputStreamUtils;
import org.apache.eagle.jpm.util.resourcefetch.url.JobListServiceURLBuilderImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


import java.io.IOException;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(InputStreamUtils.class)
public class HAURLSelectorImplTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testCheckUrl() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088", null, Constants.CompressionType.GZIP)).thenReturn(null);
        Assert.assertTrue(haurlSelector.checkUrl("http://www.xxx.com:8088"));
    }

    @Test
    public void testCheckUrl1() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088", null, Constants.CompressionType.GZIP)).thenThrow(new Exception());
        Assert.assertFalse(haurlSelector.checkUrl("http://www.xxx.com:8088"));
    }

    @Test
    public void testGetSelectedUrl() {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        Assert.assertEquals(rmBasePaths[0], haurlSelector.getSelectedUrl());
    }

    @Test
    public void testReSelectUrl() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenThrow(new Exception());
        when(InputStreamUtils.getInputStream("http://www.yyy.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(null);
        haurlSelector.checkUrl();
        Assert.assertEquals(rmBasePaths[1], haurlSelector.getSelectedUrl());
    }

    @Test
    public void testReSelectUrl1() throws Exception {
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenReturn(null);
        when(InputStreamUtils.getInputStream("http://www.yyy.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenThrow(new Exception());
        haurlSelector.checkUrl();
        Assert.assertEquals(rmBasePaths[0], haurlSelector.getSelectedUrl());
    }


    @Test
    public void testReSelectUrl2() throws Exception {
        thrown.expect(IOException.class);
        String[] rmBasePaths = new String[]{"http://www.xxx.com:8088", "http://www.yyy.com:8088"};
        HAURLSelectorImpl haurlSelector = new HAURLSelectorImpl(rmBasePaths, new JobListServiceURLBuilderImpl(), Constants.CompressionType.GZIP);
        mockStatic(InputStreamUtils.class);
        when(InputStreamUtils.getInputStream("http://www.xxx.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenThrow(new Exception());
        when(InputStreamUtils.getInputStream("http://www.yyy.com:8088/ws/v1/cluster/apps?state=RUNNING&anonymous=true", null, Constants.CompressionType.GZIP)).thenThrow(new Exception());
        haurlSelector.checkUrl();
    }
}
