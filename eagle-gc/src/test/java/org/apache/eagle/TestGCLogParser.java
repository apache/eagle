/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle;

import junit.framework.Assert;
import org.apache.eagle.gc.model.GCPausedEvent;
import org.apache.eagle.gc.parser.full.ConcurrentModeFailureParser;
import org.apache.eagle.gc.parser.full.NormalFullGCParser;
import org.apache.eagle.gc.parser.full.ParaNewPromotionFailureParser;
import org.apache.eagle.gc.parser.tenured.CMSInitialMarkParser;
import org.apache.eagle.gc.parser.tenured.CMSRemarkParser;
import org.apache.eagle.gc.parser.young.ParaNewParser;
import org.junit.Test;

public class TestGCLogParser {

    @Test
    public void testParNewParser() throws Exception {
        String line = "2015-12-28T19:42:34.013-0700: 5511298.821: [GC2015-12-28T19:42:34.014-0700: 5511298.821: [ParNew: 8563150K->138183K(9437184K), 0.1247550 secs] 78382766K->69967324K(124780544K), 0.1250410 secs] [Times: user=3.29 sys=0.00, real=0.12 secs]";
        ParaNewParser parser = new ParaNewParser();
        GCPausedEvent event = parser.parse(line);
        Assert.assertTrue(event.getYoungUsedHeapK() ==  8563150);
        Assert.assertTrue(event.getYoungTotalHeapK() ==  9437184);
    }

    @Test
    public void testCMSInitialMarkParser() throws Exception {
        String line = "2014-06-04T22:47:31.218-0700: 1582.012: [GC [1 CMS-initial-mark: 78942227K(97517568K)] 79264643K(100348736K), 0.2334170 secs] [Times: user=0.23 sys=0.00, real=0.24 secs]";
        CMSInitialMarkParser parser = new CMSInitialMarkParser();
        GCPausedEvent event = parser.parse(line);
        Assert.assertTrue(event.getTenuredUsedHeapK() ==  78942227);
        Assert.assertTrue(event.getTenuredTotalHeapK() ==  97517568);
    }

    @Test
    public void testCMSRemarkParser() throws Exception {
        String line = "2014-06-04T22:49:50.603-0700: 1721.397: [GC[YG occupancy: 2777944 K (2831168 K)]1721.398: [Rescan (parallel) , 0.1706730 secs]1721.568: [weak refs processing, 0.0156130 secs] [1 CMS-remark: 83730081K(97517568K)] 86508026K(100348736K), 0.1868130 secs] [Times: user=3.04 sys=0.01, real=0.18 secs]";
        CMSRemarkParser parser = new CMSRemarkParser();
        GCPausedEvent event = parser.parse(line);
        Assert.assertTrue(event.getTenuredUsedHeapK() ==  83730081);
        Assert.assertTrue(event.getTenuredTotalHeapK() ==  97517568);
    }

    @Test
    public void testNormalFullGCParser() throws Exception {
        String line = "2014-08-13T12:22:25.488-0700: 144.526: [Full GC2014-08-13T12:22:25.488-0700: 144.526: [CMS: 9845647K->10115891K(97517568K), 14.2064400 secs] 10215536K->10115891K(100348736K), [CMS Perm : 24119K->24107K(24320K)], 14.2066090 secs] [Times: user=13.86 sys=0.32, real=14.20 secs]";
        NormalFullGCParser parser = new NormalFullGCParser();
        GCPausedEvent event = parser.parse(line);
        Assert.assertTrue(event.getTenuredUsedHeapK() == 9845647);
        Assert.assertTrue(event.getTenuredTotalHeapK() == 97517568);
        Assert.assertTrue(event.getPermUsedHeapK() == 24119);
        Assert.assertTrue(event.getPermTotalHeapK() == 24320);
    }

    @Test
    public void testConcurrentModeFailParser() throws Exception {
        String line = "(concurrent mode failure): 89131378K->75055239K(97517568K), 430.8303930 secs] 91834503K->75055239K(100348736K), [CMS Perm : 54559K->54414K(83968K)], 431.5362150 secs] [Times: user=574.23 sys=0.00, real=431.47 secs]";
        ConcurrentModeFailureParser parser = new ConcurrentModeFailureParser();
        GCPausedEvent event = parser.parse(line);
        Assert.assertTrue(event.getTenuredUsedHeapK() == 89131378);
        Assert.assertTrue(event.getTenuredTotalHeapK() == 75055239);
        Assert.assertTrue(event.getPermUsedHeapK() == 54559);
        Assert.assertTrue(event.getPermTotalHeapK() == 83968);
    }

    @Test
    public void testParaNewPromotionFailParser() throws Exception {
        String line1 = "2014-07-08T21:52:32.910-0700: 2935883.704: [GC 2935883.704: [ParNew (promotion failed): 2827840K->2824012K(2831168K), 0.8317310 secs]2935884.536: [CMS: 71633438K->38250633K(97517568K), 211.7852880 secs] 74430253K->38250633K(100348736K), [CMS Perm : 54880K->54867K(83968K)], 212.6173060 secs] [Times: user=0.00 sys=214.88, real=212.59 secs]";
        String line2 = "2014-09-25T18:25:25.424-0700: 2568209.488: [GC2014-09-25T18:25:25.424-0700: 2568209.488: [ParNew (promotion failed): 4665858K->4627344K(4718592K), 1.2918500 secs]2014-09-25T18:25:26.716-0700: 2568210.780: [CMS: 74802314K->42339736K(95420416K), 230.2937730 secs] 79433462K->42339736K(100139008K), [CMS Perm : 49896K->49865K(83316K)], 231.5926250 secs] [Times: user=0.00 sys=233.73, real=231.56 secs]";

        String line3 = "2014-09-16T02:10:29.456-0700: 1732113.520: [GC2014-09-16T02:10:29.456-0700: 1732113.520: [ParNew (promotion failed): 4703469K->4718592K(4718592K), 0.9636440 secs]2014-09-16T02:10:30.420-0700: 1732114.484: [CMS2014-09-16T02:10:48.794-0700: 1732132.858: [CMS-concurrent-mark: 28.139/29.793 secs] [Times: user=214.69 sys=8.41, real=29.79 secs]";
        String line4 = "2014-06-05T22:57:29.955-0700: 88580.749: [GC 88580.749: [ParNew (promotion failed): 2809111K->2831168K(2831168K), 0.6941530 secs]88581.443: [CMS2014-06-05T22:57:57.509-0700: 88608.303: [CMS-concurrent-sweep: 48.562/51.183 secs] [Times: user=138.07 sys=15.38, real=51.18 secs]";

        ParaNewPromotionFailureParser parser = new ParaNewPromotionFailureParser();
        GCPausedEvent event = parser.parse(line1);

        Assert.assertTrue(event.getYoungUsedHeapK() == 2827840);
        Assert.assertTrue(event.getYoungTotalHeapK() == 2831168);
        Assert.assertTrue(event.getTenuredUsedHeapK() == 71633438);
        Assert.assertTrue(event.getTenuredTotalHeapK() == 97517568);
        Assert.assertTrue(event.getPermUsedHeapK() == 54880);
        Assert.assertTrue(event.getPermTotalHeapK() == 83968);

        event = parser.parse(line2);
        Assert.assertTrue(event.getYoungUsedHeapK() == 4665858);
        Assert.assertTrue(event.getYoungTotalHeapK() == 4718592);
        Assert.assertTrue(event.getTenuredUsedHeapK() == 74802314);
        Assert.assertTrue(event.getTenuredTotalHeapK() == 95420416);
        Assert.assertTrue(event.getPermUsedHeapK() == 49896);
        Assert.assertTrue(event.getPermTotalHeapK() == 83316);

        event = parser.parse(line3);
        Assert.assertTrue(event.getYoungUsedHeapK() == 4703469);
        Assert.assertTrue(event.getYoungTotalHeapK() == 4718592);

        event = parser.parse(line4);
        Assert.assertTrue(event.getYoungUsedHeapK() == 2809111);
        Assert.assertTrue(event.getYoungTotalHeapK() == 2831168);
    }

}
