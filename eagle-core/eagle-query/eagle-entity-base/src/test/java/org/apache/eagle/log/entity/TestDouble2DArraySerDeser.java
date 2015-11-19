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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.entity.meta.Double2DArraySerDeser;
import org.junit.Test;

/**
 * @since 7/22/15
 */
public class TestDouble2DArraySerDeser {
    private Double2DArraySerDeser double2DArraySerDeser = new Double2DArraySerDeser();

    @Test
    public void testSerDeser(){
        double[][] data = new double[][]{
                {0,1,2,4},
                {4,2,1,0},
                {4},
                null,
                {}
        };

        byte[] bytes = double2DArraySerDeser.serialize(data);
        double[][] data2 = double2DArraySerDeser.deserialize(bytes);

        assert  data.length == data2.length;
        assert data[0].length == data2[0].length;
        assert data[1].length == data2[1].length;
        assert data[2].length == data2[2].length;
        assert data[3] == data2[3] && data2[3] == null;
        assert data[4].length == data2[4].length;
    }
}