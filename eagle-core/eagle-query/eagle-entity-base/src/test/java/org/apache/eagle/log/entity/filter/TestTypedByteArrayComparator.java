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
package org.apache.eagle.log.entity.filter;

import org.apache.eagle.log.entity.meta.DoubleSerDeser;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.log.entity.meta.IntSerDeser;
import org.apache.eagle.log.entity.meta.LongSerDeser;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 2014/11/17
 */
public class TestTypedByteArrayComparator {
    @Test
    public void testCompare(){
        EntitySerDeser serDeser = new DoubleSerDeser();
        TypedByteArrayComparator comparator = new TypedByteArrayComparator(serDeser.serialize(0.9),serDeser.type());
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(0.8)) > 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(1.1)) < 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(0.9)) == 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(- 0.9)) > 0);

        serDeser = new IntSerDeser();
        comparator = new TypedByteArrayComparator(serDeser.serialize(9),serDeser.type());
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(8)) > 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(11)) < 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(9)) == 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(-9)) > 0);

        serDeser = new LongSerDeser();
        comparator = new TypedByteArrayComparator(serDeser.serialize(9l),serDeser.type());
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(8l)) > 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(11l)) < 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(9l)) == 0);
        Assert.assertTrue(comparator.compareTo(serDeser.serialize(-9l)) > 0);
    }

    @Test
    public void testClassName(){
        Assert.assertEquals("long",long.class.getName());
        Assert.assertEquals("java.lang.Long", Long.class.getName());
        Assert.assertEquals("long",long.class.toString());
        Assert.assertEquals("class java.lang.Long", Long.class.toString());
    }
}
