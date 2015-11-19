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

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.query.parser.ComparisonOperator;
import org.apache.eagle.query.parser.TokenType;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestExpressionComparator {
    @Test
    public void testCompareToForEval(){
        QualifierFilterEntity entity = new QualifierFilterEntity();
        // a+b >= a+100.0
        entity.setKey("a/b");
        entity.setKeyType(TokenType.EXP);
        entity.setValue("c");
        entity.setValueType(TokenType.EXP);
        entity.setOp(ComparisonOperator.GREATER_OR_EQUAL);
        EntityDefinition qualifierDisplayNameMap = null;
        BooleanExpressionComparator comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        Map<String,Double> context = new HashMap<String,Double>();
        Assert.assertEquals("Should return 0 because not given enough variable",0,comparator.compareTo(context));

        context.put("a", 80.0);
        context.put("b",20.0);
        context.put("c",3.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",80.0);
        context.put("b",20.0);
        context.put("c",4.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",80.0);
        context.put("b",20.0);
        context.put("c",5.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        // Return false once any Double.isInfinite ( 80.0 / 0.0 )
        Assert.assertTrue(Double.isInfinite( 80.0 / 0.0 ));
        context.put("a",80.0);
        context.put("b",0.0);
        context.put("c", 5.0);
        Assert.assertEquals(0,comparator.compareTo(context));
    }

    @Test
    public void testCompareToForOp(){
        QualifierFilterEntity entity = new QualifierFilterEntity();

        // a+b >= a+100.0
        entity.setKey("a + b");
        entity.setValue("a + 100.0");
        entity.setOp(ComparisonOperator.GREATER_OR_EQUAL);
        EntityDefinition qualifierDisplayNameMap = new EntityDefinition();

        BooleanExpressionComparator comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        Map<String,Double> context = new HashMap<String,Double>();
        context.put("a",100.1);
        context.put("b",100.1);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",100.1);
        context.put("b",100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",100.0);
        context.put("b",99.9);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",-100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        // a+b = a+100.0
        entity.setOp(ComparisonOperator.GREATER);
        comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        context.put("a",100.1);
        context.put("b",100.1);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",100.1);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",100.0);
        context.put("b",99.9);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",-100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        // a+b = a+100.0
        entity.setOp(ComparisonOperator.LESS);
        comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        context.put("a",100.1);
        context.put("b",100.1);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",100.1);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",100.0);
        context.put("b",99.9);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",-100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        // a+b <= a+100.0
        entity.setOp(ComparisonOperator.LESS_OR_EQUAL);
        comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        context.put("a",100.1);
        context.put("b",100.1);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",100.1);
        context.put("b",100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",100.0);
        context.put("b",99.9);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",-100.0);
        Assert.assertEquals(1,comparator.compareTo(context));

        entity.setOp(ComparisonOperator.NOT_EQUAL);
        comparator = new BooleanExpressionComparator(entity,qualifierDisplayNameMap);

        context.put("a",100.1);
        context.put("b",100.1);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",100.1);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",100.0);
        context.put("b",99.9);
        Assert.assertEquals(1,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b",100.0);
        Assert.assertEquals(0,comparator.compareTo(context));

        context.put("a",-200.0);
        context.put("b", -100.0);
        Assert.assertEquals(1,comparator.compareTo(context));
    }
}
