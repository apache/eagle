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

import org.apache.eagle.log.entity.EntityQualifierUtils;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.log.expression.ExpressionParser;
import org.apache.eagle.log.expression.ParsiiInvalidException;
import org.apache.eagle.query.parser.ComparisonOperator;
import org.apache.eagle.query.parser.TokenType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsii.tokenizer.ParseException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * BooleanExpressionComparator
 *
 * Currently support double expression only.
 *
 * TODO: 1) thread-safe? 2) Rewrite filter expression to evaluate once
 *
 */
public class BooleanExpressionComparator implements WritableComparable<List<KeyValue>> {
    private final static Logger LOG = LoggerFactory.getLogger(BooleanExpressionComparator.class);

    // Should be Writable
    private QualifierFilterEntity filterEntity;
    private EntityDefinition ed;

    // Should not be writable
    private double leftValue;
    private double rightValue;
    private BooleanExprFunc func = null;

    public Set<String> getRequiredFields() {
        return requiredFields;
    }

    private Set<String> requiredFields = new HashSet<String>();

    public BooleanExpressionComparator(){}

    public BooleanExpressionComparator(QualifierFilterEntity entity,EntityDefinition ed){
        this.filterEntity  = entity;
        this.ed = ed;
        try {
            this.init();
        } catch (Exception ex) {
            // Client side expression validation to fast fail if having error
            LOG.error("Got exception: "+ex.getMessage(),ex);
            throw new ExpressionEvaluationException(ex.getMessage(),ex);
        }
    }

    private void init() throws ParsiiInvalidException, ParseException {
        LOG.info("Filter expression: "+filterEntity.toString());
        if (filterEntity.getKey() != null) {
            if (filterEntity.getKeyType() == TokenType.NUMBER) {
                leftValue = Double.parseDouble(filterEntity.getKey());
            } else {
                ExpressionParser parser = ExpressionParser.parse(filterEntity.getKey());
                requiredFields.addAll(parser.getDependentFields());
            }
        } else {
            throw new IllegalStateException("QualifierFilterEntity key is null");
        }

        if (filterEntity.getValue() != null) {
            if (filterEntity.getValueType() == TokenType.NUMBER) {
                rightValue = Double.parseDouble(filterEntity.getValue());
            } else {
                ExpressionParser parser = ExpressionParser.parse(filterEntity.getValue());
                requiredFields.addAll(parser.getDependentFields());
            }
        } else {
            throw new IllegalStateException("QualifierFilterEntity value is null");
        }

        if (this.filterEntity.getOp() == null)
            throw new IllegalStateException("QualifierFilterEntity op is null");
        this.func = _opExprFuncMap.get(this.filterEntity.getOp());
        if (this.func == null)
            throw new IllegalStateException("No boolean evaluation function found for operation: " + this.filterEntity.getOp());
    }

    /**
     * if(Double.isInfinite(leftValue) || Double.isInfinite(rightValue)) return false;
     *
     * @param context Map[String,Double]
     * @return evaluation result as true (1) or false (0)
     * @throws Exception
     */
    private boolean eval(Map<String,Double> context) throws Exception {
        if(filterEntity.getKeyType() != TokenType.NUMBER){
            leftValue = eval(filterEntity.getKey(),context);
        }
        if(filterEntity.getValueType() != TokenType.NUMBER){
            rightValue = eval(filterEntity.getValue(),context);
        }
        if(Double.isInfinite(leftValue) || Double.isInfinite(rightValue)){
//            if(LOG.isDebugEnabled()) {
            if (Double.isInfinite(leftValue)) {
                LOG.warn("Evaluation result of key: " + this.filterEntity.getKey() + " is " + leftValue + " (Infinite), ignore");
            } else {
                LOG.warn("Evaluation result of value: "+this.filterEntity.getValue()+" is "+rightValue+" (Infinite), ignore");
            }
//            }
            return false;
        }
        return func.eval(leftValue,rightValue);
    }

    /**
     * if(Double.isInfinite(leftValue) || Double.isInfinite(rightValue)) return false;
     *
     * @param expr
     * @param context
     * @return
     * @throws Exception
     */
    private double eval(String expr,Map<String,Double> context) throws Exception {
        return ExpressionParser.parse(expr).eval(context);
    }

    /**
     *
     * @param row List[KeyValue] All key values in a row
     *
     * @return 0 to filter out row [false], otherwise to include row into scanner [true]
     */
    @Override
    public int compareTo(List<KeyValue> row) {
        Map<String,Double> context = new HashMap<String, Double>();
        for(KeyValue kv:row){
            String qualifierName = new String(kv.getQualifier());

            // Because assume just handle about double value
            // so ignore tag whose value is String
            if(!this.ed.isTag(qualifierName)){
                Qualifier qualifier = this.ed.getQualifierNameMap().get(qualifierName);
                String displayName = qualifier.getDisplayName();
                if(displayName == null) displayName = qualifierName;
                try {
                    if(this.requiredFields.contains(displayName)) {
                        EntitySerDeser serDeser = qualifier.getSerDeser();
                        double value = EntityQualifierUtils.convertObjToDouble(serDeser.deserialize(kv.getValue()));
                        if (Double.isNaN(value)) {
                            context.put(displayName, value);
                        }
                    }
                }catch (Exception ex){
                    LOG.warn("Failed to parse value of field "+displayName+" as double, ignore: "+ex.getMessage(),ex);
                }
            }
        }
        return compareTo(context);
    }

    /**
     * @param context Map[String,Double]
     *
     * @return context.keySet().containsAll(this.requiredFields) && eval(context) ? 1:0;
     */
    int compareTo(Map<String,Double> context){
        try {
            if(context.keySet().containsAll(this.requiredFields)){
                return eval(context)? 1:0;
            }else{
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Require variables: [" + StringUtils.join(this.requiredFields, ",") + "], but just given: [" + StringUtils.join(context.keySet(), ",") + "]");
                }
                return 0;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new ExpressionEvaluationException(e.getMessage(),e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.filterEntity.write(out);
        this.ed.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.filterEntity = new QualifierFilterEntity();
        this.filterEntity.readFields(in);
        this.ed = new EntityDefinition();
        this.ed.readFields(in);

        try {
            this.init();
        } catch (Exception ex){
            LOG.error("Got exception: "+ex.getMessage(),ex);
            throw new IOException(ex.getMessage(),ex);
        }
    }

    private static Map<ComparisonOperator,BooleanExprFunc> _opExprFuncMap = new HashMap<ComparisonOperator, BooleanExprFunc>();

    static {
        _opExprFuncMap.put(ComparisonOperator.EQUAL,new EqualExprFunc());
        _opExprFuncMap.put(ComparisonOperator.IS,new EqualExprFunc());

        _opExprFuncMap.put(ComparisonOperator.NOT_EQUAL,new NotEqualExprFunc());
        _opExprFuncMap.put(ComparisonOperator.IS_NOT,new NotEqualExprFunc());

        _opExprFuncMap.put(ComparisonOperator.LESS,new LessExprFunc());
        _opExprFuncMap.put(ComparisonOperator.LESS_OR_EQUAL,new LessOrEqualExprFunc());
        _opExprFuncMap.put(ComparisonOperator.GREATER,new GreaterExprFunc());
        _opExprFuncMap.put(ComparisonOperator.GREATER_OR_EQUAL,new GreaterOrEqualExprFunc());

        // "Life should be much better with functional programming language" - Hao Chen Nov 18th, 2014
    }

    private static interface BooleanExprFunc {
        boolean eval(double val1,double val2);
    }

    private static class EqualExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 == val2;
        }
    }
    private static class NotEqualExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 != val2;
        }
    }

    private static class LessExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 < val2;
        }
    }
    private static class LessOrEqualExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 <= val2;
        }
    }
    private static class GreaterExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 > val2;
        }
    }
    private static class GreaterOrEqualExprFunc implements BooleanExprFunc {
        @Override
        public boolean eval(double val1, double val2) {
            return val1 >= val2;
        }
    }

    public static class ExpressionEvaluationException extends RuntimeException{
        public ExpressionEvaluationException(String message, Throwable cause) {
            super(message, cause);
        }
        public ExpressionEvaluationException(String message) {
            super(message);
        }
        public ExpressionEvaluationException(Throwable cause) {
            super(cause);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+" ("+this.filterEntity.toString()+")";
    }
}