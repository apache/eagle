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

import org.apache.eagle.query.parser.ComparisonOperator;
import org.apache.eagle.query.parser.TokenType;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QualifierFilterEntity implements Writable{
	public String key;
	public String value;
	public ComparisonOperator op;
	public TokenType valueType;
	public TokenType keyType;

	public QualifierFilterEntity(){}
	public QualifierFilterEntity(String key, String value, ComparisonOperator comp, TokenType keyType, TokenType valueType) {
		super();
		this.key = key;
		this.value = value;
		this.op = comp;
		this.keyType = keyType;
		this.valueType = valueType;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public ComparisonOperator getOp() {
		return op;
	}

	public void setOp(ComparisonOperator op) {
		this.op = op;
	}

	public TokenType getValueType() {
		return valueType;
	}

	public void setValueType(TokenType valueType) {
		this.valueType = valueType;
	}

	public void setKeyType(TokenType keyType){
		this.keyType = keyType;
	}
	public TokenType getKeyType(){
		return this.keyType;
	}

	@Override
	public String toString() {
		return String.format("%s %s %s",this.key,this.op,this.value);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.key);
		out.writeUTF(this.getValue());
		out.writeUTF(this.op.name());
		out.writeUTF(this.keyType.name());
		out.writeUTF(this.valueType.name());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key = in.readUTF();
		this.value = in.readUTF();
		this.op = ComparisonOperator.valueOf(in.readUTF());
		this.keyType = TokenType.valueOf(in.readUTF());
		this.valueType = TokenType.valueOf(in.readUTF());
	}
}