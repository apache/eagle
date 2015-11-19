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

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.EntityQualifierUtils;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.query.parser.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * the steps of building hbase filters
 * 1. receive ORExpression from eagle-antlr
 * 2. iterate all ANDExpression in ORExpression
 *    2.1 put each ANDExpression to a new filter list with MUST_PASS_ONE option
 *    2.2 iterate all AtomicExpression in ANDExpression
 *       2.2.1 group AtomicExpression into 2 groups by looking up metadata, one is for tag filters, the other is for column filters
 *       2.2.2 put the above 2 filters to a filter list with MUST_PASS_ALL option
 */
public class HBaseFilterBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(HBaseFilterBuilder.class);
	
	/**
	 * syntax is @<fieldname>
	 */
//	private static final String fnRegex = "^@(.*)$";
	private static final Pattern _fnPattern = TokenConstant.ID_PATTERN;// Pattern.compile(fnRegex);
	private static final Charset _defaultCharset = Charset.forName("ISO-8859-1");

	private ORExpression _orExpr;
	private EntityDefinition _ed;
	private boolean _filterIfMissing;
	private Charset _charset = _defaultCharset;

	/**
	 * TODO: Verify performance impact
	 *
	 * @return
	 */
	public Set<String> getFilterFields() {
		return _filterFields;
	}

	/**
	 * Just add filter fields for expression filter
	 */
	private Set<String> _filterFields;

	public HBaseFilterBuilder(EntityDefinition ed, ORExpression orExpr) {
		this(ed, orExpr, false);
	}
	
	public HBaseFilterBuilder(EntityDefinition ed, ORExpression orExpr, boolean filterIfMissing) {
		this._ed = ed;
		this._orExpr = orExpr;
		this._filterIfMissing = filterIfMissing;
	}
	
	public void setCharset(String charsetName){
		_charset = Charset.forName(charsetName);
	}
	
	public Charset getCharset(){
		return _charset;
	}
	
	/**
	 * Because we don't have metadata for tag, we regard non-qualifer field as tag. So one field possibly is not a real tag when this function return true. This happens
	 * when a user input an wrong field name which is neither tag or qualifier
	 *   
	 * @param field
	 */
	private boolean isTag(String field){
		return _ed.isTag(field);
	}
	
	/**
	 * check whether this field is one entity attribute or not 
	 * @param fieldName
	 * @return
	 */
	private String parseEntityAttribute(String fieldName){
		Matcher m = _fnPattern.matcher(fieldName);
		if(m.find()){
			return m.group(1);
		}
		return null;
	}

	/**
	 * Return the partition values for each or expression. The size of the returned list should be equal to
	 * the size of FilterList that {@link #buildFilters()} returns.
	 * 
	 * TODO: For now we don't support one query to query multiple partitions. In future if partition is defined, 
	 * for the entity, internally We need to spawn multiple queries and send one query for each partition.
	 * 
	 * @return Return the partition values for each or expression. Return null if the entity doesn't support
	 * partition
	 */
	public List<String[]> getPartitionValues() {
		final String[] partitions = _ed.getPartitions();
		if (partitions == null || partitions.length == 0) {
			return null;
		}
		final List<String[]> result = new ArrayList<String[]>();
		final Map<String, String> partitionKeyValueMap = new HashMap<String, String>();
		for(ANDExpression andExpr : _orExpr.getANDExprList()) {
			partitionKeyValueMap.clear();
			for(AtomicExpression ae : andExpr.getAtomicExprList()) {
				// TODO temporarily ignore those fields which are not for attributes
				if(ae.getKeyType() == TokenType.ID) {
					final String fieldName = parseEntityAttribute(ae.getKey());
					if (fieldName == null) {
						LOG.warn(fieldName + " field does not have format @<FieldName>, ignored");
						continue;
					}
					if (_ed.isPartitionTag(fieldName) && ComparisonOperator.EQUAL.equals(ae.getOp())) {
						final String value = ae.getValue();
						partitionKeyValueMap.put(fieldName, value);
					}
				}
			}
			final String[] values = new String[partitions.length];
			result.add(values);
			for (int i = 0; i < partitions.length; ++i) {
				final String partition = partitions[i];
				final String value = partitionKeyValueMap.get(partition);
				values[i] = value;
			}
		}
		return result;
	}

	/**
	 * @see org.apache.eagle.query.parser.TokenType
	 *
	 * @return
	 */
	public FilterList buildFilters(){
		// TODO: Optimize to select between row filter or column filter for better performance
		// Use row key filter priority by default
		boolean rowFilterPriority = true;

		FilterList fltList = new FilterList(Operator.MUST_PASS_ONE);
		for(ANDExpression andExpr : _orExpr.getANDExprList()){
			
			FilterList list = new FilterList(Operator.MUST_PASS_ALL);
			Map<String, List<String>> tagFilters = new HashMap<String, List<String>>();
			List<QualifierFilterEntity> qualifierFilters = new ArrayList<QualifierFilterEntity>();
//			List<QualifierFilterEntry> tagLikeQualifierFilters = new ArrayList<QualifierFilterEntry>();

			// TODO refactor not to use too much if/else
			for(AtomicExpression ae : andExpr.getAtomicExprList()){
				// TODO temporarily ignore those fields which are not for attributes

				String fieldName = ae.getKey();
				if(ae.getKeyType() == TokenType.ID){
					fieldName = parseEntityAttribute(fieldName);
					if(fieldName == null){
						LOG.warn(fieldName + " field does not have format @<FieldName>, ignored");
						continue;
					}
				}

				String value = ae.getValue();
				ComparisonOperator op = ae.getOp();
				TokenType keyType = ae.getKeyType();
				TokenType valueType = ae.getValueType();
				QualifierFilterEntity entry = new QualifierFilterEntity(fieldName,value,op,keyType,valueType);

				// TODO Exact match, need to add escape for those special characters here, including:
				// "-", "[", "]", "/", "{", "}", "(", ")", "*", "+", "?", ".", "\\", "^", "$", "|"

				if(keyType == TokenType.ID && isTag(fieldName)){
					if ((ComparisonOperator.EQUAL.equals(op) || ComparisonOperator.IS.equals(op))
							&& !TokenType.NULL.equals(valueType))
					{
						// Use RowFilter for equal TAG
						if(tagFilters.get(fieldName) == null) tagFilters.put(fieldName, new ArrayList<String>());
						tagFilters.get(fieldName).add(value);
					} else if (rowFilterPriority && ComparisonOperator.IN.equals(op))
					{
						// Use RowFilter here by default
						if(tagFilters.get(fieldName) == null) tagFilters.put(fieldName, new ArrayList<String>());
						tagFilters.get(fieldName).addAll(EntityQualifierUtils.parseList(value));
					} else if (ComparisonOperator.LIKE.equals(op)
						|| ComparisonOperator.NOT_LIKE.equals(op)
						|| ComparisonOperator.CONTAINS.equals(op)
						|| ComparisonOperator.NOT_CONTAINS.equals(op)
						|| ComparisonOperator.IN.equals(op)
						|| ComparisonOperator.IS.equals(op)
						|| ComparisonOperator.IS_NOT.equals(op)
						|| ComparisonOperator.NOT_EQUAL.equals(op)
						|| ComparisonOperator.EQUAL.equals(op)
						|| ComparisonOperator.NOT_IN.equals(op))
					{
						qualifierFilters.add(entry);
					} else
					{
						LOG.warn("Don't support operation: \"" + op + "\" on tag field: " + fieldName + " yet, going to ignore");
						throw new IllegalArgumentException("Don't support operation: "+op+" on tag field: "+fieldName+", avaliable options: =, =!, =~, !=~, in, not in, contains, not contains");
					}
				}else{
					qualifierFilters.add(entry);
				}
			}

			// Build RowFilter for equal tags
			list.addFilter(buildTagFilter(tagFilters));

			// Build SingleColumnValueFilter
			FilterList qualifierFilterList = buildQualifierFilter(qualifierFilters);
			if(qualifierFilterList != null && qualifierFilterList.getFilters().size()>0){
				list.addFilter(qualifierFilterList);
			}else {
				if(LOG.isDebugEnabled()) LOG.debug("Ignore empty qualifier filter from "+qualifierFilters.toString());
			}
			fltList.addFilter(list);
		}
		LOG.info("Query: " + _orExpr.toString() + " => Filter: " + fltList.toString());
		return fltList;
	}
	
	/**
	 * _charset is used to decode the byte array, in hbase server, RegexStringComparator uses the same
	 * charset to decode the byte array stored in qualifier
	 * for tag filter regex, it's always ISO-8859-1 as it only comes from String's hashcode (Integer)
	 * Note: regex comparasion is to compare String
	 */
	protected Filter buildTagFilter(Map<String, List<String>> tagFilters){
		RegexStringComparator regexStringComparator = new RegexStringComparator(buildTagFilterRegex(tagFilters));
		regexStringComparator.setCharset(_charset);
		RowFilter filter = new RowFilter(CompareOp.EQUAL, regexStringComparator);
		return filter;
	}
	
	/**
	 * all qualifiers' condition must be satisfied.
	 *
	 * <H1>Use RegexStringComparator for:</H1>
	 *      IN
	 *      LIKE
	 *      NOT_LIKE
	 *
	 * <H1>Use SubstringComparator for:</H1>
	 *      CONTAINS
	 *
	 * <H1>Use EntityQualifierHelper for:</H1>
	 *      EQUALS
	 *      NOT_EUQALS
	 *      LESS
	 *      LESS_OR_EQUAL
	 *      GREATER
	 *      GREATER_OR_EQUAL
	 *
	 * <H2>
	 *     TODO: Compare performance of RegexStringComparator ,SubstringComparator ,EntityQualifierHelper
	 * </H2>
	 *
	 * @param qualifierFilters
	 * @return
	 */
	protected FilterList buildQualifierFilter(List<QualifierFilterEntity> qualifierFilters){
		FilterList list = new FilterList(Operator.MUST_PASS_ALL);
		// iterate all the qualifiers
		for(QualifierFilterEntity entry : qualifierFilters){
			// if contains expression based filter
			if(entry.getKeyType() == TokenType.EXP
					|| entry.getValueType() == TokenType.EXP
					|| entry.getKeyType() != TokenType.ID){
				if(!EagleConfigFactory.load().isCoprocessorEnabled()) {
					LOG.warn("Expression in filter may not support, because custom filter and coprocessor is disabled: " + entry.toString());
				}
				list.addFilter(buildExpressionBasedFilter(entry));
				continue;
			}

			// else using SingleColumnValueFilter
			String qualifierName = entry.getKey();
			if(!isTag(entry.getKey())){
				Qualifier qualifier = _ed.getDisplayNameMap().get(entry.getKey());
				qualifierName = qualifier.getQualifierName();
			}

			// Comparator to be used for building HBase Filter
			// WritableByteArrayComparable comparator;
            ByteArrayComparable comparable;
			if(ComparisonOperator.IN.equals(entry.getOp())
				|| ComparisonOperator.NOT_IN.equals(entry.getOp())){
				Filter setFilter = buildListQualifierFilter(entry);
				if(setFilter!=null){
					list.addFilter(setFilter);
				}
			}else{
				// If [=,!=,is,is not] NULL, use NullComparator else throw exception
				if(TokenType.NULL.equals(entry.getValueType())){
					if(ComparisonOperator.EQUAL.equals(entry.getOp())
						||ComparisonOperator.NOT_EQUAL.equals(entry.getOp())
						||ComparisonOperator.IS.equals(entry.getOp())
						||ComparisonOperator.IS_NOT.equals(entry.getOp()))
                        comparable = new NullComparator();
					else
						throw new IllegalArgumentException("Operation: "+entry.getOp()+" with NULL is not supported yet: "+entry.toString()+", avaliable options: [=, !=, is, is not] null|NULL");
				}
				// If [contains, not contains],use SubstringComparator
				else if (ComparisonOperator.CONTAINS.equals(entry.getOp())
					|| ComparisonOperator.NOT_CONTAINS.equals(entry.getOp())) {
                    comparable = new SubstringComparator(entry.getValue());
				}
				// If [like, not like], use RegexStringComparator
				else if (ComparisonOperator.LIKE.equals(entry.getOp())
						|| ComparisonOperator.NOT_LIKE.equals(entry.getOp())){
					// Use RegexStringComparator for LIKE / NOT_LIKE
					RegexStringComparator _comparator = new RegexStringComparator(buildQualifierRegex(entry.getValue()));
					_comparator.setCharset(_charset);
                    comparable = _comparator;
				} else{
					Class type = EntityQualifierUtils.getType(_ed, entry.getKey());
					// if type is null (is Tag or not found) or not defined for TypedByteArrayComparator
					if(!EagleConfigFactory.load().isCoprocessorEnabled() || type == null || TypedByteArrayComparator.get(type) == null){
                        comparable = new BinaryComparator(EntityQualifierUtils.toBytes(_ed, entry.getKey(), entry.getValue()));
					}else {
                        comparable = new TypedByteArrayComparator(EntityQualifierUtils.toBytes(_ed, entry.getKey(), entry.getValue()),type);
					}
				}

				SingleColumnValueFilter filter =
						new SingleColumnValueFilter(_ed.getColumnFamily().getBytes(), qualifierName.getBytes(), convertToHBaseCompareOp(entry.getOp()), comparable);
				filter.setFilterIfMissing(_filterIfMissing);
				list.addFilter(filter);
			}
		}

		return list;
	}

	private Filter buildExpressionBasedFilter(QualifierFilterEntity entry) {
		BooleanExpressionComparator expressionComparator  = new BooleanExpressionComparator(entry,_ed);
		_filterFields = expressionComparator.getRequiredFields();
		RowValueFilter filter = new RowValueFilter(expressionComparator);
		return filter;
	}

	/**
	 * Currently use BinaryComparator only
	 * <h2>TODO: </h2>
	 * Possibility to tune performance by using: OR[BinaryComparator,...] instead of RegexStringComparator?
	 *
	 *<br/> <br/>
	 *
	 * ! Check op must be IN or NOTIN in caller
	 *
	 * @param entry
	 * @return
	 */
	private Filter buildListQualifierFilter(QualifierFilterEntity entry){
		List<String> valueSet = EntityQualifierUtils.parseList(entry.getValue());
		Iterator<String> it = valueSet.iterator();
		String fieldName = entry.getKey();
		String qualifierName = fieldName;
		if(!_ed.isTag(entry.getKey())){
			qualifierName = _ed.getDisplayNameMap().get(entry.getKey()).getQualifierName();
		}

// TODO: Try to use RegExp just work if possible
// Because single SingleColumnValueFilter is much faster than multi SingleColumnValueFilters in OR list.
//		Class qualifierType = EntityQualifierHelper.getType(_ed,fieldName);
//		if( qualifierType == null || qualifierType == String.class){
//			boolean first = true;
//			StringBuilder filterRegex = new StringBuilder();
//			filterRegex.append("^(");
//			while(it.hasNext()) {
//				String value = it.next();
//				if(value == null) {
//					logger.warn("ignore empty value in set qualifier filter: "+entry.toString());
//					continue;
//				}
//				if(!first) filterRegex.append("|");
//				filterRegex.append(value);
//				first = false;
//			}
//			filterRegex.append(")$");
//			RegexStringComparator regexStringComparator = new RegexStringComparator(filterRegex.toString());
//			return new SingleColumnValueFilter(_ed.getColumnFamily().getBytes(), qualifierName.getBytes(),
//					convertToHBaseCompareOp(entry.getOp()), regexStringComparator);
//		}else{
		FilterList setFilterList;
		if(ComparisonOperator.IN.equals(entry.getOp())){
			setFilterList = new FilterList(Operator.MUST_PASS_ONE);
		}else if(ComparisonOperator.NOT_IN.equals(entry.getOp())) {
			setFilterList = new FilterList(Operator.MUST_PASS_ALL);
		}else{
			throw new IllegalArgumentException(String.format("Don't support operation: %s on LIST type of value yet: %s, valid options: IN/NOT IN [LIST]",entry.getOp(),entry.toString()));
		}

		while(it.hasNext()) {
			String value = it.next();
			BinaryComparator comparator = new BinaryComparator(EntityQualifierUtils.toBytes(_ed, fieldName, value));
			SingleColumnValueFilter filter =
					new SingleColumnValueFilter(_ed.getColumnFamily().getBytes(), qualifierName.getBytes(), convertToHBaseCompareOp(entry.getOp()), comparator);
			filter.setFilterIfMissing(_filterIfMissing);
			setFilterList.addFilter(filter);
		}

		return setFilterList;
//		}
	}

	/**
	 * Just used for LIKE and NOT_LIKE
	 *
	 * @param qualifierValue
	 * @return
	 */
	protected String buildQualifierRegex(String qualifierValue){
		StringBuilder sb = new StringBuilder();
//		sb.append("(?s)");
		sb.append("^");
		sb.append(qualifierValue);
		sb.append("$");
		return sb.toString();
	}
	
	  /**
	   * Appends the given ID to the given buffer, followed by "\\E".
	   * [steal it from opentsdb, thanks opentsdb :) https://github.com/OpenTSDB/opentsdb/blob/master/src/core/TsdbQuery.java]
	   */
	  private static void addId(final StringBuilder buf, final byte[] id) {
		buf.append("\\Q");
	    boolean backslash = false;
	    for (final byte b : id) {
	      buf.append((char) (b & 0xFF));
	      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
	        // So we just terminated the quoted section because we just added \E
	        // to `buf'.  So let's put a litteral \E now and start quoting again.
	        buf.append("\\\\E\\Q");
	      } else {
	        backslash = b == '\\';
	      }
	    }
	    buf.append("\\E");
	  }
	  
	  @SuppressWarnings("unused")
	  private static void addId(final StringBuilder buf, final String id) {
		    buf.append("\\Q");
		  	int len = id.length()-1;
		    boolean backslash = false;
		    for (int i =0; i < len; i++) {
		      char c = id.charAt(i);
		      buf.append(c);
		      if (c == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
		        // So we just terminated the quoted section because we just added \E
		        // to `buf'.  So let's put a litteral \E now and start quoting again.
		        buf.append("\\\\E\\Q");
		      } else {
		        backslash = c == '\\';
		      }
		    }
		    buf.append("\\E");
		  }
	
	/**
	 * one search tag may have multiple values which have OR relationship, and relationship between
	 * different search tags is AND
	 * the query is like "(TAG1=value11 OR TAG1=value12) AND TAG2=value2"
	 * @param tags
	 * @return
	 */
	protected String buildTagFilterRegex(Map<String, List<String>> tags){
		// TODO need consider that \E could be part of tag, refer to https://github.com/OpenTSDB/opentsdb/blob/master/src/core/TsdbQuery.java
		final SortedMap<Integer, List<Integer>> tagHash = new TreeMap<Integer, List<Integer>>();
		final int numOfPartitionFields = (_ed.getPartitions() == null) ? 0 : _ed.getPartitions().length;
		for(Map.Entry<String, List<String>> entry : tags.entrySet()){
			String tagName = entry.getKey();
			// Ignore tag if the tag is one of partition fields
			if (_ed.isPartitionTag(tagName)) {
				continue;
			}
			List<String> stringValues = entry.getValue();
			List<Integer> hashValues = new ArrayList<Integer>(stringValues.size());
			for(String value : stringValues){
				hashValues.add(value.hashCode());
			}
			tagHash.put(tagName.hashCode(), hashValues);
		}
		
		// header = prefix(4 bytes) + partition_hashes(4*N bytes) + timestamp (8 bytes)
		final int headerLength = 4 + numOfPartitionFields * 4 + 8;

		// <tag1:4><value1:4> ... <tagn:4><valuen:4>
		StringBuilder sb = new StringBuilder();
		sb.append("(?s)");
		sb.append("^(?:.{").append(headerLength).append("})");
		sb.append("(?:.{").append(8).append("})*"); // for any number of tags
		for (Map.Entry<Integer, List<Integer>> entry : tagHash.entrySet()) {
			try {
				addId(sb, ByteUtil.intToBytes(entry.getKey()));
				List<Integer> hashValues = entry.getValue();
				sb.append("(?:");
				boolean first = true;
				for(Integer value : hashValues){
					if(!first){
						sb.append('|');
					}
					addId(sb, ByteUtil.intToBytes(value));
					first = false;
				}
				sb.append(")");
				sb.append("(?:.{").append(8).append("})*"); // for any number of tags
			} catch (Exception ex) {
				LOG.error("constructing regex error", ex);
			}
		}
		sb.append("$");
		if(LOG.isDebugEnabled()) LOG.debug("Tag filter pattern is " + sb.toString());
		return sb.toString();
	}

	/**
	 * Convert ComparisonOperator to native HBase CompareOp
	 *
	 * Support:
	 *      =, =~,CONTAINS,<,<=,>,>=,!=,!=~
	 *
	 * @param comp
	 * @return
	 */
	protected static CompareOp convertToHBaseCompareOp(ComparisonOperator comp) {
		if(comp == ComparisonOperator.EQUAL || comp == ComparisonOperator.LIKE
				|| comp == ComparisonOperator.CONTAINS
				|| comp == ComparisonOperator.IN
				|| comp == ComparisonOperator.IS
				) {
			return CompareOp.EQUAL;
		}else if(comp == ComparisonOperator.LESS) {
			return CompareOp.LESS;
		} else if(comp == ComparisonOperator.LESS_OR_EQUAL){
			return CompareOp.LESS_OR_EQUAL;
		}else if(comp == ComparisonOperator.GREATER) {
			return CompareOp.GREATER;
		} else if(comp == ComparisonOperator.GREATER_OR_EQUAL){
			return CompareOp.GREATER_OR_EQUAL;
		} else if(comp == ComparisonOperator.NOT_EQUAL
				|| comp == ComparisonOperator.NOT_LIKE
				|| comp == ComparisonOperator.NOT_CONTAINS
				|| comp == ComparisonOperator.IS_NOT
				|| comp == ComparisonOperator.NOT_IN)
		{
			return CompareOp.NOT_EQUAL;
		} else {
			LOG.error("{} operation is not supported now\n", comp);
			throw new IllegalArgumentException("Illegal operation: "+comp+ ", avaliable options: "+ Arrays.toString(ComparisonOperator.values()));
		}
	}

	protected static CompareOp getHBaseCompareOp(String comp) {
		return convertToHBaseCompareOp(ComparisonOperator.locateOperator(comp));
	}
}
