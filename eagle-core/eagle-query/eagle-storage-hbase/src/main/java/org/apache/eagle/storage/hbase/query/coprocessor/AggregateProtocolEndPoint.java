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
package org.apache.eagle.storage.hbase.query.coprocessor;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.*;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.RawAggregator;
import org.apache.eagle.query.aggregate.timeseries.TimeSeriesAggregator;
import org.apache.eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
import org.apache.eagle.common.DateTimeUtil;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//public abstract class AbstractAggregateEndPoint extends BaseEndpointCoprocessor{
public class AggregateProtocolEndPoint extends AggregateProtos.AggregateProtocol implements AggregateProtocol, Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        return this;
    }

    public AggregateProtocolEndPoint() {}

    protected void checkNotNull(Object obj,String name) {
		if(obj==null) throw new NullPointerException(name+" is null");
	}

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do nothing
    }

//    @Override
//	public ProtocolSignature getProtocolSignature(String protocol, long version, int clientMethodsHashCode) throws IOException {
//		if (AggregateProtocol.class.getName().equals(protocol)) {
////			return new ProtocolSignature(AggregateProtocol.VERSION, null);
//			return new ProtocolSignature(98l, null);
//		}
//		throw new IOException("Unknown protocol: " + protocol);
//	}

	protected HRegion getCurrentRegion(){
		return this.env.getRegion();
	}

	/**
	 * <pre>
	 * Region-unittest,\x82\xB4\x85\xC2\x7F\xFF\xFE\xB6\xC9jNG\xEE!\x5C3\xBB\xAE\xA1:\x05\xA5\xA9x\xB0\xA1"8\x05\xFB(\xD2VY\xDB\x9A\x06\x09\xA9\x98\xC2\xE3\x8D=,1413960230654.aaf2a6c9f2c87c196f43497243bb2424.
	 * RegionID-unittest,1413960230654
	 * </pre>
	 */
	protected String getLogHeader(){
		HRegion region = this.getCurrentRegion();
		return LOG.isDebugEnabled() ? String.format("Region-%s",region.getRegionNameAsString()):
				String.format("Region-%s,%d",region.getTableDesc().getNameAsString(),region.getRegionId());
	}

	protected class InternalReadReport {
		public InternalReadReport(long counter,long startTimestamp,long stopTimestamp){
			this.counter = counter;
			this.startTimestamp = startTimestamp;
			this.stopTimestamp = stopTimestamp;
		}
		public long getCounter() {
			return counter;
		}
		public void setCounter(long counter) {
			this.counter = counter;
		}

		public long getStartTimestamp() {
			return startTimestamp;
		}

		public void setStartTimestamp(long startTimestamp) {
			this.startTimestamp = startTimestamp;
		}

		public long getStopTimestamp() {
			return stopTimestamp;
		}

		public void setStopTimestamp(long stopTimestamp) {
			this.stopTimestamp = stopTimestamp;
		}

		private long counter;
		private long startTimestamp;
		private long stopTimestamp;
	}

	/**
	 * Asynchronous HBase scan read as entity
	 *
	 * @param scan
	 * @throws java.io.IOException
	 */
	protected InternalReadReport asyncStreamRead(EntityDefinition ed, Scan scan, EntityCreationListener listener) throws IOException {
//		_init();
		long counter = 0;
		long startTimestamp = 0;
		long stopTimestamp = 0;

		InternalScanner scanner = this.getCurrentRegion().getScanner(scan);
		List<Cell> results = new ArrayList<Cell>();
		try{
			boolean hasMoreRows;
			GenericMetricShadowEntity singleMetricEntity = null;
			do{
				hasMoreRows = scanner.next(results);
				Map<String, byte[]> kvMap = new HashMap<String, byte[]>();
				if(!results.isEmpty()){
					counter ++;
					byte[] row = results.get(0).getRow();
					long timestamp = RowkeyBuilder.getTimestamp(row, ed);
					
					// Min
					if(startTimestamp == 0 || startTimestamp > timestamp ){
						startTimestamp = timestamp;
					}
					
					// Max
					if(stopTimestamp == 0 || stopTimestamp < timestamp ){
						stopTimestamp = timestamp;
					}
					
					for(Cell kv:results){
						String qualifierName = Bytes.toString(kv.getQualifier());
//						Qualifier qualifier = null;
//						if(!ed.isTag(qualifierName)){
//							qualifier = ed.getQualifierNameMap().get(qualifierName);
//							if(qualifier == null){
//								LOG.error("qualifier for   " + qualifierName + " not exist");
//								throw new NullPointerException("qualifier for field "+qualifierName+" not exist");
//							}
//						}
						if(kv.getValue()!=null) kvMap.put(qualifierName ,kv.getValue());
					}
					
					// LOG.info("DEBUG: timestamp="+timestamp+", keys=["+StringUtils.join(kvMap.keySet(),",")+"]");
					
					InternalLog internalLog = HBaseInternalLogHelper.buildObject(ed, row, timestamp, kvMap);
					if(internalLog!=null){
						TaggedLogAPIEntity logAPIEntity = null;
						try {
							logAPIEntity = HBaseInternalLogHelper.buildEntity(internalLog, ed);
							if(logAPIEntity instanceof GenericMetricEntity){
								if(singleMetricEntity == null) singleMetricEntity = new GenericMetricShadowEntity();
								GenericMetricEntity e = (GenericMetricEntity)logAPIEntity;
								if(e.getValue()!=null) {
									int count = e.getValue().length;
									@SuppressWarnings("unused")
									Class<?> cls = ed.getMetricDefinition().getSingleTimestampEntityClass();
									for (int i = 0; i < count; i++) {
										long ts = logAPIEntity.getTimestamp() + i * ed.getMetricDefinition().getInterval();
										// exclude those entity which is not within the time range in search condition. [start, end)
										singleMetricEntity.setTimestamp(ts);
										singleMetricEntity.setTags(e.getTags());
										singleMetricEntity.setValue(e.getValue()[i]);
										// Min
										if (startTimestamp == 0 || startTimestamp > ts) startTimestamp = ts;
										// Max
										if (stopTimestamp == 0 || stopTimestamp < ts) stopTimestamp = ts;
										listener.entityCreated(singleMetricEntity);
									}
								}
							}else {
								// LOG.info("DEBUG: rowKey="+logAPIEntity.getEncodedRowkey());
								listener.entityCreated(logAPIEntity);
							}
						} catch (Exception e) {
							if(internalLog!=null) {
								LOG.error("Got exception to handle " + internalLog.toString() + ": " + e.getMessage(), e);
							}
							throw new IOException(e);
						}
					}else{
						LOG.error("Got null to parse internal log for row: " + row.length + " with fields: " + kvMap);
					}
					results.clear();
				}else{
					if(LOG.isDebugEnabled()) LOG.warn("Empty batch of KeyValue");
				}
			} while(hasMoreRows);
		}catch(IOException ex){
			LOG.error(ex.getMessage(),ex);
			throw ex;
		} finally {
            if(scanner != null) {
                scanner.close();
            }
		}
		return new InternalReadReport(counter,startTimestamp,stopTimestamp);
	}

	/**
	 * Asynchronous HBase scan read as RAW qualifier
	 *
	 * @param scan
	 * @param listener
	 * @throws Exception
	 */
	protected InternalReadReport asyncStreamRead(EntityDefinition ed, Scan scan, QualifierCreationListener listener) throws IOException {
//		_init();
		long counter = 0;
		long startTimestamp = 0;
		long stopTimestamp = 0;
		InternalScanner scanner = this.getCurrentRegion().getScanner(scan);
		List<Cell> results = new ArrayList<Cell>();
		try{
			boolean hasMoreRows;//false by default
			do{
				hasMoreRows = scanner.next(results);
				Map<String, byte[]> kvMap = new HashMap<String, byte[]>();
				if(!results.isEmpty()){
					counter ++;
					byte[] row = results.get(0).getRow();
//					if(ed.isTimeSeries()){
					long timestamp = RowkeyBuilder.getTimestamp(row,ed);
					// Min
					if(startTimestamp == 0 || startTimestamp > timestamp ){
						startTimestamp = timestamp;
					}
					// Max
					if(stopTimestamp == 0 || stopTimestamp < timestamp ){
						stopTimestamp = timestamp;
					}
//					}
					
					for(Cell kv:results){
						String qualifierName = Bytes.toString(kv.getQualifier());
						Qualifier qualifier = null;
						if(!ed.isTag(qualifierName)){
							qualifier = ed.getQualifierNameMap().get(qualifierName);
							if(qualifier == null){
								LOG.error("qualifier for field " + qualifierName + " not exist");
								throw new IOException(new NullPointerException("qualifier for field "+qualifierName+" is null"));
							}
							qualifierName = qualifier.getDisplayName();
						}
						if(kv.getValue()!=null) kvMap.put(qualifierName,kv.getValue());
					}
					
//					LOG.info("DEBUG: timestamp="+timestamp+", keys=["+StringUtils.join(kvMap.keySet(),",")+"]");

					if(!kvMap.isEmpty()) listener.qualifierCreated(kvMap);
					results.clear();
				}else{
					if(LOG.isDebugEnabled()) LOG.warn("Empty batch of KeyValue");
				}
			} while(hasMoreRows);
		} catch(IOException ex){
			LOG.error(ex.getMessage(),ex);
			throw ex;
		} finally {
            if(scanner != null) {
                scanner.close();
            }
		}

		return new InternalReadReport(counter,startTimestamp,stopTimestamp);
	}

    @Override
    public void aggregate(RpcController controller, AggregateProtos.AggregateRequest request, RpcCallback<AggregateProtos.AggregateResult> done) {
        AggregateResult result = null;
        try {
            result = this.aggregate(ProtoBufConverter.fromPBEntityDefinition(request.getEntityDefinition()),
                    ProtoBufConverter.fromPBScan(request.getScan()),
                    ProtoBufConverter.fromPBStringList(request.getGroupbyFieldsList()),
                    ProtoBufConverter.fromPBByteArrayList(request.getAggregateFuncTypesList()),
                    ProtoBufConverter.fromPBStringList(request.getAggregatedFieldsList())
            );
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        }
        try {
            done.run(ProtoBufConverter.toPBAggregateResult(result));
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert result to PB-based message",e);
        }
    }

    @Override
    public void timeseriesAggregate(RpcController controller, AggregateProtos.TimeSeriesAggregateRequest request, RpcCallback<AggregateProtos.AggregateResult> done) {
        AggregateResult result = null;
        try {
            result = this.aggregate(ProtoBufConverter.fromPBEntityDefinition(request.getEntityDefinition()),
                    ProtoBufConverter.fromPBScan(request.getScan()),
                    ProtoBufConverter.fromPBStringList(request.getGroupbyFieldsList()),
                    ProtoBufConverter.fromPBByteArrayList(request.getAggregateFuncTypesList()),
                    ProtoBufConverter.fromPBStringList(request.getAggregatedFieldsList()),
                    request.getStartTime(),
                    request.getEndTime(),
                    request.getIntervalMin()
            );
        } catch (IOException e) {
            LOG.error("Failed to convert result to PB-based message",e);
            ResponseConverter.setControllerException(controller, e);
        }
        try {
            done.run(ProtoBufConverter.toPBAggregateResult(result));
        } catch (IOException e) {
            LOG.error("Failed to convert result to PB-based message",e);
            ResponseConverter.setControllerException(controller, e);
        }
    }

    private final static Logger LOG = LoggerFactory.getLogger(AggregateProtocolEndPoint.class);
    /**
     *
     * @param entityDefinition
     * @param scan
     * @param groupbyFields
     * @param aggregateFuncTypes
     * @param aggregatedFields
     * @return
     * @throws Exception
     */
    @Override
    public AggregateResult aggregate(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypes, List<String> aggregatedFields) throws IOException {
//		LOG.info("Using coprocessor instance: "+this);
        checkNotNull(entityDefinition, "entityDefinition");
        String serviceName = entityDefinition.getService();
        LOG.info(this.getLogHeader() +" raw group aggregate on service: " + serviceName + " by: " + groupbyFields + " func: " + AggregateFunctionType.fromBytesList(aggregateFuncTypes) + " fields: " + aggregatedFields);
        if(LOG.isDebugEnabled()) LOG.debug("SCAN: "+scan.toJSON());
        long _start = System.currentTimeMillis();
        final RawAggregator aggregator = new RawAggregator(groupbyFields,AggregateFunctionType.fromBytesList(aggregateFuncTypes),aggregatedFields,entityDefinition);
        InternalReadReport report = this.asyncStreamRead(entityDefinition, scan, aggregator);

        List<GroupbyKeyValue> keyValues = aggregator.getGroupbyKeyValues();
        AggregateResult result = new AggregateResult();
        result.setKeyValues(keyValues);
        result.setStartTimestamp(report.getStartTimestamp());
        result.setStopTimestamp(report.getStopTimestamp());

        long _stop = System.currentTimeMillis();
        LOG.info(String.format("%s: scan = %d rows, group = %d keys, startTime = %d, endTime = %d, spend = %d ms", this.getLogHeader(),report.getCounter(),keyValues.size(),report.getStartTimestamp(),report.getStopTimestamp(),(_stop - _start)));

        return result;
    }

    /**
     * TODO: refactor time series aggregator to remove dependency of business logic entity class
     *
     * @param entityDefinition
     * @param scan
     * @param groupbyFields
     * @param aggregateFuncTypes
     * @param aggregatedFields
     * @param intervalMin
     * @return
     * @throws Exception
     */
    @Override
    public AggregateResult aggregate(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypes, List<String> aggregatedFields, long startTime,long endTime,long intervalMin) throws IOException {
//		LOG.info("Using coprocessor instance: "+this);
        checkNotNull(entityDefinition, "entityDefinition");
        String serviceName = entityDefinition.getService();
        LOG.info(this.getLogHeader() + " time series group aggregate on service: " + serviceName + " by: " + groupbyFields + " func: " + AggregateFunctionType.fromBytesList(aggregateFuncTypes) + " fields: " + aggregatedFields + " intervalMin: " + intervalMin +
                " from: " + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(startTime) + " to: " + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(endTime));
        if(LOG.isDebugEnabled()) LOG.debug("SCAN: "+scan.toJSON());
        long _start = System.currentTimeMillis();
        final TimeSeriesAggregator aggregator = new TimeSeriesAggregator(groupbyFields,AggregateFunctionType.fromBytesList(aggregateFuncTypes),aggregatedFields,startTime,endTime,intervalMin);
        InternalReadReport report = this.asyncStreamRead(entityDefinition, scan,aggregator);
        List<GroupbyKeyValue> keyValues = aggregator.getGroupbyKeyValues();

        AggregateResult result = new AggregateResult();
        result.setKeyValues(keyValues);
        result.setStartTimestamp(report.getStartTimestamp());
        result.setStopTimestamp(report.getStopTimestamp());

        long _stop = System.currentTimeMillis();
        LOG.info(String.format("%s: scan = %d rows, group = %d keys, startTime = %d, endTime = %d, spend = %d ms", this.getLogHeader(),report.getCounter(),keyValues.size(),report.getStartTimestamp(),report.getStopTimestamp(),(_stop - _start)));

        return result;
    }

//	/**
//	 * Initialization per aggregate RPC call
//	 */
//	private void _init(){
//		this.startTimestamp = 0;
//		this.stopTimestamp = 0;
//	}
//
//	// Min
//	private long startTimestamp;
//	// Max
//	private long stopTimestamp;
//
//	public long getStartTimestamp() {
//		return startTimestamp;
//	}
//	public long getStopTimestamp() {
//		return stopTimestamp;
//	}
}