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
package org.apache.eagle.dataproc.impl.storm.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.apache.eagle.dataproc.impl.storm.hdfs.HDFSSourcedStormSpoutProvider.HDFSSpout;

public class DataCollectionHDFSSpout extends HDFSSpout{

	private static final long serialVersionUID = 8775646842131298552L;
	private Config configContext;
	private TopologyContext _context; 
	SpoutOutputCollector _collector;
	private Map<String, Boolean> processFileMap = null; 
	private static final Logger LOG = LoggerFactory.getLogger(DataCollectionHDFSSpout.class);
	
	public DataCollectionHDFSSpout(Config configContext){
		this.configContext = configContext;
		processFileMap = new HashMap<String, Boolean>();
		LOG.info("DataCollectionHDFSSpout called");
		
	}
	
	public void copyFiles(){
		LOG.info("Inside listFiles()");
		Configuration conf = new Configuration(); 
		// _____________ TO TEST THAT CORRECT HADOOP JARs ARE INCLUDED __________________
		ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs();
		if(LOG.isDebugEnabled()) {
			for (URL url : urls) {
				LOG.debug(url.getFile());
			}
		}
		// _________________________________________
        String hdfsConnectionStr = configContext.getString("dataSourceConfig.hdfsConnnection");
        LOG.info("HDFS connection string: " + hdfsConnectionStr);
       
		String hdfsPath = configContext.getString("dataSourceConfig.hdfsPath");
		LOG.info("HDFS path: " + hdfsPath);
		 
		String copyToPath = configContext.getString("dataSourceConfig.copyToPath");
		LOG.info("copyToPath: " + copyToPath);
		String srcPathStr = new String("hdfs://" + hdfsConnectionStr + hdfsPath);
		Path srcPath = new Path(srcPathStr); 
		LOG.info("listFiles called");
		LOG.info("srcPath: " + srcPath);
		try {
			FileSystem fs = srcPath.getFileSystem(conf);
			/*CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf); 
			CompressionCodec codec = codecFactory.getCodec(srcPath);
			DataInputStream inputStream = new DataInputStream(codec.createInputStream(fs.open(srcPath)));
			*/
			
			Path destPath = new Path(copyToPath);
			LOG.info("Destination path: " + destPath);
			fs.copyToLocalFile(srcPath, destPath);
			LOG.info("Copy to local succeed");
			fs.close();
							
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private List<String> getAllFiles(String root, int level){
		
		List<String> lists = new ArrayList<String>();
		File rootFile = new File(root);
		File[] tempList = rootFile.listFiles();
		if(tempList == null)
			return lists; 
		
		for(File temp:tempList){
			if(temp.isDirectory())
				lists.addAll(getAllFiles(temp.getAbsolutePath(), ++level));
			else{
				if(temp.getName().endsWith(".gz") || temp.getName().endsWith(".csv"))
					lists.add(temp.getAbsolutePath());
			}
		}
		return lists;
			
	}
	
	public List<String> listFiles(){
		
		String copyToPath = configContext.getString("dataSourceConfig.copyToPath");
		LOG.info("Reading from: " + copyToPath);
		List<String> files = new ArrayList<String>();
		files = getAllFiles(copyToPath, 0); 
		return files;
	}
	
	@Override
	public void nextTuple() {
		LOG.info("Releasing nextTuple");
		List<String> files = listFiles();
		LOG.info("Files returned: " + files.size());
		String typeOfFile = configContext.getString("dataSourceConfig.fileFormat");
		LOG.info("typeOfFile returned: " + typeOfFile);
		
		for(String fileName:files){
			LOG.info("fileName: " + fileName);
			LOG.info("processFileMap.get(fileName): " + processFileMap.get(fileName));
			if(processFileMap.get(fileName) == null || processFileMap.get(fileName) == false){
				processFileMap.put(fileName, true);
				BufferedReader br = null; 
				Reader decoder = null;
				GZIPInputStream in = null; 
				InputStream inStream = null;
				
				try{
					if(typeOfFile.equalsIgnoreCase("GZIP")){
						in = new GZIPInputStream(new FileInputStream(new File(fileName)));
						decoder = new InputStreamReader(in);
					}else if(typeOfFile.equalsIgnoreCase("CSV")){
						inStream = new FileInputStream(new File(fileName)); 
						decoder = new InputStreamReader(inStream);
					}else{
						LOG.error("No known file type specified");
						continue;
					}
					
					br = new BufferedReader(decoder);
					int lineNo = 0; 
					String line = "";
					while((line = br.readLine())!= null){
						++lineNo;
			        	//String line = br.readLine();
			        	//loggerHDFSSpout.info("line number " + lineNo + "is: " + line);
			        	//if(line == null || line.equalsIgnoreCase(""))
			        	//	break;
			        	LOG.info("Emitting line from file: " + fileName);
			        	//_collector.emit(new ValuesArray(line), lineNo);
                        _collector.emit(Arrays.asList((Object)line));
			        	LOG.info("Emitted line no: " + lineNo + " and line: " + line);
						Utils.sleep(100);
					}
				}
				catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}finally{
					try {
						if(br != null)
							br.close();
						if(decoder != null)
							decoder.close();
						if(in != null)
							in.close();
						if(inStream != null)
							inStream.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}else{
				LOG.info("Processed the files before, already done! ");
				//Utils.sleep(10000);
			}
			
		}
		
	}
	
	public void fail(Object msgId) {
	    int transactionId = (Integer) msgId;
	    LOG.info(transactionId + " failed");
	}
	
	public void ack(Object msgId) {
	    int transactionId = (Integer) msgId;
	    LOG.info(transactionId + " acknowledged");
	}

	@Override
	public void open(Map arg0, TopologyContext context,
			SpoutOutputCollector collector) {
		 _collector = collector;
		 _context = context;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("line"));
	}
}
