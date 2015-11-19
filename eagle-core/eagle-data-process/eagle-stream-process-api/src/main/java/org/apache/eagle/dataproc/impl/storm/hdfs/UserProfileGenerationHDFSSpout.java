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
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.core.StreamingProcessConstants;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.esotericsoftware.minlog.Log;

public class UserProfileGenerationHDFSSpout extends HDFSSourcedStormSpoutProvider.HDFSSpout {

	private static final long serialVersionUID = 2274234104008894386L;
	private Config configContext;
	private TopologyContext _context; 
	SpoutOutputCollector _collector;
	
	public class UserProfileData implements Serializable{
		private static final long serialVersionUID = -3315860110144736840L;
		private String user; 
		private List<String> dateTime = new ArrayList<String>(); 
		private List<Integer> hrInDay = new ArrayList<Integer>(); 
		private List<String> line = new ArrayList<String>();
		
		public String getUser() {
			return user;
		}
		public void setUser(String user) {
			this.user = user;
		}
		public String getDateTime(int index) {
			return dateTime.get(index);
		}
		public List<String> getDateTimes() {
			return dateTime;
		}
		public void setDateTime(String dateTime) {
			this.dateTime.add(dateTime);
		}
		public int getHrInDay(int index) {
			return hrInDay.get(index);
		}
		public List<Integer> getHrsInDay() {
			return hrInDay;
		}
		public void setHrInDay(int hrInDay) {
			this.hrInDay.add(hrInDay);
		}
		public String getLine(int index) {
			return line.get(index);
		}
		public List<String> getLines() {
			return line;
		}
		public void setLine(String line) {
			this.line.add(line);
		} 
		
	}
	
	private static final Logger LOG = LoggerFactory.getLogger(UserProfileGenerationHDFSSpout.class);
	
	public UserProfileGenerationHDFSSpout(Config configContext){
		this.configContext = configContext;
		LOG.info("UserProfileGenerationHDFSSpout called");
	}
	
	public void copyFiles(){
		LOG.info("Inside listFiles()");
		//Configuration conf = new Configuration();
		JobConf conf = new JobConf();
		// _____________ TO TEST THAT CORRECT HADOOP JARs ARE INCLUDED __________________
		ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs();
        if(LOG.isDebugEnabled()) {
			for (URL url : urls) {
				LOG.debug(url.getFile());
			}
		}
		// _________________________________________
        String hdfsConnectionStr = configContext.getString("dataSourceConfig.hdfsConnection");
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
			String userListFileName = configContext.getString("dataSourceConfig.userList");
			//loggerHDFSSpout.info("userListFileName: " + userListFileName);
			List<String> userList = getUser(userListFileName);
			for(String user:userList){
				Path finalSrcPath = new Path(srcPath.getName() + "/" + user);
				fs.copyToLocalFile(finalSrcPath, destPath);
			}
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
				if(temp.getName().endsWith(".csv"))
					lists.add(temp.getAbsolutePath());
			}
		}
		return lists;
			
	}
	
	public List<String> listFiles(String path){
		
		LOG.info("Reading from: " + path);
		List<String> files = new ArrayList<String>();
		files = getAllFiles(path, 0); 
		return files;
	}
	
	private List<String> getUser(String listFileName){
		List<String> userList = new ArrayList<String>();
		BufferedReader reader = null; 
		try{
			InputStream is = getClass().getResourceAsStream(listFileName);
			reader = new BufferedReader(new InputStreamReader(is));
			String line = ""; 
			while((line = reader.readLine()) != null){
				userList.add(line);
				LOG.info("User added:" + line);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				if(reader != null)
					reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return userList;
	}
	
	@Override
	public void nextTuple() {
		LOG.info("Releasing nextTuple");
		
		String userListFileName = configContext.getString("dataSourceConfig.userList");

		//loggerHDFSSpout.info("userListFileName: " + userListFileName);
		List<String> userList = getUser(userListFileName);
		//loggerHDFSSpout.info("user list size:" + userList.size());
		for(String user: userList){
			LOG.info("Processing user: " + user);
			String copyToPath = configContext.getString("dataSourceConfig.copyToPath");
			//loggerHDFSSpout.info("copyToPath: " + copyToPath);
			
			copyToPath +="/" + user; 
			List<String> files = listFiles(copyToPath);
			LOG.info("Files returned: " + files.size());
			String typeOfFile = configContext.getString("dataSourceConfig.fileFormat");
			//loggerHDFSSpout.info("typeOfFile returned: " + typeOfFile);
			UserProfileData usersProfileDataset = new UserProfileData();
				
			for(String fileName:files){
				LOG.info("FileName: " + fileName);
				usersProfileDataset.setDateTime(fileName.substring(fileName.lastIndexOf("/")+1, fileName.lastIndexOf(".")));
				BufferedReader br = null; 
				Reader decoder = null;
				InputStream inStream = null;
				
				try{
					inStream = new FileInputStream(new File(fileName));
					decoder = new InputStreamReader(inStream);
					br = new BufferedReader(decoder);
					int lineNo = 0; 
					String line = "";
					while((line = br.readLine())!= null){
						boolean containsFileHeader = configContext.getBoolean("dataSourceConfig.containsFileHeader");
						//loggerHDFSSpout.info("containsFileHeader returned: " + containsFileHeader);
						if(containsFileHeader == true && lineNo == 0){
							// ignore the header column
							lineNo++;
							continue;
						}
			        	//loggerHDFSSpout.info("emitting line from file: " + fileName);
			        	
						usersProfileDataset.setLine(line);
						usersProfileDataset.setHrInDay(lineNo);
			        	lineNo++;
					}
				}
				catch (Exception e) {
					Log.error("File operation failed");
					throw new IllegalStateException();
				}finally{
					try {
						if(br != null)
							br.close();
						if(decoder != null)
							decoder.close();
						if(inStream != null)
							inStream.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			usersProfileDataset.setUser(user);
			_collector.emit(new ValuesArray(user, "HDFSSourcedStormExecutor", usersProfileDataset));
        	LOG.info("Emitting data of length: " + usersProfileDataset.getLines().size());
			Utils.sleep(1000);
		}
		this.close();
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
		declarer.declare(new Fields(StreamingProcessConstants.EVENT_PARTITION_KEY, StreamingProcessConstants.EVENT_STREAM_NAME, StreamingProcessConstants.EVENT_ATTRIBUTE_MAP));
	}
	
	
}
