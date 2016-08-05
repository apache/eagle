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
package org.apache.eagle.service.security.hdfs.rest;
import com.mapr.fs.clicommands.MapRCliCommands;
import com.typesafe.config.Config;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.eagle.security.resolver.MetadataAccessConfigRepo;
import org.apache.eagle.service.security.hdfs.MAPRFSResourceConstants;
import org.apache.eagle.service.security.hdfs.resolver.MAPRStatusCodeResolver;
import org.apache.hadoop.conf.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Path(MAPRFSResourceConstants.MAPRFS_NAME_RESOLVER)
public class MapRNameResolver {
    private static Logger LOG = LoggerFactory.getLogger(MapRNameResolver.class);

    private  String run(String[] cmds) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;
        System.setOut(ps);
        mprcmd.run(cmds);
        System.out.flush();
        System.setOut(old);
        return baos.toString().trim();
    }

    private String extractVolumeId(JSONObject response){
        String volumeId = "VOLUME ID NOT FOUND" ;
        JSONArray list = (JSONArray) response.get("data");
        if(list.length()!=0) {
            JSONObject jsonObject = (JSONObject) list.get(0);
            volumeId = jsonObject.getString("volumeid");
        }
        return volumeId;
    }

    private Configuration maprConfig;

    private MapRCliCommands mprcmd;


/**
 * rest api : convert file/folder name to id
 * @param fName file/folder name
 * @param site sitename
 * */
    @Path(MAPRFSResourceConstants.MAPRFS_FNAME_RESOLVER)
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String getFid(@QueryParam("fName") String fName, @QueryParam("site") String site) {
        String ans = "FID NOT FOUND";
        try{
            MetadataAccessConfigRepo repo = new MetadataAccessConfigRepo();
            Config typeSafeConfig= repo.getConfig(MAPRFSResourceConstants.MAPRFS_APPLICATION, site);
            String defaultFS;
            defaultFS = typeSafeConfig.getString("fs.defaultFS");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS",defaultFS);
            mprcmd = new MapRCliCommands(conf);

            ans = fNameToFid(fName);
            return ans;
        }catch (Exception e){
            LOG.info("maprfs: can not convert file/dir name " + fName + "to fid", e);
            ans = "CAN NOT RESOLVE THIS FID";
        }
        return ans;
    }


    /**
     * rest api : convert status code  to id
     * @param sName: status name
     * @param site site name
     * */
    @Path(MAPRFSResourceConstants.MAPRFS_SNAME_RESOLVER)
    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String getSid(@QueryParam("sName") String sName,  @QueryParam("site") String site) {
        String ans= "STATUS CODE ID NOT FOUND";
        try{
            MAPRStatusCodeResolver resolver = new MAPRStatusCodeResolver();
            ans = resolver.getStatusCodeID(sName);
        }catch (Exception e){
            LOG.info("maprfs: can not convert status code to id", e);
            ans = "CAN NOT RESOLVE THIS STATUS CODE";
        }
        return ans;
    }


    /**
     * rest api : convert volume name to id by calling mapr's rest api
     * @param vName volume name
     * @param site site Name
     * */
    @Path(MAPRFSResourceConstants.MAPRFS_VNAME_RESOLVER)
    @GET
    // The Java method will produce content identified by the MIME Media type "text/plain"
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public String getVid(@QueryParam("vName") String vName, @QueryParam("site") String site) {
        String ans= "VOLUME ID NOT FOUDN";
        try{
            // call mapr rest api to get corresponding id;
            MetadataAccessConfigRepo repo = new MetadataAccessConfigRepo();
            Config typeSafeConfig= repo.getConfig(MAPRFSResourceConstants.MAPRFS_APPLICATION, site);
            String username;
            String password;
            username = typeSafeConfig.getString(MAPRFSResourceConstants.MAPRFS_USERNAME);
            password = typeSafeConfig.getString(MAPRFSResourceConstants.MAPRFS_PASSWORD);
            // call
            String restUrl = typeSafeConfig.getString(MAPRFSResourceConstants.MAPRFS_WEBUI_HTTPS) + MAPRFSResourceConstants.MAPRFS_VOLUME_INFO + "?name=" + vName;

            JSONObject response = HttpRequest.executeGet(restUrl,username,password);
            ans = extractVolumeId(response);

        }catch (Exception e){
            LOG.info("maprfs: can not convert volume name" + vName + " to id", e);
        }
        return ans;
    }


    public  String fidToFname(String id) throws Exception {
        String name = "FNAME NOT FOUND";
        String cmds[] = {"-lsfid",id};
        String results = run(cmds);
        Pattern pattern = Pattern.compile("\\s(/.*)\\s");
        Matcher matcher = pattern.matcher(results);
        if (matcher.find()){
            name = matcher.group(1);
        }
        return name;
    }

    public String fNameToFid(String name) throws Exception {
        String id = "FID NOT FOUND";
        name = "maprfs://"+name;
        String cmds[] = {"-lsd",name};
        String results = run(cmds);
        Pattern pattern = Pattern.compile("\\sp\\s([\\d\\.]+)\\s");
        Matcher matcher = pattern.matcher(results);
        if (matcher.find()){
            id = matcher.group(1);
        }
        return id;
    }





}
