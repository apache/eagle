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
package org.apache.eagle.service.generic;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.log.entity.meta.MetricDefinition;
import com.sun.jersey.api.model.AbstractResource;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.api.model.AbstractSubResourceMethod;
import com.sun.jersey.server.impl.modelapi.annotation.IntrospectionModeller;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

/**
 * @since : 7/3/14,2014
 */
@Path(MetadataResource.PATH_META)
public class MetadataResource {
	final static String PATH_META = "meta";
	final static String PATH_RESOURCE = "resource";
	final static String PATH_SERVICE = "service";

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response index(@Context Application application,
                        @Context HttpServletRequest request){
		String basePath = request.getRequestURL().toString();
		ObjectNode root = JsonNodeFactory.instance.objectNode();

		root.put(PATH_RESOURCE,joinUri(basePath,PATH_RESOURCE));
		root.put(PATH_SERVICE,joinUri(basePath,PATH_SERVICE));
		return Response.ok().entity(root).build();
	}

	@GET
	@Path(PATH_RESOURCE)
	@Produces(MediaType.APPLICATION_JSON)
	public Response listAllResourcesRoutes(@Context Application application,
	                                       @Context HttpServletRequest request){
		String basePath = request.getRequestURL().toString();
		basePath = basePath.substring(0,basePath.length() - PATH_META.length() - PATH_RESOURCE.length() -1);
		ObjectNode root = JsonNodeFactory.instance.objectNode();
		root.put("base",basePath);
		ArrayNode resources = JsonNodeFactory.instance.arrayNode();
		root.put( "resources", resources );

		for ( Class<?> aClass : application.getClasses()){
			if ( isAnnotatedResourceClass(aClass)){
				AbstractResource resource = IntrospectionModeller.createResource(aClass);
				ObjectNode resourceNode = JsonNodeFactory.instance.objectNode();
				String uriPrefix = resource.getPath().getValue();

				for ( AbstractSubResourceMethod srm : resource.getSubResourceMethods() ) {
					String uri = uriPrefix + "/" + srm.getPath().getValue();
					addTo( resourceNode, uri, srm, joinUri(basePath, uri) );
				}

				for ( AbstractResourceMethod srm : resource.getResourceMethods() ) {
					addTo( resourceNode, uriPrefix, srm, joinUri( basePath, uriPrefix ) );
				}
				resources.add( resourceNode );
			}
		}

		return Response.ok().entity( root ).build();
	}

	private String joinUri(String basePath, String uriPrefix) {
		if(basePath.endsWith("/") && uriPrefix.startsWith("/")){
			return basePath.substring(0,basePath.length()-2)+uriPrefix;
		}else if(basePath.endsWith("/") || uriPrefix.startsWith("/")){
			return basePath+ uriPrefix;
		}
		return basePath+"/"+uriPrefix;
	}

	private void addTo( ObjectNode resourceNode, String uriPrefix, AbstractResourceMethod srm, String path ){
		if(resourceNode.get( uriPrefix ) == null){
			ObjectNode inner = JsonNodeFactory.instance.objectNode();
			inner.put("path", path);
			inner.put("methods", JsonNodeFactory.instance.arrayNode());
			resourceNode.put( uriPrefix, inner );
		}

		((ArrayNode) resourceNode.get( uriPrefix ).get("methods")).add( srm.getHttpMethod() );
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private boolean isAnnotatedResourceClass( Class rc ){
		if ( rc.isAnnotationPresent( Path.class ) ) {
			return true;
		}

		for ( Class i : rc.getInterfaces() ) {
			if ( i.isAnnotationPresent( Path.class ) ) {
				return true;
			}
		}

		return false;
	}

	@GET
	@Path(PATH_SERVICE)
	@Produces(MediaType.APPLICATION_JSON)
	public Response listAllEntities(@Context Application application,
                                     @Context HttpServletRequest request) throws Exception {
		Map<String,EntityDefinition> entities = EntityDefinitionManager.entities();
		ObjectNode root = JsonNodeFactory.instance.objectNode();

		ArrayNode services = JsonNodeFactory.instance.arrayNode();

		for(Map.Entry<String,EntityDefinition> entry : entities.entrySet()){
//			ObjectNode serviceNode = JsonNodeFactory.instance.objectNode();
//			serviceNode.put(entry.getKey(),entityDefationitionAsJson(entry.getValue()));
			services.add(entityDefationitionAsJson(entry.getValue()));
		}
		root.put("count",entities.keySet().size());
		root.put("services",services);
		return Response.ok().entity(root).build();
	}

	private JsonNode entityDefationitionAsJson(EntityDefinition def) {
		ObjectNode node = JsonNodeFactory.instance.objectNode();
		node.put("service",def.getService());
		node.put("entityClass",def.getEntityClass().getName());
		node.put("table",def.getTable());
		node.put("columnFamily",def.getColumnFamily());
		node.put("prefix",def.getPrefix());
		if(def.getPartitions()!=null){
			node.put("partitions",arrayNode(def.getPartitions()));
		}
		node.put("isTimeSeries",def.isTimeSeries());

		MetricDefinition mdf = def.getMetricDefinition();
		if(mdf!=null){
			node.put("interval", mdf.getInterval());
		}

		IndexDefinition[] indexDef = def.getIndexes();
		if(indexDef!=null){
			ArrayNode indexDefArray = JsonNodeFactory.instance.arrayNode();
			for(IndexDefinition idef : indexDef){
				ObjectNode idn = JsonNodeFactory.instance.objectNode();
				idn.put("indexPrefix",idef.getIndexPrefix());

				if(idef.getIndex()!=null){
					ObjectNode index = JsonNodeFactory.instance.objectNode();
					index.put("name",idef.getIndex().name());
					index.put("columns",arrayNode(idef.getIndex().columns()));
					idn.put("index",index);
				}

				indexDefArray.add(idn);
			}
			node.put("indexs",indexDefArray);
		}
		return node;
	}

	private ArrayNode arrayNode(String[] values){
		ArrayNode an = JsonNodeFactory.instance.arrayNode();
		for(String v:values){
			an.add(v);
		}
		return an;
	}
}
