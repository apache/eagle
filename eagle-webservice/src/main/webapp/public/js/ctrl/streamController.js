/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

// =============================================================
// =                        Stream List                        =
// =============================================================
damControllers.controller('streamListCtrl', function(globalContent, damContent, $scope, $routeParams, $q, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.hideSite = true;

	$scope.streams = {};

	var _streamList = Entities.queryEntities("AlertStreamService", '@streamName=~".*"');
	var _streamSchemaList = Entities.queryEntities("AlertStreamSchemaService", '@streamName=~".*"');
	$scope.streamList = _streamList;
	$scope.streamSchemaList = _streamSchemaList;

	_streamList._promise.then(function() {
		$.each(_streamList, function(i, stream) {
			stream.metaList = [];
			$scope.streams[stream.tags.dataSource + "_" + stream.tags.streamName] = stream;
		});
	});

	$q.all([_streamList._promise, _streamSchemaList._promise]).then(function(res) {
		$.each(_streamSchemaList, function(i, meta) {
			$scope.streams[meta.tags.dataSource + "_" + meta.tags.streamName].metaList.push(meta);
		});
	});

	return;
	_streamList._promise.success(function() {
		$.each(_streamList, function(i, stream) {
			stream.metaList = Entities.queryEntities("StreamMetadataService", {streamName: stream.tags.streamName});
		});
	});
});