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
damControllers.controller('streamListCtrl', function(globalContent, damContent, $scope, $route, $routeParams, $q, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.hideSite = true;

	$scope.streams = {};
	$scope._streamEntity;
	$scope._streamEntityLock = false;

	// =========================================== List ===========================================
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
			var _stream = $scope.streams[meta.tags.dataSource + "_" + meta.tags.streamName];
			if(_stream) {
				_stream.metaList.push(meta);
			} else {
				console.warn("[Meta] Stream not match:", meta.tags.dataSource + "_" + meta.tags.streamName);
			}
		});
	});

	// =========================================== Edit ===========================================
	$scope.showStreamEditor = function(stream) {
		$("#streamMDL").modal("show");
		setTimeout(function() {
			$("#dataSource").focus();
		}, 500);

		$scope._streamEntity = {
			dataSource: "",
			streamName: "",
			description: "",
			metaList: [],
		};
		if(stream) {
			$scope._streamEntity.srcStream = stream;
			$scope._streamEntity.dataSource = stream.tags.dataSource;
			$scope._streamEntity.streamName = stream.tags.streamName;
			$scope._streamEntity.desc = stream.desc;

			$scope._streamEntity.metaList = $.map(stream.metaList, function(meta) {
				return {
					srcMeta: meta,
					attrName: meta.tags.attrName,
					attrDisplayName: meta.attrDisplayName,
					attrType: meta.attrType,
					attrDescription: meta.attrDescription,
				};
			});
		}
	};

	$scope.deleteMeta = function(meta) {
		$.dialog({
			title: "Delete confirm",
			content: "<p>You are <strong class='text-red'>DELETING</strong> the meta '<strong>" + meta.attrName + "</strong>'!</p>" +
					"<p>Proceed to delete?</p>",
			buttons: [
				{name: "Delete", class: "btn btn-danger", value: true},
				{name: "Cancel", class: "btn btn-default", value: false},
			]
		}, function(ret) {
			if(!ret) return;

			common.array.remove(meta, $scope._streamEntity.metaList);
			$scope.$apply();
		});
	};

	$scope.checkUpdateStream = function() {
		if(!$scope._streamEntity || $scope._streamEntityLock) return false;

		var _pass = true;

		if(!$scope._streamEntity.dataSource) _pass = false;
		if(!$scope._streamEntity.streamName) _pass = false;

		$.each($scope._streamEntity.metaList, function(i, meta) {
			if(!meta.attrName || !meta.attrType) {
				_pass = false;
				return false;
			}
		});

		return _pass;
	};

	$scope.confirmDeleteStream = function() {
		$.dialog({
			title: "Delete Confirm",
			content: "<span class='text-red fa fa-exclamation-triangle pull-left' style='font-size: 50px;'></span>" +
					"<p>You are <strong class='text-red'>DELETING</strong> the stream '<strong>" + $scope._streamEntity.streamName + "</strong>'!</p>" +
					"<p>Proceed to delete?</p>",
			buttons: [
				{name: "Delete", class: "btn btn-danger", value: true},
				{name: "Cancel", class: "btn btn-default", value: false},
			]
		}, function(ret) {
			if(!ret) return;

			var _promiseStream = Entities.deleteEntities("AlertStreamService", {
				dataSource: $scope._streamEntity.dataSource,
				streamName: $scope._streamEntity.streamName,
			})._promise;
			var _promiseStreamSchema = Entities.deleteEntities("AlertStreamSchemaService", {
				dataSource: $scope._streamEntity.dataSource,
				streamName: $scope._streamEntity.streamName,
			})._promise;

			$q.all(_promiseStream, _promiseStreamSchema).then(function() {
				$("#streamMDL").modal("hide");

				setTimeout(function() {
					$route.reload();
				}, 500);
			});
		});
	};

	$scope.confirmUpateStream = function() {
		$scope._streamEntityLock = true;

		// Stream entity
		var _entity = {
			prefix: "alertStream",
			tags: {
				dataSource: $scope._streamEntity.dataSource,
				streamName: $scope._streamEntity.streamName,
			},
			desc: $scope._streamEntity.desc
		};

		// Merge original stream
		if($scope._streamEntity.srcStream) {
			$.extend(_entity, $scope._streamEntity.srcStream);
		}

		// Meta entities
		var _metaList = $.map($scope._streamEntity.metaList, function(meta) {
			return {
				prefix: "alertStreamSchema",
				attrType: meta.attrType,
				attrDisplayName: meta.attrDisplayName,
				attrDescription: meta.attrDescription,
				tags: {
					dataSource: _entity.tags.dataSource,
					streamName: _entity.tags.streamName,
					attrName: meta.attrName,
				},
			};
		});

		Entities.updateEntity("AlertStreamService", _entity)._promise.then(function() {
			Entities.deleteEntities("AlertStreamSchemaService", {
				dataSource: _entity.tags.dataSource,
				streamName: _entity.tags.streamName,
			})._promise.then(function() {
				Entities.updateEntity("AlertStreamSchemaService", _metaList)._promise.finally(function() {
					Site.refresh();
				});
			}).finally(function() {
				$("#streamMDL").modal("hide");
				$scope._streamEntityLock = false;

				setTimeout(function() {
					$route.reload();
				}, 500);
			});
		}, function() {
			$scope._streamEntityLock = false;
		});
	};
});