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

(function() {
	'use strict';

	var eagleControllers = angular.module('eagleControllers');

	var publisherTypes = {
		'org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher': ["subject", "template", "sender", "recipients", "mail.smtp.host", "connection", "mail.smtp.port"],
		'org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher': ["topic", "kafka_broker", "rawAlertNamespaceLabel", "rawAlertNamespaceValue"],
		'org.apache.eagle.alert.engine.publisher.impl.AlertSlackPublisher': ["token", "channels", "severitys", "urltemplate"]
	};

	// ======================================================================================
	// =                                    Policy Create                                   =
	// ======================================================================================
	function connectPolicyEditController(entity, args) {
		var newArgs = [entity];
		Array.prototype.push.apply(newArgs, args);
		/* jshint validthis: true */
		policyEditController.apply(this, newArgs);
	}

	eagleControllers.controller('policyCreateCtrl', function ($scope, $wrapState, $timeout, PageConfig, Entity) {
		PageConfig.title = "Define Policy";
		connectPolicyEditController({}, arguments);
	});
	eagleControllers.controller('policyEditCtrl', function ($scope, $wrapState, $timeout, PageConfig, Entity) {
		PageConfig.title = "Edit Policy";
		var args = arguments;

		$scope.policyList = Entity.queryMetadata("policies/" + encodeURIComponent($wrapState.param.name));
		$scope.policyList._promise.then(function () {
			var policy = $scope.policyList[0];

			if(policy) {
				connectPolicyEditController(policy, args);
			} else {
				$.dialog({
					title: "OPS",
					content: "Policy '" + $wrapState.param.name + "' not found!"
				}, function () {
					$wrapState.go("alert.policyList");
				});
			}
		});
	});

	function policyEditController(policy, $scope, $wrapState, $timeout, PageConfig, Entity) {
		$scope.policy = policy;
		$scope.policy = common.merge({
			name: "",
			description: "",
			inputStreams: [],
			outputStreams: [],
			definition: {
				type: "siddhi",
				value: ""
			},
			partitionSpec: [],
			parallelismHint: 2
		}, $scope.policy);
		console.log("[Policy]", $scope.policy);

		var cacheSearchSourceKey;
		var searchApplications;
		$scope.searchSourceKey = "";
		$scope.applications = {};

		// ==============================================================
		// =                             UI                             =
		// ==============================================================
		$scope.sourceTab = "all";
		$scope.setSourceTab = function (tab) {
			$scope.sourceTab = tab;
		};

		// ==============================================================
		// =                           Stream                           =
		// ==============================================================
		$scope.getSearchApplication = function() {
			if(cacheSearchSourceKey !== $scope.searchSourceKey.toUpperCase()) {
				cacheSearchSourceKey = $scope.searchSourceKey.toUpperCase();

				searchApplications = {};
				$.each($scope.applications, function (appName, streams) {
					if(appName.toUpperCase().indexOf(cacheSearchSourceKey) >= 0) {
						searchApplications[appName] = streams;
					} else {
						var streamList = [];
						$.each(streams, function (i, stream) {
							if(stream.streamId.toUpperCase().indexOf(cacheSearchSourceKey) >= 0) {
								streamList.push(stream);
							}
						});

						if(streamList.length > 0) {
							searchApplications[appName] = streamList;
						}
					}
				});
			}
			return searchApplications;
		};

		$scope.streams = {};
		$scope.streamList = Entity.queryMetadata("streams");
		$scope.streamList._then(function () {
			$scope.applications = {};

			$.each($scope.streamList, function (i, stream) {
				var list = $scope.applications[stream.dataSource] = $scope.applications[stream.dataSource] || [];
				list.push(stream);
				$scope.streams[stream.streamId] = stream;
			});

			console.log($scope.applications);
		});

		$scope.isInputStreamSelected = function (streamId) {
			return $.inArray(streamId, $scope.policy.inputStreams) >= 0;
		};

		$scope.checkInputStream = function (streamId) {
			if($scope.isInputStreamSelected(streamId)) {
				$scope.policy.inputStreams = common.array.remove(streamId, $scope.policy.inputStreams);
			} else {
				$scope.policy.inputStreams.push(streamId);
			}
		};

		// ==============================================================
		// =                         Definition                         =
		// ==============================================================
		var checkPromise;
		$scope.checkDefinition = function () {
			$timeout.cancel(checkPromise);
			checkPromise = $timeout(function () {
				/* Entity.post("metadata/policies/validate", $scope.policy)._then(function (res) {
					// TODO: Wait for OPT
					console.log("Validate:", res.data.message);
				}); */
			}, 350);
		};

		// ==============================================================
		// =                         Partition                          =
		// ==============================================================
		$scope.partition = {};

		$scope.newPartition = function () {
			$scope.partition = {
				streamId: $scope.policy.inputStreams[0],
				type: "GROUPBY",
				columns: []
			};
			$(".modal[data-id='partitionMDL']").modal();
		};

		$scope.newPartitionCheckColumn = function (column) {
			return $.inArray(column, $scope.partition.columns) >= 0;
		};

		$scope.newPartitionClickColumn = function (column) {
			if($scope.newPartitionCheckColumn(column)) {
				$scope.partition.columns = common.array.remove(column, $scope.partition.columns);
			} else {
				$scope.partition.columns.push(column);
			}
		};

		$scope.addPartitionConfirm = function () {
			$scope.policy.partitionSpec.push($scope.partition);
		};

		$scope.removePartition = function (partition) {
			$scope.policy.partitionSpec = common.array.remove(partition, $scope.policy.partitionSpec);
		};
	}
})();
