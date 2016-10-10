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

	eagleControllers.controller('policyCreateCtrl', function ($scope, $wrapState, PageConfig, Entity) {
		PageConfig.subTitle = "Define Alert Policy";
		connectPolicyEditController({}, arguments);
	});
	eagleControllers.controller('policyEditCtrl', function ($scope, $wrapState, PageConfig, Entity) {
		PageConfig.subTitle = "Edit Alert Policy";
		var args = arguments;

		// TODO: Wait for backend data update
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

	function policyEditController(policy, $scope, $wrapState, PageConfig, Entity) {
		$scope.newPolicy = !policy.name;
		$scope.policyLock = false;

		$scope.publisherTypes = publisherTypes;

		$scope.selectedApplication = null;
		$scope.selectedStream = null;

		$scope.outputStream = "";

		$scope.partitionStream = null;
		$scope.partitionType = "GROUPBY";
		$scope.partitionColumns = {};

		$scope.publisherType = "org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher";
		$scope.publisher = {
			dedupIntervalMin: "PT1M"
		};
		$scope.publisherProps = {};

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
		}, policy);

		$scope.policy.definition = {
			type: $scope.policy.definition.type,
			value: $scope.policy.definition.value
		};

		console.log("[Policy]", $scope.policy);

		// =========================================================
		// =                      Check Logic                      =
		// =========================================================
		$scope.checkBasicInfo = function () {
			return !!$scope.policy.name;
		};

		$scope.checkAlertStream = function () {
			return $scope.checkBasicInfo() &&
				$scope.policy.inputStreams.length > 0 &&
				$scope.policy.outputStreams.length > 0;
		};

		$scope.checkNumber = function (str) {
			str = (str + "").trim();
			return str !== "" && common.number.isNumber(Number(str));
		};

		$scope.checkDefinition = function () {
			return $scope.checkAlertStream() &&
				!!$scope.policy.definition.value.trim() &&
				$scope.policy.parallelismHint > 0;
		};

		// =========================================================
		// =                        Stream                         =
		// =========================================================
		$scope.refreshStreamSelect = function() {
			var appStreamList;

			if(!$scope.selectedApplication) {
				$scope.selectedApplication = common.getKeys($scope.applications)[0];
			}

			appStreamList = $scope.applications[$scope.selectedApplication] || [];
			if(!common.array.find($scope.selectedStream, appStreamList)) {
				$scope.selectedStream = appStreamList[0].streamId;
			}
			if(!common.array.find($scope.partitionStream, $scope.policy.inputStreams)) {
				$scope.partitionStream = $scope.policy.inputStreams[0];
			}
		};

		$scope.streamList = Entity.queryMetadata("streams");
		$scope.streamList._then(function () {
			$scope.applications = {};

			$.each($scope.streamList, function (i, stream) {
				var list = $scope.applications[stream.dataSource] = $scope.applications[stream.dataSource] || [];
				list.push(stream);
			});

			console.log("=>", $scope.streamList);
			$scope.refreshStreamSelect();
		});

		$scope.getStreamList = function () {
			return common.array.minus($scope.streamList, $scope.policy.inputStreams, "streamId", "");
		};

		$scope.addStream = function () {
			$scope.policy.inputStreams.push($scope.selectedStream);
			$scope.refreshStreamSelect();
		};

		$scope.removeStream = function (streamId) {
			$scope.policy.inputStreams = common.array.remove(streamId, $scope.policy.inputStreams);
			$scope.refreshStreamSelect();
		};

		$scope.checkAddStream = function (streamId) {
			return !common.array.find(streamId, $scope.policy.inputStreams);
		};

		$scope.addOutputStream = function () {
			$scope.policy.outputStreams.push($scope.outputStream);
			$scope.outputStream = "";
		};

		$scope.removeOutputStream = function (streamId) {
			$scope.policy.outputStreams = common.array.remove(streamId, $scope.policy.outputStreams);
		};

		$scope.checkAddOutputStream = function () {
			return $scope.outputStream !== "" && !common.array.find($scope.outputStream, $scope.policy.outputStreams);
		};

		// =========================================================
		// =                      Definition                       =
		// =========================================================
		$scope.getPartitionColumns = function () {
			var stream = common.array.find($scope.partitionStream, $scope.streamList, "streamId");
			return (stream || {}).columns;
		};

		$scope.addPartition = function () {
			$scope.policy.partitionSpec.push({
				streamId: $scope.partitionStream,
				type: $scope.partitionType,
				columns: $.map($scope.getPartitionColumns(), function (column) {
					return $scope.partitionColumns[column.name] ? column.name : null;
				})
			});

			$scope.partitionColumns = {};
		};

		$scope.checkAddPartition = function () {
			var match = false;

			$.each($scope.getPartitionColumns(), function (i, column) {
				if($scope.partitionColumns[column.name]) {
					match = true;
					return false;
				}
			});

			return match;
		};

		$scope.removePartition = function (partition) {
			$scope.policy.partitionSpec = common.array.remove(partition, $scope.policy.partitionSpec);
		};

		// =========================================================
		// =                       Publisher                       =
		// =========================================================
		$scope.publisherList = [];

		if(!$scope.newPolicy) {
			$scope.publisherList = Entity.queryMetadata("policies/" + encodeURIComponent($scope.policy.name) + "/publishments");
		}

		$scope.addPublisher = function () {
			var publisherProps = {};
			$.each($scope.publisherTypes[$scope.publisherType], function (i, field) {
				publisherProps[field] = $scope.publisherProps[field] || "";
			});
			$scope.publisherList.push({
				name: $scope.publisher.name,
				type: $scope.publisherType,
				policyIds: [$scope.policy.name],
				properties: publisherProps,
				dedupIntervalMin: $scope.publisher.dedupIntervalMin,
				serializer : "org.apache.eagle.alert.engine.publisher.impl.StringEventSerializer"
			});
			$scope.publisher = {
				dedupIntervalMin: "PT1M"
			};
			$scope.publisherProps = {};
		};

		$scope.removePublisher = function (publisher) {
			$scope.publisherList = common.array.remove(publisher, $scope.publisherList);
		};

		$scope.checkAddPublisher = function () {
			return $scope.publisher.name &&
				!common.array.find($scope.publisher.name, $scope.publisherList, "name");
		};

		// =========================================================
		// =                         Policy                        =
		// =========================================================
		$scope.createPolicy = function () {
			// TODO: Need check the policy or publisher exist.

			$scope.policyLock = true;

			var policyPromise = Entity.create("metadata/policies", $scope.policy)._promise;
			var publisherPromiseList = $.map($scope.publisherList, function (publisher) {
				return Entity.create("metadata/publishments", publisher)._promise;
			});
			common.deferred.all(publisherPromiseList.concat(policyPromise)).then(function () {
				$.dialog({
					title: "Done",
					content: "Click confirm to go to the policy detail page."
				});
				$wrapState.go("policyDetail", {name: $scope.policy.name});
			}, function (failedList) {
				$.dialog({
					title: "OPS",
					content: $("<pre>").text(JSON.stringify(failedList, null, "\t"))
				});
				$scope.policyLock = false;
			});
		};
	}
})();
