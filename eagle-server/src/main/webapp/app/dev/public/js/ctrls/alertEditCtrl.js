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
		$scope.policyList = Entity.queryMetadata("policies");
		$scope.policyList._promise.then(function () {
			var policy = $scope.policyList.find(function (entity) {
				return entity.name === $wrapState.param.name;
			});

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
		$scope.selectedApplication = null;
		$scope.selectedStream = null;

		$scope.partitionStream = null;
		$scope.partitionType = "GROUPBY";
		$scope.partitionColumns = {};

		$scope.policy = common.merge({
			name: "",
			description: "",
			inputStreams: [],
			outputStreams: [],
			definition: {
				type: "siddhi",
				value: ""
			},
			partition: []
		}, policy);

		console.log("\n\n\n>>>", $scope.policy);

		// =========================================================
		// =                      Check Logic                      =
		// =========================================================
		$scope.checkBasicInfo = function () {
			return !!$scope.policy.name;
		};

		$scope.checkAlertStream = function () {
			return $scope.policy.inputStreams.length > 0;
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
		}

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
			return !!common.array.find(streamId, $scope.policy.inputStreams);
		};

		// =========================================================
		// =                      Definition                       =
		// =========================================================


		// =========================================================
		// =                         Mock                          =
		// =========================================================
		$scope.policy.name = "Mock";
		$scope.policy.inputStreams = ["hbase_audit_log_stream"];
	}
})();
