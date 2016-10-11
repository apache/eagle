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

	// TODO: Mock data
	var publishmentTypes = [
		{
			"type": "email",
			"className": "org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher",
			"description": "send alert to email",
			"enabled":true,
			"fields": [{"name":"sender"},{"name":"recipients"},{"name":"subject"},{"name":"smtp.server", "value":"host1"},{"name":"connection", "value":"plaintext"},{"name":"smtp.port", "value": "25"}]
		},
		{
			"type": "kafka",
			"className": "org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher",
			"description": "send alert to kafka bus",
			"enabled":true,
			"fields": [{"name":"kafka_broker","value":"sandbox.hortonworks.com:6667"},{"name":"topic"}]
		}
	];


	// ======================================================================================
	// =                                        Main                                        =
	// ======================================================================================
	eagleControllers.controller('alertCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Alert";
		$scope.getState = function() {
			return $wrapState.current.name;
		};
	});

	// ======================================================================================
	// =                                        Alert                                       =
	// ======================================================================================
	eagleControllers.controller('alertListCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.subTitle = "Explore Alerts";
	});

	// ======================================================================================
	// =                                       Policy                                       =
	// ======================================================================================
	eagleControllers.controller('policyListCtrl', function ($scope, $wrapState, PageConfig, Entity, UI) {
		PageConfig.subTitle = "Manage Policies";

		$scope.policyList = [];

		function updateList() {
			var list = Entity.queryMetadata("policies");
			list._then(function () {
				$scope.policyList = list;
			});
		}
		updateList();

		$scope.deletePolicy = function (item) {
			UI.deleteConfirm(item.name)(function (entity, closeFunc) {
				Entity.deleteMetadata("policies/" + item.name)._promise.finally(function () {
					closeFunc();
					$scope.policyList._refresh();
				});
			});
		};

		$scope.startPolicy = function (policy) {
			Entity
				.post("metadata/policies/" + encodeURIComponent(policy.name) + "/status/ENABLED", {})
				._then(updateList);
		};

		$scope.stopPolicy = function (policy) {
			Entity
				.post("metadata/policies/" + encodeURIComponent(policy.name) + "/status/DISABLED", {})
				._then(updateList);
		};
	});

	eagleControllers.controller('policyDetailCtrl', function ($scope, $wrapState, PageConfig, Entity, UI) {
		PageConfig.title = $wrapState.param.name;
		PageConfig.subTitle = "Detail";
		PageConfig.navPath = [
			{title: "Policy List", path: "/alert/policyList"},
			{title: "Detail"}
		];

		var policyList = Entity.queryMetadata("policies/" + encodeURIComponent($wrapState.param.name));
		policyList._promise.then(function () {
			$scope.policy = policyList[0];
			console.log("[Policy]", $scope.policy);

			if(!$scope.policy) {
				$.dialog({
					title: "OPS",
					content: "Policy '" + $wrapState.param.name + "' not found!"
				}, function () {
					$wrapState.go("alert.policyList");
				});
			} else {
				$scope.publisherList = Entity.queryMetadata("policies/" + encodeURIComponent($scope.policy.name) + "/publishments");
			}
		});
	});
}());
