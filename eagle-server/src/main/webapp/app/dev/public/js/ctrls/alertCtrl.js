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
	// =                                        Alert                                       =
	// ======================================================================================
	eagleControllers.controller('alertListCtrl', function ($scope, $wrapState, $interval, PageConfig, Entity) {
		PageConfig.title = "Alerts";

		$scope.alertList = Entity.queryMetadata("alerts", {size: 10000});

		// ================================================================
		// =                             Sync                             =
		// ================================================================
		var refreshInterval = $interval($scope.alertList._refresh, 1000 * 10);
		$scope.$on('$destroy', function() {
			$interval.cancel(refreshInterval);
		});
	});

	eagleControllers.controller('alertDetailCtrl', function ($scope, $wrapState, PageConfig, Entity) {
		PageConfig.title = "Alert Detail";

		$scope.alertList = Entity.queryMetadata("alerts/" + encodeURIComponent($wrapState.param.alertId));
		$scope.alertList._then(function () {
			$scope.alert = $scope.alertList[0];
			if(!$scope.alert) {
				$.dialog({
					title: "OPS",
					content: "Alert '" + $wrapState.param.alertId + "' not found!"
				});
			}
		});
	});

	// ======================================================================================
	// =                                       Stream                                       =
	// ======================================================================================
	eagleControllers.controller('alertStreamListCtrl', function ($scope, $wrapState, PageConfig, Application) {
		PageConfig.title = "Streams";

		$scope.streamList = $.map(Application.list, function (app) {
			return (app.streams || []).map(function (stream) {
				return {
					streamId: stream.streamId,
					appType: app.descriptor.type,
					siteId: app.site.siteId,
					schema: stream.schema
				};
			});
		});
	});

	// ======================================================================================
	// =                                       Policy                                       =
	// ======================================================================================
	eagleControllers.controller('policyListCtrl', function ($scope, $wrapState, PageConfig, Entity, Policy) {
		PageConfig.title = "Policies";

		$scope.policyList = [];

		function updateList() {
			var list = Entity.queryMetadata("policies");
			list._then(function () {
				$scope.policyList = list;
			});
		}
		updateList();

		$scope.deletePolicy = function(policy) {
			Policy.delete(policy).then(updateList);
		};

		$scope.startPolicy = function(policy) {
			Policy.start(policy).then(updateList);
		};

		$scope.stopPolicy = function(policy) {
			Policy.stop(policy).then(updateList);
		};
	});

	eagleControllers.controller('policyDetailCtrl', function ($scope, $wrapState, PageConfig, Entity, Policy) {
		PageConfig.title = "Policy";
		PageConfig.subTitle = "Detail";
		PageConfig.navPath = [
			{title: "Policy List", path: "/policies"},
			{title: "Detail"}
		];

		function updatePolicy() {
			var policyName = $wrapState.param.name;
			var encodePolicyName = encodeURIComponent(policyName);
			var policyList = Entity.queryMetadata("policies/" + encodePolicyName);
			policyList._promise.then(function () {
				$scope.policy = policyList[0];
				console.log("[Policy]", $scope.policy);

				if(!$scope.policy) {
					$.dialog({
						title: "OPS",
						content: "Policy '" + $wrapState.param.name + "' not found!"
					}, function () {
						$wrapState.go("policyList");
					});
					return;
				}

				Entity.post("metadata/policies/parse", $scope.policy.definition.value)._then(function (res) {
					$scope.executionPlan = res.data;
				});
			});

			$scope.policyPublisherList = Entity.queryMetadata("policies/" + encodePolicyName + "/publishments/");

			Entity.queryMetadata("schedulestates")._then(function (res) {
				var schedule = res.data || {};
				$scope.assignment = common.array.find(policyName, schedule.assignments, ["policyName"]) || {};

				var queueList = $.map(schedule.monitoredStreams, function (stream) {
					return stream.queues;
				});
				$scope.queue = common.array.find($scope.assignment.queueId, queueList, ["queueId"]);
			});
		}
		updatePolicy();

		$scope.deletePolicy = function() {
			Policy.delete($scope.policy).then(function () {
				$wrapState.go("policyList");
			});
		};

		$scope.startPolicy = function() {
			Policy.start($scope.policy).then(updatePolicy);
		};

		$scope.stopPolicy = function() {
			Policy.stop($scope.policy).then(updatePolicy);
		};
	});
}());
