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

		// TODO: real api required
		$scope.alertList = [];
		for(var i = 0 ; i < 1000 ; i += 1) {
			$scope.alertList.push({
				streamId: "HBASE_AUDIT_LOG_STREAM_APOLLO",
				policyId: "Mock",
				alertTimestamp: +new Date() - i * 1000,
				alertData: {key1: "value1", key2: "value2", key3: "value3"}
			});
		}
	});

	eagleControllers.controller('alertDetailCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Alert Detail";

		$scope.alert = {
			alertId: "TODO: this is a mock",
			streamId: "HBASE_AUDIT_LOG_STREAM_APOLLO",
			policyId: "Mock",
			alertTimestamp: +new Date(),
			alertData: {key1: "value1", key2: "value2", key3: "value3"}
		};
	});

	// ======================================================================================
	// =                                       Stream                                       =
	// ======================================================================================
	eagleControllers.controller('alertStreamListCtrl', function ($scope, $wrapState, PageConfig, Application) {
		PageConfig.title = "Alert";
		PageConfig.subTitle = "Streams";

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
