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

	var colorMapping = {
		WARNING: '#f39c12',
		CRITICAL: '#dd4b39',
		FATAL: '#dd4b39',
		OK: '#00a65a'
	};
	var eagleControllers = angular.module('eagleControllers');

	// ======================================================================================
	// =                                        Alert                                       =
	// ======================================================================================
	eagleControllers.controller('alertListCtrl', function ($q, $scope, $wrapState, PageConfig, CompatibleEntity, Time) {
		PageConfig.title = "Alert Incidents";
		$scope.site = $wrapState.param.siteId;

		$scope.alertList = [];
		$scope.loading = false;

		function loadAlerts() {
			$scope.loading = true;
			// Alert List
			var list = CompatibleEntity.query("LIST", {
				query: "AlertService",
				condition: {siteId: $scope.site},
				startTime: new Time('startTime'),
				endTime: new Time('endTime')
			});
			list._then(function () {
				$scope.alertList = list;
			});

			// Alert Trend
			$scope.alertTrend = [];
			var alertTrend = CompatibleEntity.timeSeries({
				condition: { siteId: $scope.site },
				groups: 'severity',
				fields: ['count'],
				query: 'AlertService',
				startTime: new Time('startTime'),
				endTime: new Time('endTime'),
				limit: 100000,
			});
			alertTrend._promise.then(function () {
				$scope.alertTrend = $.map(alertTrend, function (series) {
					var type = series.group.key[0];
					return $.extend({}, series, {
						name: type,
						type: 'bar',
						stack: 'severity',
						itemStyle: {
							normal: {
								color: colorMapping[type],
							},
						},
					});
				});
			});

			$q.all([list._promise, alertTrend._promise]).then(function () {
				$scope.loading = false;
			});
		}
		loadAlerts();

		Time.onReload(loadAlerts, $scope);
	});

	eagleControllers.controller('alertDetailCtrl', function ($sce, $scope, $wrapState, PageConfig, CompatibleEntity, Time) {
		PageConfig.title = "Alert Detail";

		$scope.site = $wrapState.param.siteId;

		var endTime = new Time($wrapState.param.timestamp).add(1, 'd');
		var startTime = new Time($wrapState.param.timestamp).subtract(7, 'd');
		$scope.alertList = CompatibleEntity.query("LIST", {
			query: "AlertService",
			condition: {
				alertId: $wrapState.param.alertId,
			},
			startTime: startTime,
			endTime: endTime,
		});
		$scope.alertList._then(function () {
			$scope.alert = $scope.alertList[0];

			if(!$scope.alert) {
				$.dialog({
					title: "OPS",
					content: "Alert '" + $wrapState.param.alertId + "' not found!"
				});
				return;
			}

			$scope.alertBody = $sce.trustAsHtml(($scope.alert.alertBody + "")
				.replace(/\\r/g, '\r')
				.replace(/\\n/g, '\n')
			);
		});
	});

	// ======================================================================================
	// =                                       Stream                                       =
	// ======================================================================================
	eagleControllers.controller('alertStreamListCtrl', function ($scope, $wrapState, PageConfig, Application, Entity) {
		PageConfig.title = "Alert Streams";

		$scope.streamList = [];
		$scope.site = $wrapState.param.siteId;

		Entity.queryMetadata("streams", { siteId: $scope.site })._then(function (res) {
			$scope.streamList = $.map(res.data, function (stream) {
				var application = Application.findProvider(stream.dataSource);
				return $.extend({application: application}, stream);
			});
		});

		$scope.dataSources = {};
		Entity.queryMetadata("datasources")._then(function(res) {
			$.each(res.data, function (i, dataSource) {
				$scope.dataSources[dataSource.name] = dataSource;
			});
		});

		$scope.showDataSource = function (stream) {
			var dataSource = $scope.dataSources[stream.dataSource];
			$.dialog({
				title: dataSource.name,
				content: $("<pre class='text-break'>").html(JSON.stringify(dataSource, null, "\t")),
				size: "large"
			});
		};
	});

	// ======================================================================================
	// =                                       Policy                                       =
	// ======================================================================================
	eagleControllers.controller('policyListCtrl', function ($scope, $wrapState, PageConfig, Entity, Policy) {
		PageConfig.title = "Alert Policies";
		$scope.loading = false;

		$scope.policyList = [];
		$scope.site = $wrapState.param.siteId;

		function updateList() {
			var list = Entity.queryMetadata("policies", { siteId: $scope.site });
			$scope.loading = true;

			list._then(function () {
				$scope.loading = false;
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

	eagleControllers.controller('policyDetailCtrl', function ($scope, $wrapState, $interval, UI, PageConfig, Time, Entity, CompatibleEntity, Policy) {
		PageConfig.title = "Policy";
		PageConfig.subTitle = "Detail";
		PageConfig.navPath = [
			{title: "Policy List", path: "/site/" + $wrapState.param.siteId + "/policies"},
			{title: "Detail"}
		];

		$scope.site = $wrapState.param.siteId;
		$scope.tab = "setting";

		$scope.setTab = function (tab) {
			$scope.tab = tab;
		};

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
				$scope.assignment = common.array.find(policyName, schedule.assignments || [], ["policyName"]) || {};

				var queueList = $.map(schedule.monitoredStreams, function (stream) {
					return stream.queues;
				});
				$scope.queue = common.array.find($scope.assignment.queueId, queueList, ["queueId"]);
			});
		}
		updatePolicy();

		var streams = {};
		Entity.queryMetadata("datasources")._then(function(res) {
			var dataSources = {};
			$.each(res.data, function (i, dataSource) {
				dataSources[dataSource.name] = dataSource;
			});

			Entity.queryMetadata("streams")._then(function (res) {
				$.each(res.data, function (i, stream) {
					streams[stream.streamId] = stream;
					stream.dataSource = dataSources[stream.dataSource];
				});
			});
		});

		$scope.showDataSource = function (stream) {
			var dataSource = streams[stream].dataSource;
			$.dialog({
				title: dataSource.name,
				content: $("<pre class='text-break'>").html(JSON.stringify(dataSource, null, "\t")),
				size: "large"
			});
		};

		$scope.alertList = CompatibleEntity.query("LIST", {
			query: "AlertService",
			condition: {policyId: $wrapState.param.name},
			startTime: new Time().subtract(7, 'day'),
			endTime: new Time()
		});

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

		$scope.makePrototype = function () {
			$.dialog({
				title: 'Confirm',
				content: 'Do you want to make this policy as Prototype?',
				confirm: true,
			}, function (ret) {
				if (!ret) return;

				Entity.post('policyProto/create/' + $scope.policy.name, '')._then(function (res) {
					var validate = res.data;
					console.log(validate);
					if(!validate.success) {
						$.dialog({
							title: "OPS",
							content: validate.message
						});
						return;
					}
				});
			});
		};

		var refreshInterval = $interval($scope.alertList._refresh, 1000 * 60);
		$scope.$on('$destroy', function() {
			$interval.cancel(refreshInterval);
		});
	});
}());
