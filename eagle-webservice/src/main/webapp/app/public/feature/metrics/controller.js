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

	var featureControllers = angular.module('featureControllers');
	var feature = featureControllers.register("metrics");

	// ==============================================================
	// =                       Initialization                       =
	// ==============================================================

	// ==============================================================
	// =                         Controller                         =
	// ==============================================================

	// ========================= Dashboard ==========================
	feature.navItem("dashboard", "Metrics Dashboard", "line-chart");

	feature.controller('dashboard', function(PageConfig, $scope, $http, $q, UI, Site, Application) {
		var _siteApp = Site.currentSiteApplication();
		var _druidConfig = _siteApp.configObj.druid;

		$scope.dataSourceListReady = false;
		$scope.dataSourceList = [];
		$scope.dashboard = {
			groups: []
		};

		// ======================= Metric =======================
		$http.get(_druidConfig.coordinator + "/druid/coordinator/v1/metadata/datasources", {withCredentials: false}).then(function(data) {
			var _endTime = new moment();
			var _startTime = _endTime.clone().subtract(1, "d");
			var _intervals = _startTime.toISOString() + "/" + _endTime.toISOString();

			$scope.dataSourceList = $.map(data.data, function(dataSrc) {
				return {
					dataSource: dataSrc,
					metrics: []
				};
			});

			// List dataSource metrics
			var _metrixList_promiseList = $.map($scope.dataSourceList, function(dataSrc) {
				var _data = JSON.stringify({
					"queryType": "groupBy",
					"dataSource": dataSrc.dataSource,
					"granularity": "all",
					"dimensions": ["metric"],
					"aggregations": [
						{
							"type":"count",
							"name":"count"
						}
					],
					"intervals": [_intervals]
				});

				return $http.post(_druidConfig.broker + "/druid/v2", _data, {withCredentials: false}).then(function(response) {
					dataSrc.metrics = $.map(response.data, function(entity) {
						return entity.event.metric;
					});
				});
			});

			$q.all(_metrixList_promiseList).finally(function() {
				$scope.dataSourceListReady = true;
			});
		}, function() {
			$.dialog({
				title: "OPS",
				content: "Fetch data source failed. Please check Site Application Metrics configuration."
			});
		});

		// ===================== Dashboard ======================
		// TODO: Customize data load
		setTimeout(function() {
			// TODO: Mock for user data
			$scope.dashboard = {
				groups: [
					{
						name: "JMX",
						charts: [
							{
								dataSource: "nn_jmx_metric_sandbox",
								metrics: "hadoop.namenode.dfs.missingblocks",
								aggregations: ["max"]
							},
						]
					}
				]
			};
		}, 100);

		// ======================== Menu ========================
		$scope.menu = [
			{icon: "plus", title: "New Group", func: function() {
				UI.createConfirm("Group", {}, [{field: "name"}], function(entity) {
					if(common.array.find(entity.name, $scope.dashboard.groups, "name")) {
						return "Group name conflict";
					}
				}).then(null, null, function(holder) {
					$scope.dashboard.groups.push({
						name: holder.entity.name,
						charts: []
					});
					holder.closeFunc();
				});
			}}
		];

		// ======================= Groups =======================
		$scope.deleteGroup = function(group) {
			UI.deleteConfirm(group.name).then(null, null, function(holder) {
				common.array.remove(group, $scope.dashboard.groups);
				holder.closeFunc();
			});
		};
	});
})();