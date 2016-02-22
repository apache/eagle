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

		$scope._newMetricFilter = "";
		$scope._newMetricDataSrc = null;
		$scope._newMetricDataMetric = null;

		$scope.tabHolder = {};

		// ======================= Metric =======================
		$http.get(_druidConfig.coordinator + "/druid/coordinator/v1/metadata/datasources", {withCredentials: false}).then(function(data) {
			// TODO: Hard code
			var _endTime = new moment("2016-02-20 00:00:00").subtract(6, "hour");
			var _startTime = _endTime.clone().subtract(3, "hour");
			var _intervals = _startTime.toISOString() + "/" + _endTime.toISOString();

			$scope.dataSourceList = $.map(data.data, function(dataSrc) {
				return {
					dataSource: dataSrc,
					metricList: []
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
					dataSrc.metricList = $.map(response.data, function(entity) {
						return entity.event.metric;
					});
				});
			});

			$q.all(_metrixList_promiseList).finally(function() {
				$scope.dataSourceListReady = true;

				$scope._newMetricDataSrc = $scope.dataSourceList[0];
				$scope._newMetricDataMetric = common.getValueByPath($scope._newMetricDataSrc, "metricList.0");
			});
		}, function() {
			$.dialog({
				title: "OPS",
				content: "Fetch data source failed. Please check Site Application Metrics configuration."
			});
		});

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
							}
						]
					}
				]
			};
		}, 100);

		// ======================= Groups =======================
		$scope.deleteGroup = function() {
			var group = $scope.tabHolder.selectedPane.data;
			UI.deleteConfirm(group.name).then(null, null, function(holder) {
				common.array.remove(group, $scope.dashboard.groups);
				holder.closeFunc();
			});
		};

		// ======================= Chart ========================
		$scope.newChart = function() {
			$("#metricMDL").modal();
		};

		$scope.dataSourceMetricList = function(dataSrc, filter) {
			filter = (filter || "").toLowerCase().trim().split(/\s+/);
			return $.grep((dataSrc && dataSrc.metricList) || [], function(metric) {
				for(var i = 0 ; i < filter.length ; i += 1) {
					if(metric.toLowerCase().indexOf(filter[i]) === -1) return false;
				}
				return true;
			});
		};

		$scope.newMetricSelectDataSource = function(dataSrc) {
			if(dataSrc !== $scope._newMetricDataMetric) $scope._newMetricDataMetric = dataSrc.metricList[0];
			$scope._newMetricDataSrc = dataSrc;
		};
		$scope.newMetricSelectMetric = function(metric) {
			$scope._newMetricDataMetric = metric;
		};

		$scope.confirmSelectMetric = function() {
			var group = $scope.tabHolder.selectedPane.data;
			$("#metricMDL").modal('hide');

			group.charts.push({
				dataSource: $scope._newMetricDataSrc,
				metrics: $scope._newMetricDataMetric,
				aggregations: ["max"]
			});
		};
	});
})();