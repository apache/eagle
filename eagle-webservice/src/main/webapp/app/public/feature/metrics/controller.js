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
	feature.navItem("dashboard", "Metrics", "line-chart");

	feature.controller('dashboard', function(PageConfig, $scope, $http, $q, UI, Site, Authorization, Application, Entities) {
		var _siteApp = Site.currentSiteApplication();
		var _druidConfig = _siteApp.configObj.druid;
		var _refreshInterval;

		var _menu_newChart;

		$scope.lock = false;

		$scope.dataSourceListReady = false;
		$scope.dataSourceList = [];
		$scope.dashboard = {
			groups: []
		};
		$scope.dashboardEntity = null;
		$scope.dashboardReady = false;

		$scope._newMetricFilter = "";
		$scope._newMetricDataSrc = null;
		$scope._newMetricDataMetric = null;

		$scope.tabHolder = {};

		$scope.endTime = app.time.now();
		$scope.startTime = $scope.endTime.clone();

		// =================== Initialization ===================
		if(!_druidConfig || !_druidConfig.coordinator || !_druidConfig.broker) {
			$.dialog({
				title: "OPS",
				content: "Druid configuration can't be empty!"
			});
			return;
		}

		$scope.autoRefreshList = [
			{title: "Last 1 Month", timeDes: "day", getStartTime: function(endTime) {return endTime.clone().subtract(1, "month");}},
			{title: "Last 1 Day", timeDes: "thirty_minute", getStartTime: function(endTime) {return endTime.clone().subtract(1, "day");}},
			{title: "Last 6 Hour", timeDes: "fifteen_minute", getStartTime: function(endTime) {return endTime.clone().subtract(6, "hour");}},
			{title: "Last 2 Hour", timeDes: "fifteen_minute", getStartTime: function(endTime) {return endTime.clone().subtract(2, "hour");}},
			{title: "Last 1 Hour", timeDes: "minute", getStartTime: function(endTime) {return endTime.clone().subtract(1, "hour");}}
		];
		$scope.autoRefreshSelect = $scope.autoRefreshList[2];

		// ====================== Function ======================
		$scope.setAuthRefresh = function(item) {
			$scope.autoRefreshSelect = item;
			$scope.chartRefresh(true);
		};

		$scope.refreshTimeDisplay = function() {
			PageConfig.pageSubTitle = common.format.date($scope.startTime) + " ~ " + common.format.date($scope.endTime) + " [refresh interval: 30s]";
		};
		$scope.refreshTimeDisplay();

		// ======================= Metric =======================
		// Fetch metric data
		$http.get(_druidConfig.coordinator + "/druid/coordinator/v1/metadata/datasources", {withCredentials: false}).then(function(data) {
			var _endTime = new moment();
			var _startTime = _endTime.clone().subtract(1, "day");
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

		// Filter data source
		$scope.dataSourceMetricList = function(dataSrc, filter) {
			filter = (filter || "").toLowerCase().trim().split(/\s+/);
			return $.grep((dataSrc && dataSrc.metricList) || [], function(metric) {
				for(var i = 0 ; i < filter.length ; i += 1) {
					if(metric.toLowerCase().indexOf(filter[i]) === -1) return false;
				}
				return true;
			});
		};

		// New metric select
		$scope.newMetricSelectDataSource = function(dataSrc) {
			if(dataSrc !== $scope._newMetricDataMetric) $scope._newMetricDataMetric = dataSrc.metricList[0];
			$scope._newMetricDataSrc = dataSrc;
		};
		$scope.newMetricSelectMetric = function(metric) {
			$scope._newMetricDataMetric = metric;
		};

		// Confirm new metric
		$scope.confirmSelectMetric = function() {
			var group = $scope.tabHolder.selectedPane.data;
			$("#metricMDL").modal('hide');

			group.charts.push({
				chart: "line",
				dataSource: $scope._newMetricDataSrc.dataSource,
				metric: $scope._newMetricDataMetric,
				aggregations: ["max"]
			});

			$scope.chartRefresh();
		};

		// ======================== Menu ========================
		function newGroup() {
			if($scope.lock) return;

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

				setTimeout(function() {
					$scope.tabHolder.setSelect(holder.entity.name);
				}, 0);
			});
		}

		function deleteGroup() {
			var group = $scope.tabHolder.selectedPane.data;
			UI.deleteConfirm(group.name).then(null, null, function(holder) {
				common.array.remove(group, $scope.dashboard.groups);
				holder.closeFunc();
			});
		}

		_menu_newChart = {title: "Add Metric", func: function() {$scope.newChart();}};
		Object.defineProperties(_menu_newChart, {
			icon: {
				get: function() {return $scope.dataSourceListReady ? 'plus' : 'refresh fa-spin';}
			},
			disabled: {
				get: function() {return !$scope.dataSourceListReady;}
			}
		});

		$scope.menu = Authorization.isRole('ROLE_ADMIN') ? [
			{icon: "cog", title: "Configuration", list: [
				_menu_newChart,
				{icon: "trash", title: "Delete Group", danger: true, func: deleteGroup}
			]},
			{icon: "plus", title: "New Group", func: newGroup}
		] : [];

		// ===================== Dashboard ======================
		$scope.dashboardList = Entities.queryEntities("GenericResourceService", {
			site: Site.current().tags.site,
			application: Application.current().tags.application
		});
		$scope.dashboardList._promise.then(function(list) {
			$scope.dashboardEntity = list[0];
			$scope.dashboard = $scope.dashboardEntity ? common.parseJSON($scope.dashboardEntity.value) : {groups: []};
			$scope.chartRefresh();
		}).finally(function() {
			$scope.dashboardReady = true;
		});

		$scope.saveDashboard = function() {
			$scope.lock = true;

			if(!$scope.dashboardEntity) {
				$scope.dashboardEntity = {
					tags: {
						site: Site.current().tags.site,
						application: Application.current().tags.application,
						name: "/metric_dashboard/dashboard/default"
					}
				};
			}
			$scope.dashboardEntity.value = common.stringify($scope.dashboard);

			Entities.updateEntity("GenericResourceService", $scope.dashboardEntity)._promise.then(function() {
				$.dialog({
					title: "Done",
					content: "Save success!"
				});
			}, function() {
				$.dialog({
					title: "POS",
					content: "Save failed. Please retry."
				});
			}).finally(function() {
				$scope.lock = false;
			});
		};

		// ======================= Chart ========================
		$scope.configTargetChart = null;
		$scope.configPreviewChart = null;

		$scope.chartConfig = {
			xType: "time"
		};

		$scope.chartTypeList = [
			{icon: "line-chart", chart: "line"},
			{icon: "area-chart", chart: "area"},
			{icon: "bar-chart", chart: "column"},
			{icon: "pie-chart", chart: "pie"}
		];

		$scope.chartSeriesList = [
			{name: "Min", series: "min"},
			{name: "Max", series: "max"}
		];

		$scope.newChart = function() {
			$("#metricMDL").modal();
		};

		$scope.configPreviewChartMinimumCheck = function() {
			$scope.configPreviewChart.min = $scope.configPreviewChart.min === 0 ? undefined : 0;
			window.ccc1 = $scope.getChartConfig($scope.configPreviewChart);
		};

		$scope.seriesChecked = function(chart, series) {
			if(!chart) return false;
			return $.inArray(series, chart.aggregations || []) !== -1;
		};
		$scope.seriesCheckClick = function(chart, series) {
			if(!chart) return;
			if($scope.seriesChecked(chart, series)) {
				common.array.remove(series, chart.aggregations);
			} else {
				chart.aggregations.push(series);
			}
			$scope.chartSeriesUpdate(chart);
		};

		$scope.chartSeriesUpdate = function(chart) {
			chart._data = $.map(chart._oriData, function(series) {
				if($.inArray(series.key, chart.aggregations) !== -1) return series;
			});
		};

		$scope.getChartConfig = function(chart) {
			if(!chart) return null;

			var _config = chart._config = chart._config || $.extend({}, $scope.chartConfig);
			_config.yMin = chart.min;

			return _config;
		};

		$scope.configChart = function(chart) {
			$scope.configTargetChart = chart;
			$scope.configPreviewChart = $.extend({}, chart);
			delete $scope.configPreviewChart._config;
			$("#chartMDL").modal();
			setTimeout(function() {
				$(window).resize();
			}, 200);
		};

		$scope.confirmUpdateChart = function() {
			$("#chartMDL").modal('hide');
			common.extend($scope.configTargetChart, $scope.configPreviewChart);
			$scope.chartSeriesUpdate($scope.configTargetChart);
			if($scope.configTargetChart._holder) $scope.configTargetChart._holder.refreshAll();
		};

		$scope.deleteChart = function(group, chart) {
			UI.deleteConfirm(chart.metric).then(null, null, function(holder) {
				common.array.remove(chart, group.charts);
				holder.closeFunc();
				$scope.chartRefresh();
			});
		};

		$scope.chartRefresh = function(forceRefresh) {
			setTimeout(function() {
				$scope.endTime = app.time.now();
				$scope.startTime = $scope.autoRefreshSelect.getStartTime($scope.endTime);
				var _intervals = $scope.startTime.toISOString() + "/" + $scope.endTime.toISOString();

				$scope.refreshTimeDisplay();

				$.each($scope.dashboard.groups, function (i, group) {
					$.each(group.charts, function (j, chart) {
						var _data = JSON.stringify({
							"queryType": "groupBy",
							"dataSource": chart.dataSource,
							"granularity": $scope.autoRefreshSelect.timeDes,
							"dimensions": ["metric"],
							"filter": {"type": "selector", "dimension": "metric", "value": chart.metric},
							"aggregations": [
								{
									"type": "max",
									"name": "max",
									"fieldName": "maxValue"
								},
								{
									"type": "min",
									"name": "min",
									"fieldName": "maxValue"
								}
							],
							"intervals": [_intervals]
						});

						if (!chart._data || forceRefresh) {
							$http.post(_druidConfig.broker + "/druid/v2", _data, {withCredentials: false}).then(function (response) {
								chart._oriData = nvd3.convert.druid([response.data]);
								$scope.chartSeriesUpdate(chart);
								if(chart._holder) chart._holder.refresh();
							});
						} else {
							if(chart._holder) chart._holder.refresh();
						}
					});
				});
			}, 0);
		};

		_refreshInterval = setInterval(function() {
			if(!$scope.dashboardReady) return;
			$scope.chartRefresh(true);
		}, 1000 * 30);

		// ====================== Clean Up ======================
		$scope.$on('$destroy', function() {
			clearInterval(_refreshInterval);
		});
	});
})();