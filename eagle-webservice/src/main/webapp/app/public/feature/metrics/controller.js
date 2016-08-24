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
	// =                          Function                          =
	// ==============================================================
	// Format dashboard unit. Will adjust format with old version and add miss attributes.
	feature.service("DashboardFormatter", function() {
		return {
			parse: function(unit) {
				unit = unit || {};
				unit.groups = unit.groups || [];

				$.each(unit.groups, function (i, group) {
					group.charts = group.charts || [];
					$.each(group.charts, function (i, chart) {
						if (!chart.metrics && chart.metric) {
							chart.metrics = [{
								aggregations: chart.aggregations,
								dataSource: chart.dataSource,
								metric: chart.metric
							}];

							delete chart.aggregations;
							delete chart.dataSource;
							delete chart.metric;
						} else if (!chart.metrics) {
							chart.metrics = [];
						}
					});
				});

				return unit;
			}
		};
	});

	// ==============================================================
	// =                         Controller                         =
	// ==============================================================

	// ========================= Dashboard ==========================
	feature.navItem("dashboard", "Metrics", "line-chart");

	feature.controller('dashboard', function(PageConfig, $scope, $http, $q, UI, Site, Authorization, Application, Entities, DashboardFormatter) {
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
			$scope.refreshAllChart(true);
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
			var metric = {
				dataSource: $scope._newMetricDataSrc.dataSource,
				metric: $scope._newMetricDataMetric,
				aggregations: ["max"]
			};
			$("#metricMDL").modal('hide');

			if($scope.metricForConfigChart) {
				$scope.configPreviewChart.metrics.push(metric);
				$scope.refreshChart($scope.configPreviewChart, true, true);
			} else {
				group.charts.push({
					chart: "line",
					metrics: [metric]
				});
				$scope.refreshAllChart();
			}
		};

		// ======================== Menu ========================
		function _checkGroupName(entity) {
			if(common.array.find(entity.name, $scope.dashboard.groups, "name")) {
				return "Group name conflict";
			}
		}

		$scope.newGroup = function() {
			if($scope.lock) return;

			UI.createConfirm("Group", {}, [{field: "name"}], _checkGroupName).then(null, null, function(holder) {
				$scope.dashboard.groups.push({
					name: holder.entity.name,
					charts: []
				});
				holder.closeFunc();

				setTimeout(function() {
					$scope.tabHolder.setSelect(holder.entity.name);
				}, 0);
			});
		};

		function renameGroup() {
			var group = $scope.tabHolder.selectedPane.data;
			UI.updateConfirm("Group", {}, [{field: "name", name: "New Name"}], _checkGroupName).then(null, null, function(holder) {
				group.name = holder.entity.name;
				holder.closeFunc();
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
				{icon: "pencil", title: "Rename Group", func: renameGroup},
				{icon: "trash", title: "Delete Group", danger: true, func: deleteGroup}
			]},
			{icon: "plus", title: "New Group", func: $scope.newGroup}
		] : [];

		// ===================== Dashboard ======================
		$scope.dashboardList = Entities.queryEntities("GenericResourceService", {
			site: Site.current().tags.site,
			application: Application.current().tags.application
		});
		$scope.dashboardList._promise.then(function(list) {
			$scope.dashboardEntity = list[0];
			$scope.dashboard = DashboardFormatter.parse(common.parseJSON($scope.dashboardEntity.value));
			$scope.refreshAllChart();
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
		$scope.metricForConfigChart = false;
		$scope.viewChart = null;

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
			{name: "Max", series: "max"},
			{name: "Avg", series: "avg"},
			{name: "Count", series: "count"},
			{name: "Sum", series: "sum"}
		];

		$scope.newChart = function() {
			$scope.metricForConfigChart = false;
			$("#metricMDL").modal();
		};

		$scope.configPreviewChartMinimumCheck = function() {
			$scope.configPreviewChart.min = $scope.configPreviewChart.min === 0 ? undefined : 0;
		};

		$scope.seriesChecked = function(metric, series) {
			if(!metric) return false;
			return $.inArray(series, metric.aggregations || []) !== -1;
		};
		$scope.seriesCheckClick = function(metric, series, chart) {
			if(!metric || !chart) return;
			if($scope.seriesChecked(metric, series)) {
				common.array.remove(series, metric.aggregations);
			} else {
				metric.aggregations.push(series);
			}
			$scope.chartSeriesUpdate(chart);
		};

		$scope.chartSeriesUpdate = function(chart) {
			chart._data = $.map(chart._oriData, function(groupData, i) {
				var metric = chart.metrics[i];
				return $.map(groupData, function(series) {
					if($.inArray(series._key, metric.aggregations) !== -1) return series;
				});
			});
		};

		$scope.configAddMetric = function() {
			$scope.metricForConfigChart = true;
			$("#metricMDL").modal();
		};

		$scope.configRemoveMetric = function(metric) {
			common.array.remove(metric, $scope.configPreviewChart.metrics);
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
			$scope.configPreviewChart.metrics = $.map(chart.metrics, function(metric) {
				return $.extend({}, metric, {aggregations: (metric.aggregations || []).slice()});
			});
			delete $scope.configPreviewChart._config;
			$("#chartMDL").modal();
			setTimeout(function() {
				$(window).resize();
			}, 200);
		};

		$scope.confirmUpdateChart = function() {
			$("#chartMDL").modal('hide');
			$.extend($scope.configTargetChart, $scope.configPreviewChart);
			$scope.chartSeriesUpdate($scope.configTargetChart);
			if($scope.configTargetChart._holder) $scope.configTargetChart._holder.refreshAll();
			$scope.configPreviewChart = null;
		};

		$scope.deleteChart = function(group, chart) {
			UI.deleteConfirm(chart.metric).then(null, null, function(holder) {
				common.array.remove(chart, group.charts);
				holder.closeFunc();
				$scope.refreshAllChart(false, true);
			});
		};

		$scope.showChart = function(chart) {
			$scope.viewChart = chart;
			$("#chartViewMDL").modal();
			setTimeout(function() {
				$(window).resize();
			}, 200);
		};

		$scope.refreshChart = function(chart, forceRefresh, refreshAll) {
			var _intervals = $scope.startTime.toISOString() + "/" + $scope.endTime.toISOString();

			function _refreshChart() {
				if (chart._holder) {
					if (refreshAll) {
						chart._holder.refreshAll();
					} else {
						chart._holder.refresh();
					}
				}
			}

			var _tmpData, _metricPromiseList;

			if (chart._data && !forceRefresh) {
				// Refresh chart without reload
				_refreshChart();
			} else {
				// Refresh chart with reload
				_tmpData = [];
				_metricPromiseList = $.map(chart.metrics, function (metric, k) {
					// Each Metric
					var _query = JSON.stringify({
						"queryType": "groupBy",
						"dataSource": metric.dataSource,
						"granularity": $scope.autoRefreshSelect.timeDes,
						"dimensions": ["metric"],
						"filter": {"type": "selector", "dimension": "metric", "value": metric.metric},
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
							},
							{
								"type": "count",
								"name": "count",
								"fieldName": "maxValue"
							},
							{
								"type": "longSum",
								"name": "sum",
								"fieldName": "maxValue"
							}
						],
						"postAggregations" : [
							{
								"type": "javascript",
								"name": "avg",
								"fieldNames": ["sum", "count"],
								"function": "function(sum, cnt) { return sum / cnt;}"
							}
						],
						"intervals": [_intervals]
					});

					return $http.post(_druidConfig.broker + "/druid/v2", _query, {withCredentials: false}).then(function (response) {
						var _data = nvd3.convert.druid([response.data]);
						_tmpData[k] = _data;

						// Process series name
						$.each(_data, function(i, series) {
							series._key = series.key;
							if(chart.metrics.length > 1) {
								series.key = metric.metric.replace(/^.*\./, "") + "-" +series._key;
							}
						});
					});
				});

				$q.all(_metricPromiseList).then(function() {
					chart._oriData = _tmpData;
					$scope.chartSeriesUpdate(chart);
					_refreshChart();
				});
			}
		};

		$scope.refreshAllChart = function(forceRefresh, refreshAll) {
			setTimeout(function() {
				$scope.endTime = app.time.now();
				$scope.startTime = $scope.autoRefreshSelect.getStartTime($scope.endTime);

				$scope.refreshTimeDisplay();

				$.each($scope.dashboard.groups, function (i, group) {
					$.each(group.charts, function (j, chart) {
						$scope.refreshChart(chart, forceRefresh, refreshAll);
					});
				});

				$(window).resize();
			}, 0);
		};

		$scope.chartSwitchRefresh = function(source, target) {
			var _oriSize = source.size;
			source.size = target.size;
			target.size = _oriSize;

			if(source._holder) source._holder.refreshAll();
			if(target._holder) target._holder.refreshAll();

		};

		_refreshInterval = setInterval(function() {
			if(!$scope.dashboardReady) return;
			$scope.refreshAllChart(true);
		}, 1000 * 30);

		// > Chart UI
		$scope.configChartSize = function(chart, sizeOffset) {
			chart.size = (chart.size || 6) + sizeOffset;
			if(chart.size <= 0) chart.size = 1;
			if(chart.size > 12) chart.size = 12;
			setTimeout(function() {
				$(window).resize();
			}, 1);
		};

		// ========================= UI =========================
		$("#metricMDL").on('hidden.bs.modal', function () {
			if($(".modal-backdrop").length) {
				$("body").addClass("modal-open");
			}
		});

		$("#chartViewMDL").on('hidden.bs.modal', function () {
			$scope.viewChart = null;
		});

		// ====================== Clean Up ======================
		$scope.$on('$destroy', function() {
			clearInterval(_refreshInterval);
		});
	});
})();
