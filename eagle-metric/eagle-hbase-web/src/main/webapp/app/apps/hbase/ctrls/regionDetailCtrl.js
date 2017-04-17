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

(function () {
	/**
	 * `register` without params will load the module which using require
	 */
	register(function (hadoopMetricApp) {
		hadoopMetricApp.controller("regionDetailCtrl", function ($q, $wrapState, $scope, PageConfig, Time, METRIC) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			$scope.hostname = $wrapState.param.hostname;
			PageConfig.title = 'RegionServer ' + "(" + $scope.hostname + ")";
			Time.autoRefresh = false;

			var sizeoption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, true);
							}).reverse().join("<br/>");
					}
				},
				legend: {
					x: 'center', y: 'bottom'
				},
				areaStyle: {normal: {}},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value, true);
						}
					}
				}]
			};
			var digitalOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, false, 0);
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value, false);
						}
					}
				}]
			};

			var gctimeoption = {
				legend: {
					x: 'center', y: 'bottom'
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return value / 1000 + ' S';
						}
					}
				}]
			};
			$scope.chartList = [
				{
					name: "Memory Usage",
					metrics: ["nonheap", "heap"],
					option: sizeoption
				},
				{
					name: "Direct Memory Usage",
					metrics: ["directmemory"],
					option: sizeoption
				},
				{
					name: "GC count",
					metrics: ["GCCount"],
					option: {}
				},
				{
					name: "GC TimeMillis",
					metrics: ["GCTimeMillis"],
					option: gctimeoption
				},
				{
					name: "QueueSize",
					metrics: ["QueueSize"],
					option: {}
				},
				{
					name: "NumCallsInGeneralQueue",
					metrics: ["NumCallsInGeneralQueue"],
					option: {}
				},
				{
					name: "NumActiveHandler",
					metrics: ["NumActiveHandler"],
					option: {}
				},
				{
					name: "IPC Queue Time (99th)",
					metrics: ["IPCQueueTime99th"],
					option: {}
				},
				{
					name: "IPC Process Time (99th)",
					metrics: ["IPCProcessTime99th"],
					option: {}
				},
				{
					name: "QueueCallTime_num_ops",
					metrics: ["QueueCallTime_num_ops"],
					option: digitalOption
				},
				{
					name: "ProcessCallTime_num_ops",
					metrics: ["ProcessCallTime_num_ops"],
					option: digitalOption
				},
				{
					name: "RegionCount",
					metrics: ["RegionCount"],
					option: {}
				},
				{
					name: "StoreCount",
					metrics: ["StoreCount"],
					option: {}
				},
				{
					name: "MemStoreSize",
					metrics: ["MemStoreSize"],
					option: sizeoption
				},
				{
					name: "StoreFileSize",
					metrics: ["StoreFileSize"],
					option: sizeoption
				},
				{
					name: "SlitQueueLength",
					metrics: ["SlitQueueLength"],
					option: {}
				},
				{
					name: "CompactionQueueLength",
					metrics: ["CompactionQueueLength"],
					option: {}
				},
				{
					name: "FlushQueueLength",
					metrics: ["FlushQueueLength"],
					option: {}
				},
				{
					name: "BlockCacheSize",
					metrics: ["BlockCacheSize"],
					option: sizeoption
				},
				{
					name: "BlockCacheHitCount",
					metrics: ["BlockCacheHitCount"],
					option: {}
				},
				{
					name: "BlockCacheCountHitPercent",
					metrics: ["BlockCacheCountHitPercent"],
					option: digitalOption
				},
				{
					name: "TotalRequestCount",
					metrics: ["TotalRequestCount"],
					option: digitalOption
				},
				{
					name: "ReadRequestCount",
					metrics: ["ReadRequestCount"],
					option: digitalOption
				},
				{
					name: "WriteRequestCount",
					metrics: ["WriteRequestCount"],
					option: digitalOption
				}
			];

			$scope.metricList = [];
			$.each($scope.chartList, function (i) {
				var chart = $scope.chartList[i];
				var chartname = chart.name;
				$scope.metricList[chartname] = {
					title: chartname,
					series: {},
					option: {},
					loading: true,
					promises: []
				};
			});
			$scope.refresh = function () {
				var startTime = Time.startTime();
				var endTime = Time.endTime();

				METRIC.getMetricObj().then(function (res) {
					var masterMetricList = res.regionserver;
					$.each($scope.chartList, function (i) {
						var chart = $scope.chartList[i];
						var metricList = chart.metrics;
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricspromies = generateHbaseMetric(masterMetricList[metricKey], startTime, endTime, metricKey);
							var chartname = chart.name;
							$scope.metricList[chartname].promises.push(metricspromies);
						});
					});

					$.each($scope.chartList, function (k) {
						var chart = $scope.chartList[k];
						var chartname = chart.name;
						$q.all($scope.metricList[chartname].promises).then(function (resp) {
							var series = [];
							for (var r = 0; r < resp.length; r += 1) {
								var rs = resp[r][1];
								if (rs.length > 0) {
									series.push(rs);
								}
							}
							$scope.metricList[chartname] = mergeSeries(chartname, series, chart.metrics, chart.option);
						});
					});
				});

				METRIC.regionserverStatus($scope.hostname, $scope.site)._promise.then(function (res) {
					$scope.regionstatus = res;
				});
			};
			Time.onReload(function () {
				cache = {};
				$.each($scope.chartList, function (i) {
					var chart = $scope.chartList[i];
					var chartname = chart.name;
					$scope.metricList[chartname] = {
						title: chartname,
						series: {},
						option: {},
						loading: true,
						promises: []
					};
				});
				$scope.refresh();
			}, $scope);
			$scope.refresh();


			function generateHbaseMetric(name, startTime, endTime, flag) {
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				var condition = {
					site: $scope.site,
					component: "regionserver",
					host: $scope.hostname
				};
				return METRIC.aggMetricsToEntities(METRIC.hbaseMetricsAggregation(condition, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
					._promise.then(function (list) {
						var metricFlag = $.map(list, function (metrics) {
							return metrics[0].flag;
						});
						return [metricFlag, list];
					});
			}

			function mergeSeries(title, metrics, linename, option) {
				var series = [];
				$.each(metrics, function (i, metricMap) {
					$.map(metricMap, function (metric) {
						if (typeof metric !== 'undefined') {
							series.push(METRIC.metricsToSeries(linename[i], metric, option));
						}
					});
				});
				return {
					title: title,
					series: series,
					option: option || {},
					loading: false
				};
			}
		});
	});
})
();
