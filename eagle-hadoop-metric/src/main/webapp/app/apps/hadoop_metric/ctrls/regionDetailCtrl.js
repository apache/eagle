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

			$scope.chatnameList = ["Memory Usage", "Direct Memory Usage", "GC count", "GC TimeMillis", "QueueSize", "NumCallsInGeneralQueue",
				"NumActiveHandler", "IPC Queue Time (99th)", "IPC Process Time (99th)", "QueueCallTime_num_ops", "ProcessCallTime_num_ops",
				"RegionCount", "StoreCount", "MemStoreSize", "StoreFileSize", "TotalRequestCount", "ReadRequestCount", "WriteRequestCount",
				"SlitQueueLength", "CompactionQueueLength", "FlushQueueLength", "BlockCacheSize", "BlockCacheHitCount", "BlockCacheCountHitPercent"];
			$scope.chatmetricList = [["nonheap", "heap"], ["directmemory"], ["GCCount"], ["GCTimeMillis"], ["QueueSize"], ["NumCallsInGeneralQueue"],
				["NumActiveHandler"], ["IPCQueueTime99th"], ["IPCProcessTime99th"], ["QueueCallTime_num_ops"], ["ProcessCallTime_num_ops"],
				["RegionCount"], ["StoreCount"], ["MemStoreSize"], ["StoreFileSize"], ["TotalRequestCount"], ["ReadRequestCount"], ["WriteRequestCount"],
				["SlitQueueLength"], ["CompactionQueueLength"], ["FlushQueueLength"], ["BlockCacheSize"], ["BlockCacheHitCount"], ["BlockCacheCountHitPercent"]];
			$scope.chatoptionList = [sizeoption, sizeoption, {}, gctimeoption, {}, {}, {}, {}, {}, {}, {}, {}, {}, sizeoption, sizeoption, {}, {}, {}, {}, {}, {}, sizeoption, {}, {}];

			$scope.metricList = [];
			$.each($scope.chatnameList, function (i) {
				var charname = $scope.chatnameList[i];
				$scope.metricList[charname] = {
					title: $scope.chatnameList[i],
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
					$.each($scope.chatmetricList, function (i) {
						var metricList = $scope.chatmetricList[i];
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricspromies = generateHbaseMetric(masterMetricList[metricKey], startTime, endTime, metricKey);
							var charname = $scope.chatnameList[i];
							$scope.metricList[charname].promises.push(metricspromies);
						});
					});

					$.each($scope.chatmetricList, function (k) {
						var charname = $scope.chatnameList[k];
						$q.all($scope.metricList[charname].promises).then(function (resp) {
							var series = [];
							for (var r = 0; r < resp.length; r += 1) {
								var rs = resp[r][1];
								if (rs.length > 0) {
									series.push(rs);
								}
							}
							$scope.metricList[charname] = mergeSeries(charname, series, $scope.chatmetricList[k], $scope.chatoptionList[k]);
						});
					});
				});

				METRIC.regionserverStatus($scope.hostname, $scope.site)._promise.then(function (res) {
					$scope.regionstatus = res;
				});
			};
			Time.onReload(function () {
				cache = {};
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

