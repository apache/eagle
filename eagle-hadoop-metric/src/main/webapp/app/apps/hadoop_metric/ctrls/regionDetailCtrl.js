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
			$scope.metricList = [];
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
							return common.number.sizeFormat(value, 0);
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

			$scope.refresh = function () {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var metricspromies = [];
				METRIC.getMetricObj().then(function (res) {
					console.log(res.regionserver);
					var masterMetricList = res.regionserver;
					for (var metricKey in masterMetricList) {
						metricspromies.push(generateHbaseMetric(masterMetricList[metricKey], startTime, endTime, metricKey));
					}
					$q.all(metricspromies).then(function (resp) {
						var metricObj = {};
						for(var i=0; i < resp.length; i+=1) {
							metricObj[resp[i][0]] = resp[i][1];
						}
						return metricObj;
					}).then(function (seriesObj) {
						$scope.metricList = [];
						$scope.metricList.push(mergeSeries("Memory Usage", [seriesObj["nonheap"], seriesObj["heap"]], ["nonheap", "heap"], sizeoption));
						$scope.metricList.push(mergeSeries("Direct Memory Usage", [seriesObj["directmemory"]], ["directmemory"], sizeoption));
						$scope.metricList.push(mergeSeries("GC count", [seriesObj["GC count"]], ["GC count"], {}));
						$scope.metricList.push(mergeSeries("GC TimeMillis", [seriesObj["GC TimeMillis"]], ["GC TimeMillis"], gctimeoption));
						$scope.metricList.push(mergeSeries("QueueSize", [seriesObj["QueueSize"]], ["QueueSize"], {}));
						$scope.metricList.push(mergeSeries("NumCallsInGeneralQueue", [seriesObj["NumCallsInGeneralQueue"]], ["NumCallsInGeneralQueue"], {}));
						$scope.metricList.push(mergeSeries("NumActiveHandler", [seriesObj["NumActiveHandler"]], ["NumActiveHandler"], {}));
						$scope.metricList.push(mergeSeries("IPC Queue Time (99th)", [seriesObj["IPC Queue Time (99th)"]], ["IPC Queue Time (99th)"], {}));
						$scope.metricList.push(mergeSeries("IPC Process Time (99th)", [seriesObj["IPC Process Time (99th)"]], ["IPC Process Time (99th)"], {}));
						$scope.metricList.push(mergeSeries("QueueCallTime_num_ops", [seriesObj["QueueCallTime_num_ops"]], ["QueueCallTime_num_ops"], {}));
						$scope.metricList.push(mergeSeries("ProcessCallTime_num_ops", [seriesObj["ProcessCallTime_num_ops"]], ["ProcessCallTime_num_ops"], {}));
						$scope.metricList.push(mergeSeries("RegionCount", [seriesObj["RegionCount"]], ["RegionCount"], {}));
						$scope.metricList.push(mergeSeries("StoreCount", [seriesObj["StoreCount"]], ["StoreCount"], {}));
						$scope.metricList.push(mergeSeries("MemStoreSize", [seriesObj["MemStoreSize"]], ["MemStoreSize"], sizeoption));
						$scope.metricList.push(mergeSeries("StoreFileSize", [seriesObj["StoreFileSize"]], ["StoreFileSize"], sizeoption));
						$scope.metricList.push(mergeSeries("TotalRequestCount", [seriesObj["TotalRequestCount"]], ["TotalRequestCount"], {}));
						$scope.metricList.push(mergeSeries("ReadRequestCount", [seriesObj["ReadRequestCount"]], ["ReadRequestCount"], {}));
						$scope.metricList.push(mergeSeries("WriteRequestCount", [seriesObj["WriteRequestCount"]], ["WriteRequestCount"], {}));
						$scope.metricList.push(mergeSeries("SlitQueueLength", [seriesObj["SlitQueueLength"]], ["SlitQueueLength"], {}));
						$scope.metricList.push(mergeSeries("CompactionQueueLength", [seriesObj["CompactionQueueLength"]], ["CompactionQueueLength"], {}));
						$scope.metricList.push(mergeSeries("FlushQueueLength", [seriesObj["FlushQueueLength"]], ["FlushQueueLength"], {}));
						$scope.metricList.push(mergeSeries("BlockCacheSize", [seriesObj["BlockCacheSize"]], ["BlockCacheSize"], sizeoption));
						$scope.metricList.push(mergeSeries("BlockCacheHitCount", [seriesObj["BlockCacheHitCount"]], ["BlockCacheHitCount"], {}));
						$scope.metricList.push(mergeSeries("BlockCacheCountHitPercent", [seriesObj["BlockCacheCountHitPercent"]], ["BlockCacheCountHitPercent"], {}));
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
						if(typeof metric !== 'undefined') {
							series.push(METRIC.metricsToSeries(linename[i], metric, option));
						}
					});
				});
				return {
					title: title,
					series: series,
					option: option || {}
				};
			}
		});
	});
})
();
