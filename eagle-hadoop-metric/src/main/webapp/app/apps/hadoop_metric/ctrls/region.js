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
		hadoopMetricApp.controller("regionCtrl", function ($q, $wrapState, $scope, PageConfig, METRIC) {
			PageConfig.title = 'RegionServer';
			$scope.site = $wrapState.param.siteId;
			var METRIC_NAME = [
				"hadoop.memory.nonheapmemoryusage.used",
				"hadoop.memory.heapmemoryusage.used",
				"hadoop.bufferpool.direct.memoryused",
				"hadoop.hbase.jvm.gccount",
				"hadoop.hbase.jvm.gctimemillis"
			];

			$scope.metricList = [];

			function generateHbaseMetric(name, limit) {
				limit = limit || 100;
				var hbaseMetric;

				$scope.site = $wrapState.param.siteId;
				var jobCond = {
					site: $scope.site,
					component: "regionserver",
					host: "yhd-jqhadoop184.int.yihaodian.com"
				};
				hbaseMetric = METRIC.hbaseMetrics(jobCond, name, limit);
				return hbaseMetric._promise;
			}

			function mergeSeries(title, metrics, linename, option) {
				var series = [];
				$.each(metrics, function (i, metric) {
					series.push(METRIC.metricsToSeries(linename[i], metric, option));
				});
				return {
					title: title,
					series: series,
					option: option || {}
				};
			}

			var promies = [];
			$.each(METRIC_NAME, function (i, metric_name) {
				promies.push(generateHbaseMetric(metric_name, 20));
			});

			$q.all(promies).then(function (res) {
				var metric = [res[0], res[1]];
				console.log(res[3]);

				var memoption = {
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
					grid: {
						top: 70
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
					grid: {
						top: 70
					},
					legend: {
						x: 'center', y: 'bottom'
					},
					yAxis: [{
						axisLabel: {
							formatter: function (value) {
								return value/1000 + ' S';
							}
						}
					}]
				};
				$scope.metricList.push(mergeSeries("Memory Usage", metric, ["nonheap", "heap"], memoption));
				$scope.metricList.push(mergeSeries("Direct Memory Usage", [res[2]], ["directmemory"], memoption));
				$scope.metricList.push(mergeSeries("GC count", [res[3]], ["GC count"], {}));
				$scope.metricList.push(mergeSeries("Gc TimeMillis", [res[4]], ["Gc TimeMillis"], gctimeoption));
			});
		});
	});
})
();
//# sourceURL=region.js
