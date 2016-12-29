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
		hadoopMetricApp.controller("overviewCtrl", function ($q, $wrapState, $scope, PageConfig, METRIC, Time) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			var activeMasterInfo = METRIC.hbaseActiveMaster($scope.site);

			var MASTER_METRIC_ARRAY = [
				"hadoop.memory.nonheapmemoryusage.used",
				"hadoop.memory.heapmemoryusage.used",
				"hadoop.hbase.master.server.averageload",
				"hadoop.hbase.master.assignmentmanger.ritcount",
				"hadoop.hbase.master.assignmentmanger.ritcountoverthreshold",
				"hadoop.hbase.master.assignmentmanger.assign_num_ops",
				"hadoop.hbase.master.assignmentmanger.assign_min",
				"hadoop.hbase.master.assignmentmanger.assign_max",
				"hadoop.hbase.master.assignmentmanger.assign_75th_percentile",
				"hadoop.hbase.master.assignmentmanger.assign_95th_percentile",
				"hadoop.hbase.master.assignmentmanger.assign_99th_percentile",
				"hadoop.hbase.master.assignmentmanger.bulkassign_num_ops",
				"hadoop.hbase.master.assignmentmanger.bulkassign_min",
				"hadoop.hbase.master.assignmentmanger.bulkassign_max",
				"hadoop.hbase.master.assignmentmanger.bulkassign_75th_percentile",
				"hadoop.hbase.master.assignmentmanger.bulkassign_95th_percentile",
				"hadoop.hbase.master.assignmentmanger.bulkassign_99th_percentile",
				"hadoop.hbase.master.balancer.balancercluster_num_ops",
				"hadoop.hbase.master.balancer.balancercluster_min",
				"hadoop.hbase.master.balancer.balancercluster_max",
				"hadoop.hbase.master.balancer.balancercluster_75th_percentile",
				"hadoop.hbase.master.balancer.balancercluster_95th_percentile",
				"hadoop.hbase.master.balancer.balancercluster_99th_percentile",
				"hadoop.hbase.master.filesystem.hlogsplittime_min",
				"hadoop.hbase.master.filesystem.hlogsplittime_max",
				"hadoop.hbase.master.filesystem.hlogsplittime_75th_percentile",
				"hadoop.hbase.master.filesystem.hlogsplittime_95th_percentile",
				"hadoop.hbase.master.filesystem.hlogsplittime_99th_percentile",
				"hadoop.hbase.master.filesystem.hlogsplitsize_min",
				"hadoop.hbase.master.filesystem.hlogsplitsize_max",
				"hadoop.hbase.master.filesystem.metahlogsplittime_min",
				"hadoop.hbase.master.filesystem.metahlogsplittime_max",
				"hadoop.hbase.master.filesystem.metahlogsplittime_75th_percentile",
				"hadoop.hbase.master.filesystem.metahlogsplittime_95th_percentile",
				"hadoop.hbase.master.filesystem.metahlogsplittime_99th_percentile",
				"hadoop.hbase.master.filesystem.metahlogsplitsize_min",
				"hadoop.hbase.master.filesystem.metahlogsplitsize_max"
			];

			PageConfig.title = 'Overview';
			var storageOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, true, 0);
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.sizeFormat(value, 0);
						}
					}
				}]
			};
			$scope.metricList = {};

			function generateHbaseMetric(name) {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				$scope.site = $wrapState.param.siteId;

				var metrics = cache[name] = cache[name] || $q.all([activeMasterInfo._promise]).then(function (res) {
					var hostname = cache[hostname] = cache[hostname] || res[0][0].tags.hostname;
					$scope.defaultHostname = $wrapState.param.hostname || hostname;

					var jobCond = {
						site: $scope.site,
						component: "hbasemaster",
						host: $scope.defaultHostname
					};
					return METRIC.aggMetricsToEntities(METRIC.hbaseMetricsAggregation(jobCond, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime))._promise;
				});
				return metrics;
			}

			function mergeMetricToOneSeries(metricTitle, metrics, legendName, dataOption, option) {
				var series = [];
				$.each(metrics, function (i, metricMap) {
					$.map(metricMap, function (metric) {
						series.push(METRIC.metricsToSeries(legendName[i], metric, option));
					});
				});
				return {
					title: metricTitle,
					series: series,
					option: dataOption || {}
				};
			}
			// TODO: Optimize the chart count
			// TODO: ECharts dynamic refresh series bug: https://github.com/ecomfe/echarts/issues/4033
			$scope.refresh = function () {
				var hbaseservers = METRIC.hbasehostStatus({site: $scope.site});
				var promies = [];
				$.each(MASTER_METRIC_ARRAY, function (i, metric_name) {
					promies.push(generateHbaseMetric(metric_name));
				});
				promies.push(hbaseservers);
				$q.all(promies).then(function (res) {
					$scope.metricList = [
						mergeMetricToOneSeries("MemoryUsage", [res[0], res[1]], ["nonheap", "heap"], storageOption),
						mergeMetricToOneSeries("Master Averageload", [res[2]], ["averageload"]),
						mergeMetricToOneSeries("Ritcount", [res[3], res[4]], ["ritcount", "ritcountoverthreshold"]),
						mergeMetricToOneSeries("Assign", [res[5], res[6], res[7]], ["numOps", "Min", "Max"]),
						mergeMetricToOneSeries("Assign Percentile", [res[8], res[9], res[10]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BulkAssign", [res[11], res[12], res[13]], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BulkAssign Percentile", [res[14], res[15], res[16]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BalancerCluster", [res[17], res[18], res[19]], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BalancerCluster Percentile", [res[20], res[21], res[22]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitTime", [res[23], res[24]], ["HlogSplitTime_min", "HlogSplitTime_max"]),
						mergeMetricToOneSeries("HlogSplitTime Percentile", [res[25], res[26], res[27]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitSize", [res[28], res[29]], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime", [res[30], res[31]], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime Percentile", [res[32], res[33], res[34]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("MetaHlogSplitSize", [res[35], res[36]], ["Min", "Max"])
					];
					var regionhealtynum = 0;
					var regiontotal = 0;
					var hmasteractive;
					var hmasterstandby;
					$.each(res[37], function (i, server) {
						var role = server.tags.role;
						var status = server.status;
						if (role === "regionserver") {
							regiontotal++;
							if (status === "live") {
								regionhealtynum++;
							}
						}
						else if (role === "hmaster") {
							if (status === "active") {
								hmasteractive = server;
							} else {
								hmasterstandby = server;
							}

						}
					});
					$scope.regionhealtynum = regionhealtynum;
					$scope.regiontotal = regiontotal;
					$scope.hmasteractive = hmasteractive;
					$scope.hmasterstandby = hmasterstandby;
				});
			};


			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
