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

			var seriesObj = {};
			var metricObj = {};
			metricObj["nonheap"] = "hadoop.memory.nonheapmemoryusage.used";
			metricObj["heap"] = "hadoop.memory.heapmemoryusage.used";
			metricObj["averageload"] = "hadoop.hbase.master.server.averageload";
			metricObj["ritcount"] = "hadoop.hbase.master.assignmentmanger.ritcount";
			metricObj["ritcountoverthreshold"] = "hadoop.hbase.master.assignmentmanger.ritcountoverthreshold";
			metricObj["AssignNumOps"] = "hadoop.hbase.master.assignmentmanger.assign_num_ops";
			metricObj["AssignMin"] = "hadoop.hbase.master.assignmentmanger.assign_min";
			metricObj["AssignMax"] = "hadoop.hbase.master.assignmentmanger.assign_max";
			metricObj["AssignPercentile75th"] = "hadoop.hbase.master.assignmentmanger.assign_75th_percentile";
			metricObj["AssignPercentile95th"] = "hadoop.hbase.master.assignmentmanger.assign_95th_percentile";
			metricObj["AssignPercentile99th"] = "hadoop.hbase.master.assignmentmanger.assign_99th_percentile";
			metricObj["BulkAssignNum_ops"] = "hadoop.hbase.master.assignmentmanger.bulkassign_num_ops";
			metricObj["BulkAssignMin"] = "hadoop.hbase.master.assignmentmanger.bulkassign_min";
			metricObj["BulkAssignMax"] = "hadoop.hbase.master.assignmentmanger.bulkassign_max";
			metricObj["BulkAssignPercentile75th"] = "hadoop.hbase.master.assignmentmanger.bulkassign_75th_percentile";
			metricObj["BulkAssignPercentile95th"] = "hadoop.hbase.master.assignmentmanger.bulkassign_95th_percentile";
			metricObj["BulkAssignPercentile99th"] = "hadoop.hbase.master.assignmentmanger.bulkassign_99th_percentile";
			metricObj["BalancerClusterNum_ops"] = "hadoop.hbase.master.balancer.balancercluster_num_ops";
			metricObj["BalancerClusterMin"] = "hadoop.hbase.master.balancer.balancercluster_min";
			metricObj["BalancerClusterMax"] = "hadoop.hbase.master.balancer.balancercluster_max";
			metricObj["BalancerClusterPercentile75th"] = "hadoop.hbase.master.balancer.balancercluster_75th_percentile";
			metricObj["BalancerClusterPercentile95th"] = "hadoop.hbase.master.balancer.balancercluster_95th_percentile";
			metricObj["BalancerClusterPercentile99th"] = "hadoop.hbase.master.balancer.balancercluster_99th_percentile";
			metricObj["HlogSplitTimeMin"] = "hadoop.hbase.master.filesystem.hlogsplittime_min";
			metricObj["HlogSplitTimeMax"] = "hadoop.hbase.master.filesystem.hlogsplittime_max";
			metricObj["HlogSplitTimePercentile75th"] = "hadoop.hbase.master.filesystem.hlogsplittime_75th_percentile";
			metricObj["HlogSplitTimePercentile95th"] = "hadoop.hbase.master.filesystem.hlogsplittime_95th_percentile";
			metricObj["HlogSplitTimePercentile99th"] = "hadoop.hbase.master.filesystem.hlogsplittime_99th_percentile";
			metricObj["HlogSplitSizeMin"] = "hadoop.hbase.master.filesystem.hlogsplitsize_min";
			metricObj["HlogSplitSizeMax"] = "hadoop.hbase.master.filesystem.hlogsplitsize_max";
			metricObj["MetaHlogSplitTimeMin"] = "hadoop.hbase.master.filesystem.metahlogsplittime_min";
			metricObj["MetaHlogSplitTimeMax"] = "hadoop.hbase.master.filesystem.metahlogsplittime_max";
			metricObj["MetaHlogSplitTimePercentile75th"] = "hadoop.hbase.master.filesystem.metahlogsplittime_75th_percentile";
			metricObj["MetaHlogSplitTimePercentile95th"] = "hadoop.hbase.master.filesystem.metahlogsplittime_95th_percentile";
			metricObj["MetaHlogSplitTimePercentile99th"] = "hadoop.hbase.master.filesystem.metahlogsplittime_99th_percentile";
			metricObj["MetaHlogSplitSizeMin"] = "hadoop.hbase.master.filesystem.metahlogsplitsize_min";
			metricObj["MetaHlogSplitSizeMax"] = "hadoop.hbase.master.filesystem.metahlogsplitsize_max";

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

			function generateHbaseMetric(name, flag) {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				$scope.site = $wrapState.param.siteId;

				return cache[name] = cache[name] || activeMasterInfo._promise.then(function (res) {
						var hostname = cache[hostname] = cache[hostname] || res[0].tags.hostname;
						$scope.defaultHostname = $wrapState.param.hostname || hostname;

						var jobCond = {
							site: $scope.site,
							component: "hbasemaster",
							host: $scope.defaultHostname
						};
						return METRIC.aggMetricsToEntities(METRIC.hbaseMetricsAggregation(jobCond, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
							._promise.then(function (list) {
								var metricFlag = $.map(list, function (metrics) {
									return metrics[0].flag;
								});
								return [metricFlag, list];
							});
					});
			}

			function mergeMetricToOneSeries(metricTitle, metrics, legendName, dataOption, option) {
				var series = [];

				$.each(metrics, function (i, metricMap) {
					series.push(METRIC.metricsToSeries(legendName[i], metricMap[0], option));
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
				var metricspromies = [];

				for (var metricKey in metricObj) {
					metricspromies.push(generateHbaseMetric(metricObj[metricKey], metricKey));
				}
				$q.all(metricspromies).then(function (res) {
					$.each(res, function (i, metrics) {
						seriesObj[metrics[0]] = metrics[1];
					});
					$scope.metricList = [
						mergeMetricToOneSeries("MemoryUsage", [seriesObj["nonheap"], seriesObj["heap"]], ["nonheap", "heap"], storageOption, {areaStyle: {normal: {}}}),
						mergeMetricToOneSeries("Master Averageload", [seriesObj["averageload"]], ["averageload"]),
						mergeMetricToOneSeries("Ritcount", [seriesObj["ritcount"], seriesObj["ritcountoverthreshold"]], ["ritcount", "ritcountoverthreshold"]),
						mergeMetricToOneSeries("Assign", [seriesObj["AssignNumOps"], seriesObj["AssignMin"], seriesObj["AssignMax"]], ["numOps", "Min", "Max"]),
						mergeMetricToOneSeries("Assign Percentile", [seriesObj["AssignPercentile75th"], seriesObj["AssignPercentile95th"], seriesObj["AssignPercentile99th"]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BulkAssign", [seriesObj["BulkAssignNum_ops"], seriesObj["BulkAssignMin"], seriesObj["BulkAssignMax"]], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BulkAssign Percentile", [seriesObj["BulkAssignPercentile75th"], seriesObj["BulkAssignPercentile95th"], seriesObj["BulkAssignPercentile99th"]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BalancerCluster", [seriesObj["BalancerClusterNum_ops"], seriesObj["BalancerClusterMin"], seriesObj["BalancerClusterMax"]], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BalancerCluster Percentile", [seriesObj["BalancerClusterPercentile75th"], seriesObj["BalancerClusterPercentile95th"], seriesObj["BalancerClusterPercentile99th"]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitTime", [seriesObj["HlogSplitTimeMin"], seriesObj["HlogSplitTimeMax"]], ["HlogSplitTime_min", "HlogSplitTime_max"]),
						mergeMetricToOneSeries("HlogSplitTime Percentile", [seriesObj["HlogSplitTimePercentile75th"], seriesObj["HlogSplitTimePercentile95th"], seriesObj["HlogSplitTimePercentile99th"]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitSize", [seriesObj["HlogSplitSizeMin"], seriesObj["HlogSplitSizeMax"]], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime", [seriesObj["MetaHlogSplitTimeMin"], seriesObj["MetaHlogSplitTimeMax"]], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime Percentile", [seriesObj["MetaHlogSplitTimePercentile75th"], seriesObj["MetaHlogSplitTimePercentile95th"], seriesObj["MetaHlogSplitTimePercentile99th"]], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("MetaHlogSplitSize", [seriesObj["MetaHlogSplitSizeMin"], seriesObj["MetaHlogSplitSizeMax"]], ["Min", "Max"])
					];
				});
				hbaseservers._promise.then(function (res) {
					var regionhealtynum = 0;
					var regiontotal = 0;
					var hmasteractive;
					var hmasterstandby;
					$.each(res, function (i, server) {
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
				})
			};


			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
