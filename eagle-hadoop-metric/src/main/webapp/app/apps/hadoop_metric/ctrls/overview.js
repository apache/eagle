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
			var mapRes = common.map2;
			$scope.site = $wrapState.param.siteId;
			var activeMasterInfo = METRIC.hbaseActiveMaster($scope.site);
			var metricMap = common.map1;
			metricMap.put("nonheap", "hadoop.memory.nonheapmemoryusage.used");
			metricMap.put("heap", "hadoop.memory.heapmemoryusage.used");
			metricMap.put("averageload", "hadoop.hbase.master.server.averageload");
			metricMap.put("ritcount", "hadoop.hbase.master.assignmentmanger.ritcount");
			metricMap.put("ritcountoverthreshold", "hadoop.hbase.master.assignmentmanger.ritcountoverthreshold");
			metricMap.put("AssignNumOps", "hadoop.hbase.master.assignmentmanger.assign_num_ops");
			metricMap.put("AssignMin", "hadoop.hbase.master.assignmentmanger.assign_min");
			metricMap.put("AssignMax", "hadoop.hbase.master.assignmentmanger.assign_max");
			metricMap.put("AssignPercentile75th", "hadoop.hbase.master.assignmentmanger.assign_75th_percentile");
			metricMap.put("AssignPercentile95th", "hadoop.hbase.master.assignmentmanger.assign_95th_percentile");
			metricMap.put("AssignPercentile99th", "hadoop.hbase.master.assignmentmanger.assign_99th_percentile");
			metricMap.put("BulkAssignNum_ops", "hadoop.hbase.master.assignmentmanger.bulkassign_num_ops");
			metricMap.put("BulkAssignMin", "hadoop.hbase.master.assignmentmanger.bulkassign_min");
			metricMap.put("BulkAssignMax", "hadoop.hbase.master.assignmentmanger.bulkassign_max");
			metricMap.put("BulkAssignPercentile75th", "hadoop.hbase.master.assignmentmanger.bulkassign_75th_percentile");
			metricMap.put("BulkAssignPercentile95th", "hadoop.hbase.master.assignmentmanger.bulkassign_95th_percentile");
			metricMap.put("BulkAssignPercentile99th", "hadoop.hbase.master.assignmentmanger.bulkassign_99th_percentile");
			metricMap.put("BalancerClusterNum_ops", "hadoop.hbase.master.balancer.balancercluster_num_ops");
			metricMap.put("BalancerClusterMin", "hadoop.hbase.master.balancer.balancercluster_min");
			metricMap.put("BalancerClusterMax", "hadoop.hbase.master.balancer.balancercluster_max");
			metricMap.put("BalancerClusterPercentile75th", "hadoop.hbase.master.balancer.balancercluster_75th_percentile");
			metricMap.put("BalancerClusterPercentile95th", "hadoop.hbase.master.balancer.balancercluster_95th_percentile");
			metricMap.put("BalancerClusterPercentile99th", "hadoop.hbase.master.balancer.balancercluster_99th_percentile");
			metricMap.put("HlogSplitTimeMin", "hadoop.hbase.master.filesystem.hlogsplittime_min");
			metricMap.put("HlogSplitTimeMax", "hadoop.hbase.master.filesystem.hlogsplittime_max");
			metricMap.put("HlogSplitTimePercentile75th", "hadoop.hbase.master.filesystem.hlogsplittime_75th_percentile");
			metricMap.put("HlogSplitTimePercentile95th", "hadoop.hbase.master.filesystem.hlogsplittime_95th_percentile");
			metricMap.put("HlogSplitTimePercentile99th", "hadoop.hbase.master.filesystem.hlogsplittime_99th_percentile");
			metricMap.put("HlogSplitSizeMin", "hadoop.hbase.master.filesystem.hlogsplitsize_min");
			metricMap.put("HlogSplitSizeMax", "hadoop.hbase.master.filesystem.hlogsplitsize_max");
			metricMap.put("MetaHlogSplitTimeMin", "hadoop.hbase.master.filesystem.metahlogsplittime_min");
			metricMap.put("MetaHlogSplitTimeMax", "hadoop.hbase.master.filesystem.metahlogsplittime_max");
			metricMap.put("MetaHlogSplitTimePercentile75th", "hadoop.hbase.master.filesystem.metahlogsplittime_75th_percentile");
			metricMap.put("MetaHlogSplitTimePercentile95th", "hadoop.hbase.master.filesystem.metahlogsplittime_95th_percentile");
			metricMap.put("MetaHlogSplitTimePercentile99th", "hadoop.hbase.master.filesystem.metahlogsplittime_99th_percentile");
			metricMap.put("MetaHlogSplitSizeMin", "hadoop.hbase.master.filesystem.metahlogsplitsize_min");
			metricMap.put("MetaHlogSplitSizeMax", "hadoop.hbase.master.filesystem.metahlogsplitsize_max");

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

			function generateHbaseMetric(name, param) {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				$scope.site = $wrapState.param.siteId;

				var metrics = $q.all([activeMasterInfo._promise]).then(function (res) {
						var hostname = cache[hostname] = cache[hostname] || res[0][0].tags.hostname;
						$scope.defaultHostname = $wrapState.param.hostname || hostname;

						var jobCond = {
							site: $scope.site,
							component: "hbasemaster",
							host: $scope.defaultHostname
						};
						return METRIC.aggMetricsToEntities(METRIC.hbaseMetricsAggregation(jobCond, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime), param)
							._promise.then(function (list) {
								var metricFlag = $.map(list, function (metrics) {
									return metrics[0].flag;
								});
								return [metricFlag, list];
							});

					});
				return metrics;
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
				var summaryPromises = [];
				for (var i = 0; i < metricMap.keys().length; i += 1) {
					var key = [];
					key = metricMap.keys()[i];
					var value = metricMap.get(key);
					metricspromies.push(generateHbaseMetric(value, key));
				}
				summaryPromises.push(hbaseservers._promise);
				$q.all(metricspromies).then(function (res) {
					$.each(res, function (i, metrics) {
						mapRes.put(metrics[0], metrics[1]);
					});
					$scope.metricList = [
						mergeMetricToOneSeries("MemoryUsage", [mapRes.get("nonheap"), mapRes.get("heap")], ["nonheap", "heap"], storageOption),
						mergeMetricToOneSeries("Master Averageload", [mapRes.get("averageload")], ["averageload"]),
						mergeMetricToOneSeries("Ritcount", [mapRes.get("ritcount"), mapRes.get("ritcountoverthreshold")], ["ritcount", "ritcountoverthreshold"]),
						mergeMetricToOneSeries("Assign", [mapRes.get("AssignNumOps"), mapRes.get("AssignMin"), mapRes.get("AssignMax")], ["numOps", "Min", "Max"]),
						mergeMetricToOneSeries("Assign Percentile", [mapRes.get("AssignPercentile75th"), mapRes.get("AssignPercentile95th"), mapRes.get("AssignPercentile99th")], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BulkAssign", [mapRes.get("BulkAssignNum_ops"), mapRes.get("BulkAssignMin"), mapRes.get("BulkAssignMax")], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BulkAssign Percentile", [mapRes.get("BulkAssignPercentile75th"), mapRes.get("BulkAssignPercentile95th"), mapRes.get("BulkAssignPercentile99th")], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("BalancerCluster", [mapRes.get("BalancerClusterNum_ops"), mapRes.get("BalancerClusterMin"), mapRes.get("BalancerClusterMax")], ["num_ops", "min", "max"]),
						mergeMetricToOneSeries("BalancerCluster Percentile", [mapRes.get("BalancerClusterPercentile75th"), mapRes.get("BalancerClusterPercentile95th"), mapRes.get("BalancerClusterPercentile99th")], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitTime", [mapRes.get("HlogSplitTimeMin"), mapRes.get("HlogSplitTimeMax")], ["HlogSplitTime_min", "HlogSplitTime_max"]),
						mergeMetricToOneSeries("HlogSplitTime Percentile", [mapRes.get("HlogSplitTimePercentile75th"), mapRes.get("HlogSplitTimePercentile95th"), mapRes.get("HlogSplitTimePercentile99th")], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("HlogSplitSize", [mapRes.get("HlogSplitSizeMin"), mapRes.get("HlogSplitSizeMax")], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime", [mapRes.get("MetaHlogSplitTimeMin"), mapRes.get("MetaHlogSplitTimeMax")], ["Min", "Max"]),
						mergeMetricToOneSeries("MetaHlogSplitTime Percentile", [mapRes.get("MetaHlogSplitTimePercentile75th"), mapRes.get("MetaHlogSplitTimePercentile95th"), mapRes.get("MetaHlogSplitTimePercentile99th")], ["75th", "95th", "99th"]),
						mergeMetricToOneSeries("MetaHlogSplitSize", [mapRes.get("MetaHlogSplitSizeMin"), mapRes.get("MetaHlogSplitSizeMax")], ["Min", "Max"])
					];
				});
				$q.all(summaryPromises).then(function (res) {
					var regionhealtynum = 0;
					var regiontotal = 0;
					var hmasteractive;
					var hmasterstandby;
					$.each(res[0], function (i, server) {
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
				mapRes.clear();
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
//@ sourceURL=overview.js
