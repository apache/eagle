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
			$scope.hostname = "10.17.28.15";
			/*$scope.hostSelect = {
			 hostList: [
			 {ip: "10.17.28.182", host: "yhd-jqhadoop182.int.yihaodian.com"},
			 {ip: "10.17.28.183", host: "yhd-jqhadoop183.int.yihaodian.com"}
			 ],
			 selectedHost: {ip: "10.17.28.182", host: "yhd-jqhadoop182.int.yihaodian.com"}
			 };*/


			var METRIC_NAME_ARRAY = [
				["MemoryUsage", ["nonheap", "hadoop.memory.nonheapmemoryusage.used"]],
				["MemoryUsage", ["heap", "hadoop.memory.heapmemoryusage.used"]],
				["Master Averageload", ["averageload", "hadoop.hbase.master.server.averageload"]],
				["Ritcount", ["ritcount", "hadoop.hbase.master.assignmentmanger.ritcount"]],
				["Ritcountoverthreshold", ["ritcountoverthreshold", "hadoop.hbase.master.assignmentmanger.ritcountoverthreshold"]],
				["Assign", ["assignNumOps", "hadoop.hbase.master.assignmentmanger.assign_num_ops"]],
				["Assign", ["assignMin", "hadoop.hbase.master.assignmentmanger.assign_min"]],
				["Assign", ["assignMax", "hadoop.hbase.master.assignmentmanger.assign_max"]],
				["Assign Percentile", ["75th", "hadoop.hbase.master.assignmentmanger.assign_75th_percentile"],
					["95th", "hadoop.hbase.master.assignmentmanger.assign_95th_percentile"],
					["99th", "hadoop.hbase.master.assignmentmanger.assign_99th_percentile"]
				],
				["BulkAssign", ["bulkAssign_num_ops", "hadoop.hbase.master.assignmentmanger.bulkassign_num_ops"]],
				["BulkAssign", ["bulkAssign_min", "hadoop.hbase.master.assignmentmanger.bulkassign_min"]],
				["BulkAssign", ["bulkAssign_max", "hadoop.hbase.master.assignmentmanger.bulkassign_max"]],
				["BulkAssign Percentile", ["75th", "hadoop.hbase.master.assignmentmanger.bulkassign_75th_percentile"],
					["95th", "hadoop.hbase.master.assignmentmanger.bulkassign_95th_percentile"],
					["99th", "hadoop.hbase.master.assignmentmanger.bulkassign_99th_percentile"]
				],
				["BalancerCluster", ["balancerCluster_num_ops", "hadoop.hbase.master.balancer.balancercluster_num_ops"]],
				["BalancerCluster", ["balancerCluster_min", "hadoop.hbase.master.balancer.balancercluster_min"]],
				["BalancerCluster", ["balancerCluster_max", "hadoop.hbase.master.balancer.balancercluster_max"]],
				["BalancerCluster Percentile",
					["75th", "hadoop.hbase.master.balancer.balancercluster_75th_percentile"],
					["95th", "hadoop.hbase.master.balancer.balancercluster_95th_percentile"],
					["99th", "hadoop.hbase.master.balancer.balancercluster_99th_percentile"]
				],
				["HlogSplitTime", ["HlogSplitTime_min", "hadoop.hbase.master.filesystem.hlogsplittime_min"]],
				["HlogSplitTime", ["HlogSplitTime_max", "hadoop.hbase.master.filesystem.hlogsplittime_max"]],
				["BalancerCluster Percentile",
					["75th", "hadoop.hbase.master.filesystem.hlogsplittime_75th_percentile"],
					["95th", "hadoop.hbase.master.filesystem.hlogsplittime_95th_percentile"],
					["99th", "hadoop.hbase.master.filesystem.hlogsplittime_99th_percentile"]
				],
				["HlogSplitSize", ["Min", "hadoop.hbase.master.filesystem.hlogsplitsize_min"],
					["Max", "hadoop.hbase.master.filesystem.hlogsplitsize_max"]
				],
				["MetaHlogSplitTime", ["Min", "hadoop.hbase.master.filesystem.metahlogsplittime_min"],
					["Max", "hadoop.hbase.master.filesystem.metahlogsplittime_max"]
				],
				["MetaHlogSplitTime Percentile",
					["75th", "hadoop.hbase.master.filesystem.metahlogsplittime_75th_percentile"],
					["95th", "hadoop.hbase.master.filesystem.metahlogsplittime_95th_percentile"],
					["99th", "hadoop.hbase.master.filesystem.metahlogsplittime_99th_percentile"]
				],
				["MetaHlogSplitSize",
					["Min", "hadoop.hbase.master.filesystem.metahlogsplitsize_min"],
					["Max", "hadoop.hbase.master.filesystem.metahlogsplitsize_max"]
				]
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
									common.number.abbr(point.value, true);
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value, true);
						}
					}
				}]
			};
			$scope.metricList = {};


			// TODO: Optimize the chart count
			// TODO: ECharts dynamic refresh series bug: https://github.com/ecomfe/echarts/issues/4033
			$scope.refresh = function () {
				var startTime = Time.startTime();
				var endTime = Time.endTime();


				function generateHbaseMetric(name, option, dataOption, limit) {
					limit = limit || 20;
					var count = name.length - 1 || 1;
					var hbaseMetric = [];
					var series = [];

					var startTime = Time.startTime();
					var endTime = Time.endTime();

					$scope.site = $wrapState.param.siteId;
					var jobCond = {
						site: $scope.site,
						component: "hbasemaster",
						host: $scope.hostname
					};

					for (var i = 1; i <= count; i += 1) {
						var hbaseMetricsPromise = cache[name[i][1]] = cache[name[i][1]] || METRIC.hbaseMetrics(jobCond, name[i][1], startTime, endTime, limit)._promise;
						hbaseMetric.push(hbaseMetricsPromise);
					}
					return $q.all(hbaseMetric).then(function (res) {
						for (var i = 0; i < count; i += 1) {
							var data = [];
							data = $.map(res[i], function (metric) {
								return {
									x: metric.timestamp,
									y: metric.value[0]
								};
							});
							series.push($.extend({
								name: name[i + 1][0],
								type: 'line',
								data: data,
								showSymbol: false
							}, option));
						}

						return {
							title: name[0],
							series: series,
							dataOption: dataOption || {}
						};
					});
				}

				var hbaseservers = METRIC.hbasehostStatus({site: $scope.site});
				$q.all([
					generateHbaseMetric(METRIC_NAME_ARRAY[0], {smooth: true}, storageOption),
					generateHbaseMetric(METRIC_NAME_ARRAY[1], {smooth: true}, storageOption),
					generateHbaseMetric(METRIC_NAME_ARRAY[2], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[3], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[4], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[5], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[6], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[7], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[8], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[9], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[10], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[11], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[12], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[13], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[14], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[15], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[16], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[17], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[18], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[19], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[20], {}, storageOption),
					generateHbaseMetric(METRIC_NAME_ARRAY[21], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[22], {}),
					generateHbaseMetric(METRIC_NAME_ARRAY[23], {}),
					hbaseservers
				]).then(function (res) {
					$scope.metricList = [
						res[0], res[1], res[2], res[3], res[4],
						res[5], res[6], res[7], res[8], res[9],
						res[10], res[11], res[12], res[13], res[14],
						res[15], res[16], res[17], res[18], res[19],
						res[20], res[21], res[22], res[23]
					];
					var regionhealtynum = 0;
					var regiontotal = 0;
					var hmasteractive;
					var hmasterstandby;
					$.each(res[24], function (i, server) {
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
//# sourceURL=overview.js
