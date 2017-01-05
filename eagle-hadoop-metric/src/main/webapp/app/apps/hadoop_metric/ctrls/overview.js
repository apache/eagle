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
					if(typeof metricMap !== 'undefined') {
						series.push(METRIC.metricsToSeries(legendName[i], metricMap[0], option));
					}
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
				METRIC.getMetricObj().then(function (res) {
					var masterMetricList = res.master;
					for (var metricKey in masterMetricList) {
						metricspromies.push(generateHbaseMetric(masterMetricList[metricKey], metricKey));
					}
					$q.all(metricspromies).then(function (resp) {
						var metricObj = {};
						for(var i=0; i < resp.length; i+=1) {
							metricObj[resp[i][0]] = resp[i][1];
						}
						return metricObj;
					}).then(function (seriesObj) {
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

