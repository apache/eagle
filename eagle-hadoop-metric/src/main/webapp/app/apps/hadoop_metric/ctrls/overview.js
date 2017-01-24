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
			var activeMasterInfo = METRIC.hbaseMaster($scope.site, "active", 1);

			PageConfig.title = 'HBase';
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
							return common.number.abbr(value, true);
						}
					}
				}]
			};


			function generateHbaseMetric(name, flag) {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				$scope.site = $wrapState.param.siteId;
				var result = cache[name] || activeMasterInfo._promise.then(function (res) {
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
				return result;
			}

			function mergeMetricToOneSeries(metricTitle, metrics, legendName, dataOption, option) {
				var series = [];

				$.each(metrics, function (i, metricMap) {
					if (typeof metricMap !== 'undefined') {
						series.push(METRIC.metricsToSeries(legendName[i], metricMap[0], option));
					}
				});
				return {
					title: metricTitle,
					series: series,
					option: dataOption || {},
					loading: false
				};
			}

			function countHBaseRole(site, status, role, groups, filed, limit) {
				var jobCond = {
					site: site,
					status: status,
					role: role
				};
				return METRIC.aggHBaseInstance(jobCond, groups, filed, limit);
			}

			function sumAllRegions(site, role, groups, filed, limit) {
				var jobCond = {
					site: site,
					role: role
				};
				return METRIC.aggHBaseInstance(jobCond, groups, filed, limit);
			}

			// TODO: Optimize the chart count
			// TODO: ECharts dynamic refresh series bug: https://github.com/ecomfe/echarts/issues/4033
			$scope.chatnameList = ["MemoryUsage", "Master Averageload", "Ritcount", "AssignOpsNum", "Assign", "BulkAssignOpsNum",
				"BulkAssign", "BalancerClusterOpsNum", "BalancerCluster", "HlogSplitTime", "HlogSplitTime Percentile",
				"HlogSplitSize", "MetaHlogSplitTime", "MetaHlogSplitTime Percentile", "MetaHlogSplitSize"];
			$scope.chatmetricList = [["nonheap", "heap"], ["averageload"], ["ritcount", "ritcountoverthreshold"], ["AssignNumOps"], ["AssignMin", "AssignMax", "AssignPercentile75th", "AssignPercentile95th", "AssignPercentile99th"],
				["BulkAssignNum_ops"], ["BulkAssignMin", "BulkAssignMax", "BulkAssignPercentile75th", "BulkAssignPercentile95th", "BulkAssignPercentile99th"], ["BalancerClusterNum_ops"],
				["BalancerClusterMin", "BalancerClusterMax", "BalancerClusterPercentile75th", "BalancerClusterPercentile95th", "BalancerClusterPercentile99th"],
				["HlogSplitTimeMin", "HlogSplitTimeMax"], ["HlogSplitTimePercentile75th", "HlogSplitTimePercentile95th", "HlogSplitTimePercentile99th"],
				["MetaHlogSplitTimeMin", "MetaHlogSplitTimeMax"], ["MetaHlogSplitTimeMin", "MetaHlogSplitTimeMax"],
				["MetaHlogSplitTimePercentile75th", "MetaHlogSplitTimePercentile95th", "MetaHlogSplitTimePercentile99th"], ["MetaHlogSplitSizeMin", "MetaHlogSplitSizeMax"]
			];
			$scope.chatdataoptionList = [storageOption, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}];
			$scope.chatlineNameList = [["nonheap", "heap"], ["averageload"], ["ritcount", "ritcountoverthreshold"], ["numOps"], ["min", "max", "75th", "95th", "99th"], ["num_ops"],
				["min", "max", "75th", "95th", "99th"], ["num_ops"], ["min", "max", "75th", "95th", "99th"], ["HlogSplitTime_min", "HlogSplitTime_max"], ["75th", "95th", "99th"], ["Min", "Max"], ["Min", "Max"], ["75th", "95th", "99th"], ["Min", "Max"]];
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

				METRIC.getMetricObj().then(function (res) {
					var masterMetricList = res.master;
					$.each($scope.chatmetricList, function (i) {
						var metricList = $scope.chatmetricList[i];
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricspromies = generateHbaseMetric(masterMetricList[metricKey], metricKey);
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
							$scope.metricList[charname] = mergeMetricToOneSeries(charname, series, $scope.chatlineNameList[k], $scope.chatdataoptionList[k]);
						});
					});
				});

				countHBaseRole($scope.site, "active", "hmaster", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.hmasteractivenum = data.value[0];
					});
				});
				countHBaseRole($scope.site, "standby", "hmaster", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.hmasterstandbynum = data.value[0];
					});
				});

				countHBaseRole($scope.site, "live", "regionserver", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.regionserverhealtynum = data.value[0];
					});
				});
				countHBaseRole($scope.site, "dead", "regionserver", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.regionserverunhealtynum = data.value[0];
					});
				});
				sumAllRegions($scope.site, "regionserver", ["site"], "sum(numRegions)")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.regionsnum = data.value[0];
					});
				});

				activeMasterInfo._promise.then(function (res) {
					var hostname = cache[hostname] = cache[hostname] || res[0].tags.hostname;
					$scope.defaultHostname = $wrapState.param.hostname || hostname;
					var jobCond = {
						site: $scope.site,
						component: "hbasemaster",
						host: $scope.defaultHostname
					};
					METRIC.hbaseMomentMetric(jobCond, "hadoop.hbase.master.server.averageload", 1).then(function (res) {
						$scope.hmasteraverageload = (typeof res.data.obj[0] !== 'undefined') ? res.data.obj[0].value[0] : "-1";
					});
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
