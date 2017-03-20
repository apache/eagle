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
	register(function (systemMetricApp) {
		systemMetricApp.controller("overviewCtrl", function ($q, $wrapState, $scope, PageConfig, SYSTEMMETRIC, Time) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			PageConfig.title = 'System Dashbord';

			$scope.coresList = aggGroup($scope.site, ["cores"], "count");

			var digitalKbOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value / 1024 /1024, false, 0) + "Gb";
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return common.number.abbr(value / 1024 /1024, false, 0);
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
				}],
				trans: true 
			};

			var averageOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									common.number.abbr(point.value, false);
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
							return common.number.abbr(value, false);
						}
					}
				}],
				avg: true
			};

			function countStatus(site, status, groups, filed, limit) {
				var jobCond = {
					site: site,
					status: status
				};
				return SYSTEMMETRIC.aggSystemInstance(jobCond, groups, filed, limit);
			}

			function aggGroup(site, groups, filed, limit) {
				var jobCond = {
					site: site
				};
				return SYSTEMMETRIC.aggSystemInstance(jobCond, groups, filed, limit);
			}

			function generateMetric(name, flag) {
				var result = cache[name] || generateOvewviewMetric(name, flag);
				return result;
			}

			function generateOvewviewMetric(name, flag){
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);
				$scope.site = $wrapState.param.siteId;
				var jobCond = {
					site: $scope.site
				};
				return SYSTEMMETRIC.aggMetricsToEntities(SYSTEMMETRIC.systemMetricsAggregation(jobCond, name, ["host","device"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
				._promise.then(function (list) {
					var metricFlag = $.map(list, function (metrics) {
						return metrics[0].flag;
					});
					return [metricFlag, list];
				});
			}

			function mergeMetricToOneSeries(metricTitle, metrics, legendName, dataOption, option) {
				var series = [];
				$.each(metrics, function (i, metricMap) {
					var mergeSeries = metricMap[0];
					$.each(metricMap, function (j, metriSeries) {
						if(j > 0){
							$.each(metriSeries, function(n, dataNode){
								mergeSeries[n].value[0] = mergeSeries[n].value[0] * 1 + dataNode.value * 1;
							});
						}
					});
					if(dataOption.avg){
						$.each(mergeSeries, function(m, item) {
							item.value[0] = item.value[0] / metricMap.length;
						});
					}
					series.push(SYSTEMMETRIC.metricsToSeries(legendName[i], mergeSeries, option, dataOption.trans));
				});
				return {
					title: metricTitle,
					series: series,
					option: dataOption || {},
					loading: false
				};
			}

			$scope.chartList = [
				{
					name: "Memory Usage",
					metrics: ["memfree"],
					linename: ["memfree"],
					option: digitalKbOption
				},
				{
					name: "Loadavg",
					metrics: ["1minloadavg", "5minloadavg", "15minloadavg"],
					linename: ["1minloadavg", "5minloadavg", "15minloadavg"],
					option: digitalOption
				},
				{
					name: "Cpu Usage",
					metrics: ["cputotalusage"],
					linename: ["cputotalusage"],
					option: averageOption
				},
				{
					name: "Network IO",
					metrics: ["receivedbytes", "transmitbytes"],
					linename: ["receive", "transmit"],
					option: sizeoption
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
				// status count
				countStatus($scope.site, "active", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.systemactivenum = data.value[0];
					});
				}, function () {
					$scope.systemactivenum = -1;
				});

				countStatus($scope.site, "warning", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.systemwarningnum = data.value[0];
					});
				}, function () {
					$scope.systemwarningnum = -1;
				});

				countStatus($scope.site, "error", ["site"], "count")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.systemerrornum = data.value[0];
					});
				}, function () {
					$scope.systemerrornum = -1;
				});

				// memoery count
				aggGroup($scope.site, ["site"], "sum(totalMemoryMB),sum(usedMemoryMB),sum(totalDiskGB),sum(usedDiskGB)")._promise.then(function (res) {
					$.map(res, function (data) {
						$scope.totalMemoryMB = data.value[0];
						$scope.usedMemoryMB = data.value[1];
						$scope.totalDiskGB = data.value[2];
						$scope.usedDiskGB = data.value[3];
					});
				}, function () {
					$scope.totalMemoryMB = -1;
					$scope.usedMemoryMB = -1;
					$scope.totalDiskGB = -1;
					$scope.usedDiskGB = -1;
				});

				// metric chart
				SYSTEMMETRIC.getMetricObj().then(function (res) {
					var overviewMetricList = res.totaleverage;
					console.log(overviewMetricList);
					$.each($scope.chartList, function (i) {
						var chart = $scope.chartList[i];
						var metricList = chart.metrics;
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricspromies = generateMetric(overviewMetricList[metricKey], metricKey);
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
								if (typeof rs !== 'undefined' && rs.length > 0) {
									series.push(rs);
								}
							}
							$scope.metricList[chartname] = mergeMetricToOneSeries(chartname, series, chart.linename, chart.option);
						});
					});
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
		});
	});
})();
