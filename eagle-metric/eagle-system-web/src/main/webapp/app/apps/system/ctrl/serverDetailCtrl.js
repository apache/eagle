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
		systemMetricApp.controller("serverDetailCtrl", function ($q, $wrapState, $scope, PageConfig, Time, SYSTEMMETRIC) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			$scope.hostname = $wrapState.param.hostname;
			PageConfig.title = 'System Node' + "(" + $scope.hostname + ")";
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
							return common.number.abbr(value, true);
						}
					}
				}],
				trans: true 
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

			function generateMetric(name, flag) {
				var result = cache[name] || generateChartMetric(name, flag);
				return result;
			}

			function generateChartMetric(name, flag){
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);
				$scope.site = $wrapState.param.siteId;
				var jobCond = {
					site: $scope.site,
					host: $scope.hostname
				};
				return SYSTEMMETRIC.aggMetricsToEntities(SYSTEMMETRIC.systemMetricsAggregation(jobCond, name, ["device"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
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
					if (typeof metricMap !== 'undefined') {
						$.each(metricMap, function (j, metriSeries) {
							var linename = [];
							if(typeof metriSeries.group !== 'undefined'){
								linename.push(metriSeries.group);
							}
							linename.push(legendName[i]);
							series.push(SYSTEMMETRIC.metricsToSeries(linename.join("."), metriSeries, option, dataOption.trans));
						});
					}
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
					name: "Cpu Usage",
					metrics: ["cputotalusage"],
					linename: ["usage"],
					option: digitalOption
				},
				{
					name: "Memory Usage",
					metrics: ["momoryusage"],
					linename: ["usage"],
					option: digitalOption
				},
				{
					name: "Network IO",
					metrics: ["receivedbytes", "transmitbytes"],
					linename: ["receive", "transmit"],
					option: sizeoption
				},
				{
					name: "Loadavg",
					metrics: ["1minloadavg", "5minloadavg", "15minloadavg"],
					linename: ["1minloadavg", "5minloadavg", "15minloadavg"],
					option: digitalOption
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
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var metricspromies = [];

				SYSTEMMETRIC.getMetricObj().then(function (res) {
					var overviewMetricList = res.systemnode;
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

				var condition = {
                    site: $scope.site,
                    role: "system",
                    hostname : $scope.hostname
                };
				SYSTEMMETRIC.hostStatus(condition, 1)._promise.then(function (res) {
					$scope.hostStatus = res;
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
})
();
