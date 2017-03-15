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
				return SYSTEMMETRIC.aggMetricsToEntities(SYSTEMMETRIC.systemMetricsAggregation(jobCond, name, ["site","hostname"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
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
						series.push(SYSTEMMETRIC.metricsToSeries(legendName[i], metricMap[0], option));
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
					linename: ["cputotalusage"],
					option: {}
				},
				{
					name: "Memory Usage",
					metrics: ["momoryusage"],
					linename: ["momoryusage"],
					option: {}
				},
				{
					name: "Network IO",
					metrics: ["receivedbytes", "transmitbytes"],
					linename: ["write", "read"],
					option: {}
				},
				{
					name: "OS Idletime",
					metrics: ["idletime"],
					linename: ["idletime"],
					option: {}
				},
				{
					name: "OS Idletime",
					metrics: ["idletime"],
					linename: ["idletime"],
					option: {}
				},
				{
					name: "Disk Usage",
					metrics: ["diskused"],
					linename: ["diskused"],
					option: {}
				},
				{
					name: "Disk IO",
					metrics: ["readrate", "writerate"],
					linename: ["readrate", "writerate"],
					option: {}
				},
				
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
					var overviewMetricList = res.totaleverage;
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
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})
();
