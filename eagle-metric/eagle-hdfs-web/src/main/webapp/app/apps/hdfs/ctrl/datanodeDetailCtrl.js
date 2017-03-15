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
	register(function (hdfsMetricApp) {
		hdfsMetricApp.controller("datanodeDetailCtrl", function ($q, $wrapState, $scope, PageConfig, Time, HDFSMETRIC) {
			var cache = {};
			$scope.site = $wrapState.param.siteId;
			$scope.hostname = $wrapState.param.hostname;
			PageConfig.title = 'Datanode ' + "(" + $scope.hostname + ")";
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

			var gctimeoption = {
				legend: {
					x: 'center', y: 'bottom'
				},
				yAxis: [{
					axisLabel: {
						formatter: function (value) {
							return value / 1000 + ' S';
						}
					}
				}]
			};
			$scope.chartList = [
				{
					name: "Memory Usage",
					metrics: ["nonheap", "heap"],
					option: sizeoption
				},
				{
					name: "Fsdatasetstate",
					metrics: ["capacity", "dfsused"],
					option: {}
				},
				{
					name: "Xceivercount",
					metrics: ["xceivercount"],
					option: {}
				},
				{
					name: "Rpcqueuetimeavgtime",
					metrics: ["rpcqueuetimeavgtime"],
					option: gctimeoption
				},
				{
					name: "Rpcprocessingtimeavgtime",
					metrics: ["rpcprocessingtimeavgtime"],
					option: gctimeoption
				},
				{
					name: "Numopenconnections",
					metrics: ["numopenconnections"],
					option: {}
				},
				{
					name: "Callqueuelength",
					metrics: ["callqueuelength"],
					option: {}
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

				HDFSMETRIC.getMetricObj().then(function (res) {
					var masterMetricList = res.datanode;
					$.each($scope.chartList, function (i) {
						var chart = $scope.chartList[i];
						var metricList = chart.metrics;
						$.each(metricList, function (j) {
							var metricKey = metricList[j];
							var metricspromies = generateHdfsMetric(masterMetricList[metricKey], startTime, endTime, metricKey);
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
								if (rs.length > 0) {
									series.push(rs);
								}
							}
							$scope.metricList[chartname] = mergeSeries(chartname, series, chart.metrics, chart.option);
						});
					});
				});

				HDFSMETRIC.getStatusByRoleAndHost("HdfsServiceInstance",$scope.hostname, "datanode", $scope.site)._promise.then(function (res) {
					$scope.datanodestatus = res;
				});
			};
			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();


			function generateHdfsMetric(name, startTime, endTime, flag) {
				var interval = Time.diffInterval(startTime, endTime);
				var intervalMin = interval / 1000 / 60;
				var trendStartTime = Time.align(startTime, interval);
				var trendEndTime = Time.align(endTime, interval);

				var condition = {
					site: $scope.site,
					component: "datanode",
					host: $scope.hostname
				};
				return HDFSMETRIC.aggMetricsToEntities(HDFSMETRIC.hadoopMetricsAggregation(condition, name, ["site"], "avg(value)", intervalMin, trendStartTime, trendEndTime), flag)
					._promise.then(function (list) {
						var metricFlag = $.map(list, function (metrics) {
							return metrics[0].flag;
						});
						return [metricFlag, list];
					});
			}

			function mergeSeries(title, metrics, linename, option) {
				var series = [];
				$.each(metrics, function (i, metricMap) {
					$.map(metricMap, function (metric) {
						if (typeof metric !== 'undefined') {
							series.push(HDFSMETRIC.metricsToSeries(linename[i], metric, option));
						}
					});
				});
				return {
					title: title,
					series: series,
					option: option || {},
					loading: false
				};
			}
		});
	});
})
();
