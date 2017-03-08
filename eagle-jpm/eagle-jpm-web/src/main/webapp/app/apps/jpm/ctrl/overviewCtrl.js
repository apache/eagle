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
	register(function (jpmApp) {
		jpmApp.controller("overviewCtrl", function ($q, $wrapState, $element, $scope, $timeout, PageConfig, Time, Entity, JPM) {
			var cache = {};
			$scope.aggregationMap = {
				job: "jobId",
				user: "user",
				jobType: "jobType",
				queue: "queue",
			};

			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Overview";

			$scope.type = "job";

			$scope.commonOption = {
				animation: false,
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
								$.map(points, function (point) {
									return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
											point.seriesName + ": " +
										common.number.format(point.value, true);
								}).reverse().join("<br/>");
					}
				},
				grid: {
					top: 70
				},
				yAxis: [{
					axisLabel: {formatter: function (value) {
						return common.number.abbr(value, true);
					}}
				}]
			};

			// ======================================================================
			// =                          Refresh Overview                          =
			// ======================================================================
			$scope.typeChange = function () {
				$timeout($scope.refresh, 1);
			};

			// TODO: Optimize the chart count
			// TODO: ECharts dynamic refresh series bug: https://github.com/ecomfe/echarts/issues/4033
			$scope.refresh = function () {
				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;

				function getTopList(metric, scopeVariable) {
					var deferred = $q.defer();

					metric = common.template(metric, {
						type: $scope.type.toLocaleLowerCase()
					});

					if(scopeVariable) {
						$scope[scopeVariable] = [];
						$scope[scopeVariable]._done = false;
						$scope[scopeVariable + "List"] = [];
					}

					var aggregation = $scope.aggregationMap[$scope.type];

					var aggPromise = cache[metric] = cache[metric] || JPM.aggMetricsToEntities(
						JPM.aggMetrics({site: $scope.site}, metric, [aggregation], "avg(value), sum(value) desc", intervalMin, startTime, endTime, 10)
					)._promise.then(function (list) {
						var series = $.map(list, function (metrics) {
							return JPM.metricsToSeries(metrics[0].tags[aggregation], metrics, {
								stack: "stack",
								areaStyle: {normal: {}}
							});
						});

						var topList = $.map(series, function (series) {
							return {
								name: series.name,
								total: common.number.sum(series.data, "y") * intervalMin
							};
						}).sort(function (a, b) {
							return b.total - a.total;
						});

						return [series, topList];
					});

					aggPromise.then(function (args) {
						if(scopeVariable) {
							$scope[scopeVariable] = args[0];
							$scope[scopeVariable]._done = true;
							$scope[scopeVariable + "List"] = args[1];
						}
					});

					return deferred.promise;
				}

				getTopList("hadoop.${type}.history.minute.cpu_milliseconds", "cpuUsageSeries");
				getTopList("hadoop.${type}.history.minute.physical_memory_bytes", "physicalMemorySeries");
				getTopList("hadoop.${type}.history.minute.virtual_memory_bytes", "virtualMemorySeries");
				getTopList("hadoop.${type}.history.minute.hdfs_bytes_read", "hdfsBtyesReadSeries");
				getTopList("hadoop.${type}.history.minute.hdfs_bytes_written", "hdfsBtyesWrittenSeries");
				getTopList("hadoop.${type}.history.minute.hdfs_read_ops", "hdfsReadOpsSeries");
				getTopList("hadoop.${type}.history.minute.hdfs_write_ops", "hdfsWriteOpsSeries");
				getTopList("hadoop.${type}.history.minute.file_bytes_read", "fileBytesReadSeries");
				getTopList("hadoop.${type}.history.minute.file_bytes_written", "fileBytesWrittenSeries");
			};

			Time.onReload(function () {
				cache = {};
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
