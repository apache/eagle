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
		jpmApp.controller("overviewCtrl", function ($q, $wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Overview";

			$scope.type = "job";

			$scope.switchType = function (type) {
				$scope.type = type;
				$scope.refresh();
			};

			$scope.commonOption = {
				animation: false,
				grid: {
					top: 60
				}
			};

			// ======================================================================
			// =                          Refresh Overview                          =
			// ======================================================================
			// TODO: ECharts dynamic refresh series bug: https://github.com/ecomfe/echarts/issues/4033
			$scope.refresh = function () {
				function getTopList(metric, aggregation, scopeVariable) {
					var deferred = $q.defer();

					if(scopeVariable) {
						$scope[scopeVariable] = [];
					}

					JPM.aggMetrics({site: $scope.site}, metric, [aggregation], "sum(value) desc", false, startTime, endTime, 10)
						._promise
						.then(function (list) {
							// Get name list
							return $.map(list, function (obj) {
								return obj.key[0];
							});
						})
						.then(function (list) {
							var promiseList = $.map(list, function (name, i) {
								return JPM.aggMetricsToEntities(
									JPM.aggMetrics({site: $scope.site, jobId: name}, metric, ["site"], "max(value)", intervalMin, startTime, endTime)
								)._promise.then(function (list) {
									return $.extend({
										stack: "job",
										areaStyle: {normal: {}}
									}, JPM.metricsToSeries(name, list));
								});
							});

							$q.all(promiseList).then(function (series) {
								if(scopeVariable) {
									$scope[scopeVariable] = series;
								}
								deferred.resolve(series);
							});
						});

					return deferred.promise;
				}

				var startTime = Time.startTime();
				var endTime = Time.endTime();
				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;

				console.log("refresh!!!");

				getTopList("hadoop.job.history.minute.cpu_milliseconds", "jobId", "cpuUsageSeries");
				getTopList("hadoop.job.history.minute.hdfs_bytes_read", "jobId", "hdfsBtyesReadSeries");
				getTopList("hadoop.job.history.minute.hdfs_bytes_written", "jobId", "hdfsBtyesWrittenSeries");
				getTopList("hadoop.job.history.minute.hdfs_read_ops", "jobId", "hdfsReadOpsSeries");
				getTopList("hadoop.job.history.minute.hdfs_write_ops", "jobId", "hdfsWriteOpsSeries");
				getTopList("hadoop.job.history.minute.file_bytes_read", "jobId", "fileBytesReadSeries");
				getTopList("hadoop.job.history.minute.file_bytes_written", "jobId", "fileBytesWrittenSeries");
			};

			Time.onReload($scope.refresh, $scope);
			$scope.refresh();
		});
	});
})();
