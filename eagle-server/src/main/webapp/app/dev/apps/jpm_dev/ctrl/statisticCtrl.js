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
		var colorMap = {
			"SUCCEEDED": "#00a65a",
			"FAILED": "#dd4b39",
			"KILLED": "#CCCCCC",
			"ERROR": "#f39c12"
		};

		jpmApp.controller("statisticCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Job Statistic";

			$scope.type = "daily";

			$scope.switchType = function (type) {
				$scope.type = type;
				$scope.refreshDistribution();
			};

			// Time level distribution
			var distributionCache = {};

			$scope.jobDistributionSeriesOption = {};

			$scope.refreshDistribution = function () {
				var startTime, endTime;
				var metric, minInterval, field;

				switch ($scope.type) {
					case "weekly":
						endTime = Time("weekEnd");
						startTime = Time("week").subtract(7 * 12, "day");
						metric = "hadoop.job.history.day.count";
						minInterval = 1440 * 7;
						field = "sum(value)";
						break;
					case "daily":
						endTime = Time("dayEnd");
						startTime = Time("day").subtract(31, "day");
						metric = "hadoop.job.history.day.count";
						minInterval = 1440;
						field = "max(value)";
						break;
					case "hourly":
						endTime = Time("hourEnd");
						startTime = Time("day").subtract(2, "day");
						metric = "hadoop.job.history.hour.count";
						minInterval = 60;
						field = "sum(value)";
						break;
				}

				$scope.jobDistributionSeries = [];
				$scope.jobDistributionCategoryFunc = function (value) {
					if($scope.type === "hourly") {
						return Time.format(value, "HH:mm");
					}
					return Time.format(value, "MM-DD");
				};
				var promise = distributionCache[$scope.type] = distributionCache[$scope.type] || JPM.aggMetricsToEntities(
					JPM.aggMetrics({site: $scope.site}, metric, ["jobStatus"], field, minInterval, startTime, endTime)
				)._promise.then(function (list) {
					var series = $.map(list, function (metrics) {
						return JPM.metricsToSeries(metrics[0].tags["jobStatus"], metrics, {
							stack: "stack",
							type: "bar"
						});
					});
					common.array.doSort(series, "name", true, ["SUCCEEDED", "FAILED", "KILLED", "ERROR"]);
					$scope.jobDistributionSeriesOption.color = $.map(series, function (series) {
						return colorMap[series.name];
					});

					return series;
				});

				promise.then(function(series) {
					$scope.jobDistributionSeries = series;
				});
			};

			$scope.refreshDistribution();

			/*$scope.commonChartOption = {
				grid: {
					top: 60
				}
			};

			$scope.refresh = function () {
				var endTime = Time("day");
				var startTime = endTime.clone().subtract(2, "hour");
				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;

				JPM.aggMetricsToEntities(
					JPM.groups("JobExecutionService", {site: $scope.site}, ["user"], "count desc", intervalMin, startTime, endTime, 10, 1000000)
				)._promise.then(function (list) {
					$scope.topUserJobCountSeries = $.map(list, function (subList) {
						return JPM.metricsToSeries(subList[0].tags.user, subList);
					});
					$scope.topUserJobCountSeries._done = true;
				});

				JPM.aggMetricsToEntities(
					JPM.groups("JobExecutionService", {site: $scope.site}, ["jobType"], "count desc", intervalMin, startTime, endTime, 10, 1000000)
				)._promise.then(function (list) {
					$scope.topTypeJobCountSeries = $.map(list, function (subList) {
						return JPM.metricsToSeries(subList[0].tags.jobType, subList);
					});
					$scope.topTypeJobCountSeries._done = true;
				});
			};

			Time.onReload($scope.refresh, $scope);
			$scope.refresh();*/
		});
	});
})();
