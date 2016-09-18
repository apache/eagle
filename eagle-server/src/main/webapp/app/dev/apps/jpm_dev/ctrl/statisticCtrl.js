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
		jpmApp.controller("statisticCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Job Statistic";

			$scope.type = "daily";

			$scope.switchType = function (type) {
				$scope.type = type;
				$scope.refresh();
			};

			$scope.commonChartOption = {
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
			$scope.refresh();
		});
	});
})();
