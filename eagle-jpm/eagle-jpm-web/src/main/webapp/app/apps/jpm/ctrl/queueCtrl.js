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
		jpmApp.controller("queueCtrl", function ($q, $wrapState, $element, $scope, $timeout, PageConfig, Time, Entity, JPM) {
			PageConfig.title = "Queue";
			PageConfig.subTitle = "Overview";

			$scope.site = $wrapState.param.siteId;

			$scope.refresh = function () {
				console.log('refresh!!!');

				var startTime = new Time('startTime');
				var endTime = new Time('endTime');
				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;
				JPM.aggMetricsToEntities(
					JPM.groups('RunningQueueService', { site: $scope.site }, ['queue', 'parentQueue'], 'max(absoluteUsedCapacity)', intervalMin, startTime, endTime)
				)._promise.then(function (list) {
					/* $scope.topUserJobCountTrendSeries = $.map(list, function (subList) {
						return JPM.metricsToSeries(subList[0].tags.user, subList, {
							stack: "user",
							areaStyle: {normal: {}}
						});
					});

					var flatten = flattenTrendSeries("User", $scope.topUserJobCountTrendSeries);
					$scope.topUserJobCountSeries = flatten.series;
					$scope.topUserJobCountSeriesCategory = flatten.category; */
					console.log('~>', list);

					$scope.queueTrendSeries = $.map(list, function (subList) {
						// console.log('>>>', subList);

						return JPM.metricsToSeries(subList[0].tags.queue, subList, {
							stack: "queue",
							areaStyle: {normal: {}}
						});
					});
					console.log('=>', $scope.queueTrendSeries);
				});
			};

			Time.onReload(function () {
				$scope.refresh();
			}, $scope);
			$scope.refresh();
		});
	});
})();
