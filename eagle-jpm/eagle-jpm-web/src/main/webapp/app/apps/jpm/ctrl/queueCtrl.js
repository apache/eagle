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
		var QUEUE_ROOT = 'unassigned';

		jpmApp.controller("queueCtrl", function ($q, $wrapState, $element, $scope, $timeout, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;
			$scope.currentQueue = $wrapState.param.queue;
			$scope.selectedQueue = '';
			$scope.selectedSubQueue = '';
			$scope.trendLoading = true;

			PageConfig.title = "Queue";
			PageConfig.subTitle = $scope.currentQueue || "Overview";
			var navPath = PageConfig.navPath = [];

			$scope.chartOption = {
				tooltip: {
					formatter: function (points) {
						return points[0].name + "<br/>" +
							$.map(points, function (point) {
								return '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
									point.seriesName + ": " +
									Math.floor(point.value) + '%';
							}).reverse().join("<br/>");
					}
				},
				yAxis: [{
					axisLabel: {formatter: function (value) {
						return value + '%';
					}}
				}]
			};

			// Load queue tree
			(function () {
				var startTime = new Time('startTime');
				var endTime = startTime.clone().add(1, 'h');
				JPM
					.groups('RunningQueueService', { site: $scope.site, queue: $scope.currentQueue }, ['queue', 'parentQueue'], 'count', 60, startTime, endTime)
					._promise
					.then(function (list) {
						$.each(list, function (i, entity) {
							var parent = entity.key[1];
							$scope.parentQueue = parent === QUEUE_ROOT ? null : parent;

							// Update navigation path
							navPath.push(
								{
									title: $scope.parentQueue || 'queue list',
									icon: 'sitemap',
									param: ['siteId', 'startTime', 'endTime', $scope.parentQueue && 'queue=' + $scope.parentQueue],
									path: "/jpm/queue"
								},
								{title: $scope.currentQueue}
							);

							return false;
						});
					});
			})();

			// Refresh Trend Chart
			$scope.refresh = function () {
				$scope.trendLoading = true;
				var startTime = new Time('startTime');
				var endTime = new Time('endTime');
				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;
				var condition = {site: $scope.site};
				if ($scope.currentQueue) condition.parentQueue = $scope.currentQueue;

				var promiseList = [];
				// Load sub queue trend
				promiseList.push(JPM.aggMetricsToEntities(
					JPM.groups('RunningQueueService', condition, ['queue', 'parentQueue'], 'max(absoluteUsedCapacity)', intervalMin, startTime, endTime)
				)._promise.then(function (list) {
					$scope.subQueueList = [];

					// Filter top queue
					var queueTrendSeries = $.map(list, function (subList) {
						var tags = subList[0].tags;
						if (!$scope.currentQueue && tags.parentQueue !== QUEUE_ROOT) return;

						var name = subList[0].tags.queue;
						$scope.subQueueList.push(name);
						return JPM.metricsToSeries(name, subList, {
							stack: "queue",
							areaStyle: {normal: {}}
						});
					});

					$scope.selectedQueue = common.getValueByPath(queueTrendSeries, ['0', 'name']);
					$scope.refreshQueueStatistic();

					return queueTrendSeries;
				}));

				if ($scope.currentQueue) {
					// Load current queue trend
					promiseList.push(JPM.aggMetricsToEntities(
						JPM.groups('RunningQueueService', { site: $scope.site, queue: $scope.currentQueue }, ['queue'], 'max(absoluteUsedCapacity)', intervalMin, startTime, endTime)
					)._promise.then(function (list) {
						// Filter top queue
						var queueTrendSeries = $.map(list, function (subList) {
							return JPM.metricsToSeries(subList[0].tags.queue, subList, {
								areaStyle: {normal: {}}
							});
						});

						return queueTrendSeries;
					}));

					// Load parent capacity line
					var DISPLAY_MARK_NAME = ['Guaranteed Capacity', 'Max Capacity'];
					promiseList.push(JPM.aggMetricsToEntities(
						JPM.groups('RunningQueueService', { site: $scope.site, queue: $scope.currentQueue }, ['queue'], 'max(absoluteCapacity), max(absoluteMaxCapacity)', intervalMin, startTime, endTime)
					)._promise.then(function (list) {
						return $.map(list, function (valueSeries, i) {
							var pointValue = common.getValueByPath(valueSeries, [0, 'value', 0], 0);

							return JPM.metricsToSeries(DISPLAY_MARK_NAME[i], valueSeries, {
								markPoint: {
									silent: true,
									label: {
										normal: {
											formatter: function () {
												return DISPLAY_MARK_NAME[i];
											},
											position: 'insideRight',
											textStyle: {
												color: '#333',
												fontWeight: 'bolder',
												fontSize: 14,
											}
									}
									},
									data: [
										{
											name: '',
											coord: [valueSeries.length - 1, pointValue],
											symbolSize: 40,
											itemStyle: {
												normal: {color: 'rgba(0,0,0,0)'}
											}
										}
									],
								},
								lineStyle: {
									normal: { type: 'dashed' }
								}
							});
						});
					}));
				}

				$q.all(promiseList).then(function (seriesList) {
					$scope.queueTrendSeries = seriesList[0];

					if (seriesList[1]) {
						if (!seriesList[0].length) {
							$scope.queueTrendSeries = seriesList[1];
						}
						$scope.queueTrendSeries = $scope.queueTrendSeries.concat(seriesList[2]);
					}
					$scope.trendLoading = false;
				});
			};

			// Refresh Queue static info
			$scope.refreshQueueStatistic = function () {
				var startTime = new Time('startTime');
				var endTime = new Time('endTime');
			};

			// Go to sub queue view
			$scope.switchToSubQueue = function () {
				$wrapState.go(".", {
					queue: $scope.selectedSubQueue,
					startTime: Time.format('startTime'),
					endTime: Time.format('endTime'),
				});
			};

			Time.onReload(function () {
				$scope.refresh();
			}, $scope);
			$scope.refresh();

		});
	});
})();
