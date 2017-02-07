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
			var TEXT_MAX_CAPACITY = 'Max Capacity';
			var DISPLAY_MARK_NAME = ['Guaranteed Capacity', TEXT_MAX_CAPACITY];

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
						JPM.groups('RunningQueueService',
							{ site: $scope.site, queue: $scope.currentQueue },
							['queue'],
							'max(absoluteUsedCapacity), max(memory), max(absoluteCapacity), max(absoluteMaxCapacity)',
							intervalMin, startTime, endTime)
					)._promise.then(function (list) {
						// Filter top queue
						return $.map(list, function (valueSeries, seriesIndex) {
							var seriesName = valueSeries[0].tags.queue;
							var option = {};
							if (seriesIndex === 0) {
								option.areaStyle = {normal: {}};
							} else if (seriesIndex === 1) {
								return;
							} else {
								seriesName = DISPLAY_MARK_NAME[seriesIndex - 2];
								var markDisplayText = seriesName;

								if (seriesName === TEXT_MAX_CAPACITY) {
									var lastMemory = list[1][list[1].length - 1].value[0];
									var lastCapacity = list[0][list[0].length - 1].value[0];
									var lastMaxCapacity = list[seriesIndex][list[seriesIndex].length - 1].value[0];
									var lastMaxMemory = lastMemory / lastCapacity * lastMaxCapacity;
									lastMaxMemory *= 1024 * 1024;

									if (!isNaN(lastMaxMemory)) markDisplayText += '(Memory:' + common.number.abbr(lastMaxMemory, true, 0) + ')';
								}

								var pointValue = common.getValueByPath(valueSeries, [valueSeries.length - 1, 'value', 0], 0);
								option = {
									markPoint: {
										silent: true,
										label: {
											normal: {
												formatter: function () {
													return markDisplayText;
												},
												position: 'insideRight',
												textStyle: {
													color: '#333',
													fontSize: 12,
												}
											}
										},
										data: [
											{
												name: '',
												coord: [valueSeries.length - 1, pointValue],
												symbolSize: 30,
												itemStyle: {
													normal: {color: 'rgba(0,0,0,0)'}
												}
											}
										],
									},
									lineStyle: {
										normal: { type: 'dotted' }
									}
								};
							}

							return JPM.metricsToSeries(seriesName, valueSeries, option);
						});
					}));
				}

				$q.all(promiseList).then(function (seriesList) {

					var subQueuesSeries = seriesList[0];
					$scope.queueTrendSeries = subQueuesSeries;

					if (seriesList[1]) {
						var queueSeries = [seriesList[1][0]];
						var capacitySeries = seriesList[1].slice(1);

						if (!subQueuesSeries.length) {
							$scope.queueTrendSeries = queueSeries;
						}
						$scope.queueTrendSeries = $scope.queueTrendSeries.concat(capacitySeries);
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
