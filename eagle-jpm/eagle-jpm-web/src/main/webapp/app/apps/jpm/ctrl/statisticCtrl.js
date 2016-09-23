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

		var DURATION_BUCKETS = [0, 30, 60, 120, 300, 600, 1800, 3600, 2 * 3600, 3 * 3600];

		jpmApp.controller("statisticCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM, Chart) {
			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Job Statistics";

			$scope.type = "hourly";

			$scope.switchType = function (type) {
				$scope.type = type;
				$scope.refreshDistribution();
			};

			// ===============================================================
			// =                   Time Level Distribution                   =
			// ===============================================================
			function parseDayBuckets(startTime, endTime) {
				startTime = startTime.clone().hour(0).minute(0).second(0);
				endTime = endTime.clone().hour(0).minute(0).second(0);

				var _buckets = [];
				var _start = startTime.clone();

				do {
					var _end = _start.clone().date(1).add(1, "month").date(0);
					if (_end.isAfter(endTime)) {
						_end = endTime.clone();
					}
					var _dayDes = moment.duration(_end.diff(_start)).asDays() + 1;
					_buckets.push(_dayDes);

					_start = _end.clone().add(1, "day");
				} while (!_start.isAfter(endTime));

				return _buckets;
			}

			var distributionCache = {};
			$scope.distributionSelectedType = "";
			$scope.distributionSelectedIndex = -1;

			$scope.jobDistributionSeriesOption = {};

			$scope.refreshDistribution = function () {
				var type = $scope.type;
				var startTime, endTime;
				var metric, minInterval, field;
				$scope.distributionSelectedIndex = -1;

				switch (type) {
					case "monthly":
						endTime = Time("monthEnd");
						startTime = endTime.clone().subtract(365, "day").date(1).hours(0).minutes(0).seconds(0);
						metric = "hadoop.job.history.day.count";
						minInterval = 1440;
						field = "max(value)";
						break;
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
					if(type === "hourly") {
						return Time.format(value, "HH:mm");
					}
					return Time.format(value, "MM-DD");
				};
				var promise = distributionCache[type] = distributionCache[type] || JPM.aggMetricsToEntities(
					JPM.aggMetrics({site: $scope.site}, metric, ["jobStatus"], field, minInterval, startTime, endTime)
				)._promise.then(function (list) {
						if(type === "monthly") {
							var buckets = parseDayBuckets(startTime, endTime);
							list = $.map(list, function (units) {
								// Merge by day buckets
								units = units.concat();
								return [$.map(buckets, function (dayCount) {
									var subUnits = units.splice(0, dayCount);
									var sum = common.number.sum(subUnits, ["value", 0]);

									return $.extend({}, subUnits[0], {
										value: [sum]
									});
								})];
							});
						}
						return list;
				}).then(function(list) {
					var series = $.map(list, function (metrics) {
						return JPM.metricsToSeries(metrics[0].tags["jobStatus"], metrics, {
							stack: "stack",
							type: "bar",
							itemStyle: {
								normal: {
									borderWidth: 2
								}
							}
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

			// ==============================================================
			// =                         Drill Down                         =
			// ==============================================================
			$scope.commonChartOption = {
				grid: {
					left: 42,
					bottom: 60,
					containLabel: false
				},
				yAxis: [{
					minInterval: 1
				}],
				xAxis: {
					axisLabel: {
						interval: 0
					}
				}
			};
			$scope.commonTrendChartOption = {
				yAxis: [{
					minInterval: 1
				}],
				grid: {
					top: 60,
					left: 42,
					bottom: 20,
					containLabel: false
				}
			};

			$scope.topUserJobCountSeries = [];
			$scope.topTypeJobCountSeries = [];

			$scope.drillDownCategoryFunc = function (value) {
				switch ($scope.type) {
					case "monthly":
						return Time.format(value, "MM-DD");
					case "weekly":
					case "daily":
						return Time.format(value, "MM-DD HH:mm");
					default:
						return Time.format(value, "HH:mm");
				}
			};

			$scope.bucketDurationCategory = [];
			$.each(DURATION_BUCKETS, function (i, start) {
				var end = DURATION_BUCKETS[i + 1];

				start *= 1000;
				end *= 1000;

				if(!start) {
					$scope.bucketDurationCategory.push("<" + Time.diffStr(end));
				} else if(!end) {
					$scope.bucketDurationCategory.push(">" + Time.diffStr(start));
				} else {
					$scope.bucketDurationCategory.push(Time.diffStr(start) + "\n~\n" + Time.diffStr(end));
				}
			});

			function flattenTrendSeries(name, series) {
				var len = series.length;
				var category = [];
				var data = [];
				var needBreakLine = series.length > 6;
				$.each(series, function (i, series) {
					category.push((needBreakLine && i % 2 !== 0 ? "\n" : "") + series.name);
					data.push(common.number.sum(series.data, ["y"]));
				});
				return {
					category: category.reverse(),
					series: [{
						name: name,
						data: data.reverse(),
						type: "bar",
						itemStyle: {
							normal: {
								color: function (point) {
									return Chart.color[len - point.dataIndex - 1];
								}
							}
						}
					}]
				};
			}

			$scope.distributionClick = function (event) {
				if(event.componentType !== "series") return;

				$scope.distributionSelectedType = event.seriesName;
				$scope.distributionSelectedIndex = event.dataIndex;
				var timestamp = 0;

				// Highlight logic
				$.each($scope.jobDistributionSeries, function (i, series) {
					timestamp = series.data[$scope.distributionSelectedIndex].x;

					common.merge(series, {
						itemStyle: {
							normal: {
								color: function (point) {
									if(point.seriesName === $scope.distributionSelectedType && point.dataIndex === $scope.distributionSelectedIndex) {
										return "#60C0DD";
									}
									return colorMap[point.seriesName];
								}
							}
						}
					});
				});

				// Data fetch
				var startTime = Time(timestamp);
				var endTime;

				switch ($scope.type) {
					case "monthly":
						endTime = startTime.clone().add(1, "month").subtract(1, "s");
						break;
					case "weekly":
						endTime = startTime.clone().add(7, "day").subtract(1, "s");
						break;
					case "daily":
						endTime = startTime.clone().add(1, "day").subtract(1, "s");
						break;
					case "hourly":
						endTime = startTime.clone().add(1, "hour").subtract(1, "s");
						break;
				}

				var intervalMin = Time.diffInterval(startTime, endTime) / 1000 / 60;

				// ===================== Top User Job Count =====================
				$scope.topUserJobCountSeries = [];
				$scope.topUserJobCountTrendSeries = [];
				JPM.aggMetricsToEntities(
					JPM.groups("JobExecutionService", {site: $scope.site, currentState: $scope.distributionSelectedType}, ["user"], "count desc", intervalMin, startTime, endTime, 10, 1000000)
				)._promise.then(function (list) {
					$scope.topUserJobCountTrendSeries = $.map(list, function (subList) {
						return JPM.metricsToSeries(subList[0].tags.user, subList, {
							stack: "user",
							areaStyle: {normal: {}}
						});
					});

					var flatten = flattenTrendSeries("User", $scope.topUserJobCountTrendSeries);
					$scope.topUserJobCountSeries = flatten.series;
					$scope.topUserJobCountSeriesCategory = flatten.category;
				});

				// ===================== Top Type Job Count =====================
				$scope.topTypeJobCountSeries = [];
				$scope.topTypeJobCountTrendSeries = [];
				JPM.aggMetricsToEntities(
					JPM.groups("JobExecutionService", {site: $scope.site, currentState: $scope.distributionSelectedType}, ["jobType"], "count desc", intervalMin, startTime, endTime, 10, 1000000)
				)._promise.then(function (list) {
					$scope.topTypeJobCountTrendSeries = $.map(list, function (subList) {
						return JPM.metricsToSeries(subList[0].tags.jobType, subList, {
							stack: "type",
							areaStyle: {normal: {}}
						});
					});

					var flatten = flattenTrendSeries("Job Type", $scope.topTypeJobCountTrendSeries);
					$scope.topTypeJobCountSeries = flatten.series;
					$scope.topTypeJobCountSeriesCategory = flatten.category;
				});

				if($scope.distributionSelectedType === "FAILED") {
					// ====================== Failure Job List ======================
					$scope.jobList = JPM.jobList({site: $scope.site, currentState: "FAILED"}, startTime, endTime, [
						"jobId",
						"jobName",
						"user",
						"startTime",
						"jobType"
					]);
				} else {
					// ============== Job Duration Distribution Count ===============
					$scope.jobList = null;

					$scope.jobDurationDistributionSeries = [];
					/**
					 * @param {{}} res
					 * @param {{}} res.data
					 * @param {[]} res.data.jobTypes
					 * @param {[]} res.data.jobCounts
					 */
					JPM.jobDistribution($scope.site, DURATION_BUCKETS.join(","), startTime, endTime).then(function (res) {
						var data = res.data;
						var jobTypes = {};
						$.each(data.jobTypes, function (i, type) {
							jobTypes[type] = [];
						});
						$.each(data.jobCounts, function (index, statistic) {
							$.each(statistic.jobCountByType, function (type, value) {
								jobTypes[type][index] = value;
							});
						});

						$scope.jobDurationDistributionSeries = $.map(jobTypes, function (list, type) {
							return {
								name: type,
								data: list,
								type: "bar",
								stack: "jobType"
							};
						});
					});
				}

				return true;
			};

			$scope.refreshDistribution();
		});
	});
})();
