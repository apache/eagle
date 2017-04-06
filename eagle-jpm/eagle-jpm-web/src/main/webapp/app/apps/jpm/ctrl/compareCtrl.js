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
		/**
		 * @param {{}} $scope
		 * @param {{}} $scope.trendChart
		 */
		jpmApp.controller("compareCtrl", function ($q, $wrapState, $scope, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;
			$scope.jobDefId = $wrapState.param.jobDefId;

			$scope.jobTrendCategory = [];

			$scope.fromJob = null;
			$scope.toJob = null;

			PageConfig.title = "Job History";
			PageConfig.subTitle = $scope.jobDefId;

			$scope.getStateClass = JPM.getStateClass;

			var browserAction = true;

			// ==================================================================
			// =                         Fetch Job List                         =
			// ==================================================================
            var endTime = new Time();
            var startTime = endTime.clone().subtract(365,'day');
            var condition = JPM.condition({site: $scope.site, jobDefId:$scope.jobDefId});
			var jobList = $scope.jobList = JPM.list("JobExecutionService", condition, startTime, endTime, [], 10000);
			jobList._promise.then(function () {
				if(jobList.length <= 1) {
					$.dialog({
						title: "No statistic info",
						content: "Current job do not have enough statistic info. Please check 'jobDefId' if your job have run many times."
					});
				}

				function findJob(jobId) {
					return common.array.find(jobId, jobList, "tags.jobId");
				}

				var TASK_BUCKET_TIMES = [0, 30, 60, 120, 300, 600, 1800, 3600, 7200, 18000];
				function taskDistribution(jobId) {
					return JPM.taskDistribution($scope.site, jobId, TASK_BUCKET_TIMES.join(",")).then(
						/**
						 * @param {{}} res
						 * @param {{}} res.data
						 * @param {[]} res.data.finishedTaskCount
						 * @param {[]} res.data.runningTaskCount
						 */
						function (res) {
							var result = {};
							var data = res.data;
							var finishedTaskCount = data.finishedTaskCount;
							var runningTaskCount = data.runningTaskCount;

							/**
							 * @param {number} item.taskCount
							 */
							var finishedTaskData = $.map(finishedTaskCount, function (item) {
								return item.taskCount;
							});
							/**
							 * @param {number} item.taskCount
							 */
							var runningTaskData = $.map(runningTaskCount, function (item) {
								return item.taskCount;
							});

							result.taskSeries = [{
								name: "Finished Tasks",
								type: "bar",
								stack: jobId,
								data: finishedTaskData
							}, {
								name: "Running Tasks",
								type: "bar",
								stack: jobId,
								data: runningTaskData
							}];

							result.finishedTaskCount = finishedTaskCount;
							result.runningTaskCount = runningTaskCount;

							return result;
						});
				}

				var taskDistributionCategory = $.map(TASK_BUCKET_TIMES, function (current, i) {
					var curDes = Time.diffStr(TASK_BUCKET_TIMES[i] * 1000);
					var nextDes = Time.diffStr(TASK_BUCKET_TIMES[i + 1] * 1000);

					if(!curDes && nextDes) {
						return "<" + nextDes;
					} else if(nextDes) {
						return curDes + "\n~\n" + nextDes;
					}
					return ">" + curDes;
				});

				// ========================= Job Trend ==========================
				function refreshParam() {
					browserAction = false;
					$wrapState.go(".", {
						from: common.getValueByPath($scope.fromJob, "tags.jobId"),
						to: common.getValueByPath($scope.toJob, "tags.jobId")
					});
					setTimeout(function () {
						browserAction = true;
					}, 0);
				}

				function getMarkPoint(name, x, y, color) {
					return {
						name: name,
						silent: true,
						coord: [x, y],
						symbolSize: 20,
						label: {
							normal: { show: false },
							emphasis: { show: false }
						}, itemStyle: {
							normal: { color: color }
						}
					};
				}

				function refreshTrendMarkPoint() {
					var fromX = null, fromY = null;
					var toX = null, toY = null;
					$.each(jobList, function (index, job) {
						if($scope.fromJob && $scope.fromJob.tags.jobId === job.tags.jobId) {
							fromX = index;
							fromY = job.durationTime;
						}
						if($scope.toJob && $scope.toJob.tags.jobId === job.tags.jobId) {
							toX = index;
							toY = job.durationTime;
						}
					});

					markPoint.data = [];
					if(!common.isEmpty(fromX)) {
						markPoint.data.push(getMarkPoint("<From Job>", fromX, fromY, "#00c0ef"));
					}
					if(!common.isEmpty(toX)) {
						markPoint.data.push(getMarkPoint("<To Job>", toX, toY, "#3c8dbc"));
					}

					$scope.trendChart.refresh();
				}

				var jobListTrend = $.map(jobList, function (job) {
					var time = Time.format(job.startTime);
					$scope.jobTrendCategory.push(time);
					return job.durationTime;
				});

				$scope.jobTrendOption = {
					yAxis: [{
						axisLabel: {
							formatter: function (value) {
								return Time.diffStr(value);
							}
						}
					}],
					tooltip: {
						formatter: function (points) {
							var point = points[0];
							return point.name + "<br/>" +
								'<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + point.color + '"></span> ' +
								point.seriesName + ": " + Time.diffStr(point.value);
						}
					}
				};

				var markPoint = {
					data: [],
					silent: true
				};

				$scope.jobTrendSeries = [{
					name: "Job Duration",
					type: "line",
					data: jobListTrend,
					symbolSize: 10,
					showSymbol: false,
					markPoint: markPoint
				}];

				$scope.compareJobSelect = function (e, job) {
					var index = e.dataIndex;
					job = job || jobList[index];
					if(!job) return;

					var event = e.event ? e.event.event : e;

					if(event.ctrlKey) {
						$scope.fromJob = job;
					} else if(event.shiftKey) {
						$scope.toJob = job;
					} else {
						if ($scope.fromJob) {
							$scope.toJob = $scope.fromJob;
						}
						$scope.fromJob = job;
					}

					refreshTrendMarkPoint();
					refreshParam();
					refreshComparisonDashboard();
				};

				$scope.exchangeJobs = function () {
					var tmpJob = $scope.fromJob;
					$scope.fromJob = $scope.toJob;
					$scope.toJob = tmpJob;
					refreshTrendMarkPoint();
					refreshParam();
					refreshComparisonDashboard();
				};

				// ======================= Job Comparison =======================
				$scope.taskOption = {
					xAxis: {axisTick: { show: true }}
				};

				function getComparedValue(path) {
					var val1 = common.getValueByPath($scope.fromJob, path);
					var val2 = common.getValueByPath($scope.toJob, path);
					return common.number.compare(val1, val2);
				}

				$scope.jobCompareClass = function (path) {
					var diff = getComparedValue(path);
					if(typeof diff !== "number" || Math.abs(diff) < 0.01) return "hide";
					if(diff < 0.05) return "label label-success";
					if(diff < 0.15) return "label label-warning";
					return "label label-danger";
				};

				$scope.jobCompareValue = function (path) {
					var diff = getComparedValue(path);
					return (diff >= 0 ? "+" : "") + Math.floor(diff * 100) + "%";
				};

				/**
				 * get 2 interval data list category. (minutes level)
				 * @param {[]} list1
				 * @param {[]} list2
				 * @return {Array}
				 */
				function intervalCategories(list1, list2) {
					var len = Math.max(list1.length, list2.length);
					var category = [];
					for(var i = 0 ; i < len ; i += 1) {
						category.push((i + 1) + "min");
					}
					return category;
				}

				function refreshComparisonDashboard() {
					if(!$scope.fromJob || !$scope.toJob) return;

					var fromJobCond = {
						jobId: $scope.fromJob.tags.jobId,
						site: $scope.site
					};
					var toJobCond = {
						jobId: $scope.toJob.tags.jobId,
						site: $scope.site
					};

					var from_startTime = Time($scope.fromJob.startTime).subtract(1, "hour");
					var from_endTime = ($scope.fromJob.endTime ? Time($scope.fromJob.endTime) : Time()).add(1, "hour");
					var to_startTime = Time($scope.toJob.startTime).subtract(1, "hour");
					var to_endTime = ($scope.toJob.endTime ? Time($scope.toJob.endTime) : Time()).add(1, "hour");

					$scope.fromJob._cache = $scope.fromJob._cache || {};
					$scope.toJob._cache = $scope.toJob._cache || {};

					/**
					 * Generate metric level chart series
					 * @param {string} metric
					 * @param {string} seriesName
					 */
					function metricComparison(metric, seriesName) {
						var from_metric = $scope.fromJob._cache[metric] =
							$scope.fromJob._cache[metric] || JPM.metrics(fromJobCond, metric, from_startTime, from_endTime);
						var to_metric = $scope.toJob._cache[metric] =
							$scope.toJob._cache[metric] || JPM.metrics(toJobCond, metric, to_startTime, to_endTime);

						var holder = {};

						$q.all([from_metric._promise, to_metric._promise]).then(function () {
							from_metric = JPM.metricsToInterval(from_metric, 1000 * 60);
							to_metric = JPM.metricsToInterval(to_metric, 1000 * 60);

							var series_from = JPM.metricsToSeries("from job " + seriesName, from_metric, true);
							var series_to = JPM.metricsToSeries("to job " + seriesName, to_metric, true);

							holder.categories = intervalCategories(from_metric, to_metric);
							holder.series = [series_from, series_to];
						});

						return holder;
					}

					// Dashboard1: Containers metrics
					$scope.comparisonChart_Container = metricComparison("hadoop.job.runningcontainers", "running containers");

					// Dashboard 2: Allocated
					$scope.comparisonChart_allocatedMB = metricComparison("hadoop.job.allocatedmb", "allocated MB");

					// Dashboard 3: vCores
					$scope.comparisonChart_vCores = metricComparison("hadoop.job.allocatedvcores", "vCores");

					// Dashboard 4: Task distribution
					var from_distributionPromise = $scope.fromJob._cache.distributionPromise =
						$scope.fromJob._cache.distributionPromise || taskDistribution($scope.fromJob.tags.jobId);
					var to_distributionPromise = $scope.toJob._cache.distributionPromise =
						$scope.toJob._cache.distributionPromise || taskDistribution($scope.toJob.tags.jobId);
					var comparisonChart_taskDistribution = $scope.comparisonChart_taskDistribution = {
						categories: taskDistributionCategory
					};

					$q.all([from_distributionPromise, to_distributionPromise]).then(function (args) {
						var from_data = args[0];
						var to_data = args[1];

						var from_taskSeries = $.map(from_data.taskSeries, function (series) {
							return $.extend({}, series, {name: "From " + series.name});
						});
						var to_taskSeries = $.map(to_data.taskSeries, function (series) {
							return $.extend({}, series, {name: "To " + series.name});
						});

						comparisonChart_taskDistribution.series = from_taskSeries.concat(to_taskSeries);
					});
				}

				// ======================== Job Refresh =========================
				function jobRefresh() {
					$scope.fromJob = findJob($wrapState.param.from);
					$scope.toJob = findJob($wrapState.param.to);
					$(window).resize();
					refreshTrendMarkPoint();
					refreshComparisonDashboard();
				}

				// ======================= Initialization =======================
				jobRefresh();

				$scope.$on('$locationChangeSuccess', function() {
					if(browserAction) {
						jobRefresh();
					}
				});
			});
		});
	});
})();
