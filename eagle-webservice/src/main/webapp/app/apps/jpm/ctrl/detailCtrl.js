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
		jpmApp.controller("detailCtrl", function ($q, $wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			var TASK_BUCKET_TIMES = [0, 30, 60, 120, 300, 600, 1800, 3600, 7200, 18000];
			var i;
			var startTime, endTime;
			var metric_allocatedMB, metric_allocatedVCores, metric_runningContainers;
			var nodeTaskCountList;
			var historyJobList;

			$scope.site = $wrapState.param.siteId;
			$scope.jobId = $wrapState.param.jobId;

			PageConfig.title = "Job";
			PageConfig.subTitle = $scope.jobId;

			$scope.getStateClass = JPM.getStateClass;
			$scope.compareChart = null;

			var jobCond = {
				jobId: $scope.jobId,
				site: $scope.site
			};

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

			$scope.taskCategory = $.map(TASK_BUCKET_TIMES, function (current, i) {
				var curDes = Time.diffStr(TASK_BUCKET_TIMES[i] * 1000);
				var nextDes = Time.diffStr(TASK_BUCKET_TIMES[i + 1] * 1000);

				if(!curDes && nextDes) {
					return "<" + nextDes;
				} else if(nextDes) {
					return curDes + "\n~\n" + nextDes;
				}
				return ">" + curDes;
			});

			$scope.taskOption = {
				xAxis: {axisTick: { show: true }}
			};

			function intervalCategories(list1, list2) {
				var len = Math.max(list1.length, list2.length);
				var category = [];
				for(var i = 0 ; i < len ; i += 1) {
					category.push((i + 1) + "min");
				}
				return category;
			}

			function getComparedValue(path) {
				var val1 = common.getValueByPath($scope.comparedJob, path);
				var val2 = common.getValueByPath($scope.job, path);
				return common.number.compare(val1, val2);
			}

			$scope.jobCompareClass = function (path) {
				var diff = getComparedValue(path);
				if(typeof diff !== "number" || Math.abs(diff) < 0.01) return "hide";
				if(diff < 0.05) return "text-success";
				if(diff < 0.15) return "text-warning";
				return "text-danger";
			};

			$scope.jobCompareValue = function (path) {
				var diff = getComparedValue(path);
				return "(" + (diff >= 0 ? "+" : "") + Math.floor(diff * 100) + "%)";
			};

			// =========================================================================
			// =                               Fetch Job                               =
			// =========================================================================
			JPM.jobList(jobCond)._promise.then(function (list) {
				$scope.job = list[list.length - 1];
				console.log("[JPM] Fetch job:", $scope.job);

				if(!$scope.job) {
					$.dialog({
						title: "OPS!",
						content: "Job not found!"
					}, function () {
						$wrapState.go("jpmList", {siteId: $scope.site});
					});
					return;
				}

				startTime = Time($scope.job.startTime).subtract(1, "hour");
				endTime = Time().add(1, "hour");
				$scope.isRunning = !$scope.job.currentState || ($scope.job.currentState || "").toUpperCase() === 'RUNNING';

				// Dashboard 1: Allocated, vCores & Containers metrics
				metric_allocatedMB = JPM.metrics(jobCond, "hadoop.job.allocatedmb", startTime, endTime);
				metric_allocatedVCores = JPM.metrics(jobCond, "hadoop.job.allocatedvcores", startTime, endTime);
				metric_runningContainers = JPM.metrics(jobCond, "hadoop.job.runningcontainers", startTime, endTime);

				$q.all([metric_allocatedMB._promise, metric_allocatedVCores._promise, metric_runningContainers._promise]).then(function () {
					var series_allocatedMB = JPM.metricsToSeries("allocated MB", metric_allocatedMB);
					var series_allocatedVCores = JPM.metricsToSeries("vCores", metric_allocatedVCores);
					var series_runningContainers = JPM.metricsToSeries("running containers", metric_runningContainers);
					series_allocatedMB.yAxisIndex = 1;
					$scope.allocatedSeries = [series_allocatedMB, series_allocatedVCores, series_runningContainers];
				});

				// Dashboard 2: Task duration
				taskDistribution($scope.job.tags.jobId).then(function (data) {
					$scope.taskSeries = data.taskSeries;

					$scope.taskSeriesClick = function (e) {
						var taskCount = e.seriesIndex === 0 ? data.finishedTaskCount : data.runningTaskCount;
						$scope.taskBucket = taskCount[e.dataIndex];
					};

					$scope.backToTaskSeries = function () {
						$scope.taskBucket = null;
						setTimeout(function () {
							$(window).resize();
						}, 0);
					};
				});

				// Dashboard 3: Running task
				nodeTaskCountList = JPM.groups(
					$scope.isRunning ? "RunningTaskExecutionService" : "TaskExecutionService"
					, jobCond, startTime, endTime, ["hostname"], "count", 1000000);
				nodeTaskCountList._promise.then(function () {
					var nodeTaskCountMap = [];

					$.each(nodeTaskCountList, function (i, obj) {
						var count = obj.value[0];
						nodeTaskCountMap[count] = (nodeTaskCountMap[count] || 0) + 1;
					});

					$scope.nodeTaskCountCategory = [];
					for(i = 0 ; i < nodeTaskCountMap.length ; i += 1) {
						nodeTaskCountMap[i] = nodeTaskCountMap[i] || 0;
						$scope.nodeTaskCountCategory.push(i + " tasks");
					}

					nodeTaskCountMap.splice(0, 1);
					$scope.nodeTaskCountCategory.splice(0, 1);

					$scope.nodeTaskCountSeries = [{
						name: "node count",
						type: "line",
						data: nodeTaskCountMap
					}];
				});

				// ==================================================================
				// =                         Job Comparison                         =
				// ==================================================================
				if($scope.job.tags.jobDefId) {
					$scope.historyJobCategory = [];
					historyJobList = JPM.findMRJobs($scope.site, $scope.job.tags.jobDefId);
					historyJobList._promise.then(function () {
						/**
						 * @param {{}} job
						 * @param {number} job.durationTime
						 */
						var historyJobDurationList = $.map(historyJobList, function (job) {
							var time = Time.format(job.startTime);
							$scope.historyJobCategory.push(time);
							return job.durationTime;
						});

						var currentX = 0, currentY = 0;
						$.each(historyJobList, function (index, job) {
							if($scope.job.tags.jobId === job.tags.jobId) {
								currentX = index;
								currentY = job.durationTime;
							}
						});

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

						var markPoint = {
							data: [getMarkPoint("<Current Job>", currentX, currentY, "#3c8dbc")]
						};

						$scope.historyJobDurationSeries = [{
							name: "Job Duration",
							type: "line",
							data: historyJobDurationList,
							markPoint: markPoint
						}];

						$scope.compareJobSelect = function (e) {
							var index = e.dataIndex;
							var job = historyJobList[index];
							if(!job || job.tags.jobId === $scope.job.tags.jobId) return;
							$scope.comparedJob = job;

							markPoint.data[1] = getMarkPoint("<Compared Job>", index, job.durationTime, "#00c0ef");

							$scope.compareChart.refresh();

							// Clear
							$scope.comparisonContainerSeries = null;
							$scope.comparisonAllocatedMBSeries = null;

							// Compare Data
							var compared_metric_allocatedMB, compared_metric_runningContainers, compared_metric_allocatedVCores;

							var startTime = Time($scope.job.startTime).subtract(1, "hour");
							var endTime = Time().add(1, "hour");
							var jobCond = {
								jobId: job.tags.jobId,
								site: $scope.site
							};

							// Compared Dashboard 1: Containers metrics
							compared_metric_runningContainers = JPM.metrics(jobCond, "hadoop.job.runningcontainers", startTime, endTime);
							compared_metric_runningContainers._promise.then(function () {
								// Align the metrics from different job in same interval
								var interval_metric_runningContainers = JPM.metricsToInterval(metric_runningContainers, 1000 * 60);
								var compared_interval_metric_runningContainers = JPM.metricsToInterval(compared_metric_runningContainers, 1000 * 60);

								// Convert metrics to chart series
								var series_runningContainers = JPM.metricsToSeries("current job running containers", interval_metric_runningContainers, true);
								var series_compared_runningContainers = JPM.metricsToSeries("compared job running containers", compared_interval_metric_runningContainers, true);

								$scope.comparisonContainerCategories = intervalCategories(interval_metric_runningContainers, compared_interval_metric_runningContainers);
								$scope.comparisonContainerSeries = [series_runningContainers, series_compared_runningContainers];
							});

							// Compared Dashboard 2: Allocated
							compared_metric_allocatedMB = JPM.metrics(jobCond, "hadoop.job.allocatedmb", startTime, endTime);
							compared_metric_allocatedMB._promise.then(function () {
								// Align the metrics from different job in same interval
								var interval_metric_allocatedMB = JPM.metricsToInterval(metric_allocatedMB, 1000 * 60);
								var compared_interval_metric_allocatedMB = JPM.metricsToInterval(compared_metric_allocatedMB, 1000 * 60);

								// Convert metrics to chart series
								var series_allocatedMB = JPM.metricsToSeries("current job allocated MB", interval_metric_allocatedMB, true);
								var series_compared_allocatedMB = JPM.metricsToSeries("compared job allocated MB", compared_interval_metric_allocatedMB, true);

								$scope.comparisonAllocatedMBCategories = intervalCategories(interval_metric_allocatedMB, compared_interval_metric_allocatedMB);
								$scope.comparisonAllocatedMBSeries = [series_allocatedMB, series_compared_allocatedMB];
							});

							// Compared Dashboard 3: vCores
							compared_metric_allocatedVCores = JPM.metrics(jobCond, "hadoop.job.allocatedvcores", startTime, endTime);
							compared_metric_allocatedVCores._promise.then(function () {
								// Align the metrics from different job in same interval
								var interval_metric_allocatedVCores = JPM.metricsToInterval(metric_allocatedVCores, 1000 * 60);
								var compared_interval_metric_allocatedVCores = JPM.metricsToInterval(compared_metric_allocatedVCores, 1000 * 60);

								// Convert metrics to chart series
								var series_allocatedVCores = JPM.metricsToSeries("current job vCores", interval_metric_allocatedVCores, true);
								var series_compared_allocatedVCores = JPM.metricsToSeries("compared job vCores", compared_interval_metric_allocatedVCores, true);

								$scope.comparisonAllocatedVCoresCategories = intervalCategories(interval_metric_allocatedVCores, compared_interval_metric_allocatedVCores);
								$scope.comparisonAllocatedVCoresSeries = [series_allocatedVCores, series_compared_allocatedVCores];
							});

							// Compared Dashboard 2: Task duration
							taskDistribution($scope.comparedJob.tags.jobId).then(function (data) {
								var comparedTaskSeries = $.map(data.taskSeries, function (series) {
									return $.extend({}, series, {name: "Compared " + series.name});
								});
								$scope.comparedTaskSeries = $scope.taskSeries.concat(comparedTaskSeries);
							});
						};

						/*console.log("~>", historyJobList);
						console.log("~>", $scope.historyJobCategory);
						console.log("~>", historyJobDurationList);*/
						// TODO: wait for job task
					});
				}
			});
		});
	});
})();
