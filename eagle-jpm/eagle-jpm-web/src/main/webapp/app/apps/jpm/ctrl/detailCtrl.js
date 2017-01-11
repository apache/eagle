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

			// =========================================================================
			// =                               Fetch Job                               =
			// =========================================================================
			JPM.findMRJobs($scope.site, undefined, $scope.jobId)._promise.then(function (list) {
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
				$scope.startTimestamp = $scope.job.startTime;
				$scope.endTimestamp = $scope.job.endTime;
				$scope.isRunning = !$scope.job.currentState || ($scope.job.currentState || "").toUpperCase() === 'RUNNING';


				// =============================== task attempt ===============================
				if ($scope.job.currentState === 'FAILED') {
					$scope.taskAttemptList = JPM.list('TaskAttemptErrorCategoryService', {
						site: $scope.job.tags.site,
						jobId: $scope.job.tags.jobId
					},
						Time($scope.startTimestamp).subtract(1, 'day'),
						Time($scope.endTimestamp).add(1, 'day'));
					$scope.taskAttemptCategories = {};
					$scope.taskAttemptList._promise.then(function () {
						$.each($scope.taskAttemptList, function (i, attempt) {
							$scope.taskAttemptCategories[attempt.tags.errorCategory] =
								($scope.taskAttemptCategories[attempt.tags.errorCategory] || 0) + 1;
						});
					});
				}

				// ================================ dashboards ================================
				// Dashboard 1: Allocated MB
				metric_allocatedMB = JPM.metrics(jobCond, "hadoop.job.allocatedmb", startTime, endTime);

				metric_allocatedMB._promise.then(function () {
					var series_allocatedMB = JPM.metricsToSeries("allocated MB", metric_allocatedMB);
					$scope.allocatedSeries = [series_allocatedMB];
				});

				// Dashboard 2: vCores & Containers metrics
				metric_allocatedVCores = JPM.metrics(jobCond, "hadoop.job.allocatedvcores", startTime, endTime);
				metric_runningContainers = JPM.metrics(jobCond, "hadoop.job.runningcontainers", startTime, endTime);

				$q.all([metric_allocatedVCores._promise, metric_runningContainers._promise]).then(function () {
					var series_allocatedVCores = JPM.metricsToSeries("vCores", metric_allocatedVCores);
					var series_runningContainers = JPM.metricsToSeries("running containers", metric_runningContainers);
					$scope.vCoresSeries = [series_allocatedVCores, series_runningContainers];
				});

				// Dashboard 3: Task duration
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

				// Dashboard 4: Running task
				nodeTaskCountList = JPM.groups(
					$scope.isRunning ? "RunningTaskExecutionService" : "TaskExecutionService",
					jobCond, ["hostname"], "count", null, startTime, endTime, null, 1000000);
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
						type: "bar",
						data: nodeTaskCountMap
					}];
				});

			});
		});
	});
})();
