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
			var i;
			var startTime, endTime;
			var metric_allocatedMB, metric_allocatedVCores, metric_runningContainers;
			var metric_taskDuration;
			var nodeTaskCountList;

			$scope.site = $wrapState.param.siteId;
			$scope.jobId = $wrapState.param.jobId;

			PageConfig.title = "Job";
			PageConfig.subTitle = $scope.jobId;

			$scope.getStateClass = JPM.getStateClass;

			var jobCond = {
				jobId: $scope.jobId,
				site: $scope.site
			};

			function metricsToSeries(name, metrics) {
				var data = $.map(metrics, function (metric) {
					return {
						x: metric.timestamp,
						y: metric.value[0]
					};
				});
				return {
					name: name,
					type: "line",
					data: data
				};
			}

			JPM.jobList(jobCond)._promise.then(function (list) {
				$scope.job = list[list.length - 1];
				$scope.job = list[0];
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

				if($scope.isRunning) {
					// TODO: Time series interval required

					/*var taskList = JPM.list("RunningTaskExecutionService", jobCond, startTime, endTime, [
						"elapsedTime",
						"taskId",
						"status"
					]);

					console.log(taskList);*/

					// Dashboard 1: Allocated, vCores & Containers metrics
					metric_allocatedMB = JPM.metrics(jobCond, "hadoop.job.allocatedmb", startTime, endTime);
					metric_allocatedVCores = JPM.metrics(jobCond, "hadoop.job.allocatedvcores", startTime, endTime);
					metric_runningContainers = JPM.metrics(jobCond, "hadoop.job.runningcontainers", startTime, endTime);

					$q.all([metric_allocatedMB._promise, metric_allocatedVCores._promise, metric_runningContainers._promise]).then(function () {
						var series_allocatedMB = metricsToSeries("allocated MB", metric_allocatedMB);
						var series_allocatedVCores = metricsToSeries("vCores", metric_allocatedVCores);
						var series_runningContainers = metricsToSeries("running containers", metric_runningContainers);
						series_allocatedMB.yAxisIndex = 1;
						$scope.allocatedSeries = [series_allocatedMB, series_allocatedVCores, series_runningContainers];
					});

					// Dashboard 2: Task duration
					/*metric_taskDuration = JPM.metrics(jobCond, "hadoop.task.taskduration", startTime, endTime);
					metric_taskDuration._promise.then(function () {
						var series_taskDuration = metricsToSeries("task duration", metric_taskDuration);
						$scope.taskSeries = [series_taskDuration];
					});*/
				}

				// Dashboard 3: Running task
				nodeTaskCountList = JPM.groups("TaskExecutionService", jobCond, startTime, endTime, ["hostname"], "count", 1000000);
				nodeTaskCountList._promise.then(function () {
					var nodeTaskCountMap = [];

					$.each(nodeTaskCountList, function (i, obj) {
						var count = obj.value[0];
						nodeTaskCountMap[count] = (nodeTaskCountMap[count] || 0) + 1;
					});

					console.log("-->", nodeTaskCountMap.length);
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
					console.log(">>>", nodeTaskCountMap, $scope.nodeTaskCountCategory);
				});
			});
		});
	});
})();
