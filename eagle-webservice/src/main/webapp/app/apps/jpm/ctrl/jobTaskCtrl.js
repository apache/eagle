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
		var BUCKET_COUNT = 30;
		var TASK_FIELDS = [
			"rack",
			"hostname",
			"taskType",
			"taskId",
			"taskStatus",
			"startTime",
			"endTime",
			"jobCounters"
		];

		//var TASK_STATUS = ["NEW", "SCHEDULED", "RUNNING", "SUCCEEDED", "FAILED", "KILL_WAIT", "KILLED"];
		var TASK_STATUS = ["SUCCEEDED", "FAILED", "KILLED"];
		var TASK_STATUS = ["SUCCESS", "FAIL", "KILL"];
		var TASK_STATUS_COLOR = ["#00a65a", "#dd4b39", "#999"];

		/**
		 * @typedef {{}} Task
		 * @property {string} taskStatus
		 * @property {number} startTime
		 * @property {number} endTime
		 * @property {number} _bucket
		 */

		jpmApp.controller("jobTaskCtrl", function ($wrapState, $scope, PageConfig, Time, JPM) {
			$scope.site = $wrapState.param.siteId;
			$scope.jobId = $wrapState.param.jobId;

			var startTime = Number($wrapState.param.startTime);
			var endTime = Number($wrapState.param.endTime);

			PageConfig.title = "Task";
			PageConfig.subTitle = $scope.jobId;

			var timeDiff = endTime - startTime;
			var timeDes = Math.ceil(timeDiff / BUCKET_COUNT);
			console.log(">>>", timeDiff, timeDes);

			$scope.bucketCategory = [];
			for(var i = 0 ; i < BUCKET_COUNT ; i += 1) {
				$scope.bucketCategory.push(Time.diffStr(i * timeDes) + "\n~\n" + Time.diffStr((i + 1) * timeDes));
			}

			// ==========================================================================
			// =                               Fetch Task                               =
			// ==========================================================================
			$scope.list = JPM.list("TaskExecutionService", {site: $scope.site, jobId: $scope.jobId}, startTime, endTime, TASK_FIELDS, 1000000);
			$scope.list._promise.then(function () {
				console.log(">>>", $scope.list);
				// ======================= Bucket Distribution ======================
				$.each($scope.list,
					/**
					 * @param {number} i
					 * @param {Task} task
					 */
					function (i, task) {
						task._bucket = Math.floor((task.startTime - startTime + (task.endTime - task.startTime) / 2) / timeDes);
					});


				// ======================== Status Statistic ========================
				$scope.statusOption = {
					xAxis: {
						axisTick: { show: true },
						scale: false
					}
				};

				var bucket_status = {};
				$.each($scope.list,
					/**
					 * @param {number} i
					 * @param {Task} task
					 */
					function (i, task) {
						var status = bucket_status[task.taskStatus] = (bucket_status[task.taskStatus] || []);
						status[task._bucket] = (status[task._bucket] || 0) + 1;
					});

				$scope.statusSeries = $.map(TASK_STATUS, function (status, index) {
					return {
						name: status,
						type: "scatter",
						itemStyle: {normal: {color: TASK_STATUS_COLOR[index]}},
						data: $.map(bucket_status[status] || [], function (count, i) {
							if(!count) return;
							var size = Math.log(1 + count) * 5;
							return [[i, 1 + index, size]];
						}),
						symbolSize: function (data) {
							return data[2];
						}
					};
				});
				$scope.statusSeries.push({
					name: "?",
					type: "line",
					data: $.map($scope.bucketCategory, function () {return 0;})
				});

				console.log(">>>", $scope.statusSeries);
				console.log(">>>", JSON.stringify( $scope.statusSeries[0].data));
			});
		});
	});
})();
