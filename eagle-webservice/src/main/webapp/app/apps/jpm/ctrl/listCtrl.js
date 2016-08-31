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
		var JOB_STATES = ["NEW", "NEW_SAVING", "SUBMITTED", "ACCEPTED", "RUNNING", "FINISHED", "SUCCEEDED", "FAILED", "KILLED"];

		jpmApp.controller("listCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			function verifyTime(str, format) {
				var date = Time(str);
				if(str === Time.format(date, format)) {
					return date;
				}
			}

			// Initialization
			var startTime = verifyTime($wrapState.param.startTime, Time.FORMAT);
			var endTime = verifyTime($wrapState.param.endTime, Time.FORMAT);
			if(!startTime || !endTime) {
				endTime = Time();
				startTime = endTime.clone().subtract(2, "hour");

				$wrapState.go(".", {
					startTime: Time.format(startTime),
					endTime: Time.format(endTime)
				}, {location: "replace"});

				return;
			}

			PageConfig.title = "YARN Jobs";
			PageConfig.subTitle = "list";
			$scope.getStateClass = JPM.getStateClass;
			$scope.tableScope = {};

			$scope.startTimeInput = Time.format(startTime);
			$scope.endTimeInput = Time.format(endTime);

			$scope.site = $wrapState.param.siteId;
			$scope.searchPathList = [["tags", "jobId"], ["tags", "user"], ["tags", "queue"], ["currentState"]];

			$scope.fillSearch = function (key) {
				$("#jobList").find(".search-box input").val(key).trigger('input');
			};

			$scope.refreshList = function () {
				/**
				 * @namespace
				 * @property {[]} jobList
				 * @property {{}} jobList.tags						unique job key
				 * @property {string} jobList.tags.jobId			Job Id
				 * @property {string} jobList.tags.user				Submit user
				 * @property {string} jobList.tags.queue			Queue
				 * @property {string} jobList.currentState			Job state
				 * @property {string} jobList.submissionTime		Submission time
				 * @property {string} jobList.startTime				Start time
				 * @property {string} jobList.endTime				End time
				 * @property {string} jobList.numTotalMaps			Maps count
				 * @property {string} jobList.numTotalReduces		Reduce count
				 * @property {string} jobList.runningContainers		Running container count
				 */

				$scope.jobList = Entity.merge($scope.jobList, JPM.jobList({site: $scope.site}, startTime, endTime, [
					"jobId",
					"jobDefId",
					"jobName",
					"jobExecId",
					"currentState",
					"user",
					"queue",
					"submissionTime",
					"startTime",
					"endTime",
					"numTotalMaps",
					"numTotalReduces",
					"runningContainers"
				], 100000));
				$scope.jobStateList = [];

				$scope.jobList._then(function () {
					var now = Time();
					var jobStates = {};
					$.each($scope.jobList, function (i, job) {
						jobStates[job.currentState] = (jobStates[job.currentState] || 0) + 1;
						job.duration = Time.diff(job.startTime, job.endTime || now);
					});

					$scope.jobStateList = $.map(JOB_STATES, function (state) {
						var value = jobStates[state];
						delete  jobStates[state];
						if(!value) return null;
						return {
							key: state,
							value: value
						};
					});

					$.each(jobStates, function (key, value) {
						$scope.jobStateList.push({
							key: key,
							value: value
						});
					});
				});
			};

			// Time component
			$element.on("change.jpm", "#startTime", function () {
				var time = verifyTime($(this).val(), Time.FORMAT);
				if(time) {
					$(this).trigger("input");
				}
			});
			$element.on("change.jpm", "#endTime", function () {
				var time = verifyTime($(this).val(), Time.FORMAT);
				if(time) {
					$(this).trigger("input");
				}
			});

			$scope.checkDateRange = function () {
				var startTime = verifyTime($scope.startTimeInput, Time.FORMAT);
				var endTime = verifyTime($scope.endTimeInput, Time.FORMAT);
				return startTime && endTime;
			};
			$scope.changeDateRange = function () {
				startTime = verifyTime($scope.startTimeInput, Time.FORMAT);
				endTime = verifyTime($scope.endTimeInput, Time.FORMAT);

				$scope.refreshList();
			};

			// Load list
			$scope.refreshList();

			// Clean up
			$scope.$on('$destroy', function() {
				$element.off("change.jpm");
			});
		});
	});
})();
