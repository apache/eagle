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
	function verifyTime(str, format) {
		var date = moment(str);
		if(str === date.format(format)) {
			return date;
		}
	}

	/**
	 * `register` without params will load the module which using require
	 */
	register(function (jpmApp) {
		jpmApp.controller("listCtrl", function ($element, $scope, PageConfig, Time, Entity, JPM) {
			PageConfig.title = "Job Performance Monitoring";
			PageConfig.subTitle = "list";

			// Initialization
			var endTime = Time();
			var startTime = endTime.clone().subtract(1, "day");

			$scope.refreshList = function () {
				$scope.jobList = Entity.merge($scope.jobList, JPM.list("apollo", startTime, endTime, [
					"jobID",
					"currentState",
					"user",
					"queue",
					"submissionTime",
					"startTime",
					"endTime",
					"numTotalMaps",
					"numTotalReduces",
					"runningContainers"
				]));
				$scope.jobList._then(function () {
					$.each($scope.jobList, function (i, job) {
						job.duration = Time.diff(job.startTime, job.endTime);
					});
				});
			};
			$scope.refreshList();

			$scope.getStateClass = function (state) {
				switch ((state || "").toUpperCase()) {
				case "NEW":
				case "NEW_SAVING":
				case "SUBMITTED":
				case "ACCEPTED":
					return "warning";
				case "RUNNING":
					return "info";
				case "SUCCESS":
					return "success";
				case "FINISHED":
					return "primary";
				case "FAILED":
					return "danger";
				}
				return "default";
			};

			// Time component
			$element.on("change.jpm", "#startTime", function () {
				var time = verifyTime($(this).val(), Time.FORMAT);
				if(time) {
					startTime = time;
					$scope.refreshList();
				}
			});
			$element.on("change.jpm", "#endTime", function () {
				var time = verifyTime($(this).val(), Time.FORMAT);
				if(time) {
					endTime = time;
					$scope.refreshList();
				}
			});
			setTimeout(function () {
				$("#startTime").val(startTime.format(Time.FORMAT));
				$("#endTime").val(endTime.format(Time.FORMAT));
			}, 0);

			// Clean up
			$scope.$on('$destroy', function() {
				$element.off("change.jpm");
			});
		});
	});
})();
