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
		jpmApp.controller("overviewCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			$scope.site = $wrapState.param.siteId;

			PageConfig.title = "Overview";

			var endTime = Time("day");
			var startTime = endTime.clone().subtract(2, "day");
			endTime.add(1, "day").subtract(1, "second");

			JPM.get(JPM.QUERY_MR_JOB_COUNT, {
				site: $scope.site,
				intervalInSecs: 900,
				durationBegin: Time.format(startTime),
				durationEnd: Time.format(endTime)
			}).then(
				/**
				 * @param {{}} res
				 * @param {{}} res.data
				 * @param {[]} res.data.jobCounts
				 */
				function (res) {
					var data = res.data;
					//var jobTypes = data.jobTypes;
					var jobCounts = data.jobCounts;
					console.log(">>>", jobCounts);
					var jobTypesData = {};
					$.each(jobCounts,
						/**
						 * @param i
						 * @param {{}} jobCount
						 * @param {{}} jobCount.timeBucket
						 * @param {{}} jobCount.jobCountByType
						 */
						function (i, jobCount) {
							var timestamp = jobCount.timeBucket;

							$.each(jobCount.jobCountByType, function (type, count) {
								var countList = jobTypesData[type] = jobTypesData[type] || [];
								countList[timestamp] = count;
							});
					});
					console.log(">>>", jobTypesData);
			});
		});
	});
})();
