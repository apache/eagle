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
		jpmApp.controller("detailCtrl", function ($wrapState, $element, $scope, PageConfig, Time, Entity, JPM) {
			PageConfig.title = "Job";
			$scope.getStateClass = JPM.getStateClass;

			$scope.site = $wrapState.param.siteId;
			$scope.jobId = $wrapState.param.jobId;

			JPM.list({
				jobId: $scope.jobId,
				site: $scope.site
			})._promise.then(function (res) {
				$scope.job = common.getValueByPath(res, "data.obj.0");
				PageConfig.subTitle = $scope.jobId;
				console.log(">>>", $scope.job);

				if(!$scope.job) {
					$.dialog({
						title: "OPS!",
						content: "Job not found!"
					}, function () {
						$wrapState.go("jpmList", {siteId: $scope.site});
					});
				}
			});
		});
	});
})();
