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
		jpmApp.directive("jpmWidget", function () {
			return {
				restrict: 'AE',
				controller: function($scope, $interval, Application, JPM, Time) {
					var site = $scope.site;
					var refreshInterval;

					if(!site) {
						$scope.list = $.map(Application.find("JPM_WEB_APP"), function (app) {
							return {
								siteId: app.site.siteId,
								siteName: app.site.siteName || app.site.siteId,
								count: -1
							};
						});
					} else {
						$scope.list = [{
							siteId: site.siteId,
							siteName: site.siteName || site.siteId,
							count: -1
						}];
					}

					function refresh() {
						$.each($scope.list, function (i, site) {
							var query = JPM.getQuery("GROUPS", site.siteId);
							var url = common.template(query, {
								query: "RunningJobExecutionService",
								condition: '@site="' + site.siteId + '" AND @internalState="RUNNING"',
								groups: "@site",
								field: "count",
								order: "",
								top: "",
								limit: 100000,
								startTime: Time.format(Time().subtract(3, "d")),
								endTime: Time.format(Time().add(1, "d"))
							});
							JPM.get(url).then(function (res) {
								site.count = common.getValueByPath(res, ["data", "obj", 0, "value", 0]);
							});
						});
					}

					refresh();
					refreshInterval = $interval(refresh, 30 * 1000);

					$scope.$on('$destroy', function() {
						$interval.cancel(refreshInterval);
					});
				},
				template:
				'<div class="small-box bg-aqua jpm">' +
					'<div class="inner">' +
						'<h3>JPM</h3>' +
						'<p ng-repeat="site in list track by $index">' +
							'<a ui-sref="jpmList({siteId: site.siteId})">' +
								'<strong>{{site.siteName}}</strong>: ' +
								'<span ng-show="site.count === -1" class="fa fa-refresh fa-spin no-animate"></span>' +
								'<span ng-show="site.count !== -1">{{site.count}}</span> Running Jobs' +
							'</a>' +
						'</p>' +
					'</div>' +
					'<div class="icon">' +
						'<i class="fa fa-taxi"></i>' +
					'</div>' +
				'</div>',
				replace: true
			};
		});

		/**
		 * Customize the widget content. Return false will prevent auto compile.
		 * @param {{}} $element
		 * @param {function} $element.append
		 */
		function registerWidget($element) {
			$element.append(
				$("<div jpm-widget data-site='site'>")
			);
		}

		jpmApp.widget("jobStatistic", registerWidget);
		jpmApp.widget("jobStatistic", registerWidget, true);
	});
})();
