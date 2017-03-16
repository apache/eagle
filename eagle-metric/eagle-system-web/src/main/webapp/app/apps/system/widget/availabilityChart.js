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
	register(function (systemMetricApp) {
		systemMetricApp.directive("systemMetricWidget", function () {
			return {
				restrict: 'AE',
				controller: function ($scope, $attrs, SYSTEMMETRIC, Application, $interval, Site, $wrapState) {
					// Get site
					var site = $scope.site;
					var refreshInterval;

					if(!site) {
						$scope.list = $.map(Application.find("SYSTEM_METRIC_WEB_APP"), function (app) {
							return {
								siteId: app.site.siteId,
								siteName: app.site.siteName || app.site.siteId
							};
						});
					} else {
						$scope.list = [{
							siteId: site.siteId,
							siteName: site.siteName || site.siteId
						}];
					}
					// Get type
					$scope.type = $attrs.type;

					// Customize chart color
					$scope.bgColor = "yellow-active";

					function countStatus(site, status, groups, filed, limit) {
						var jobCond = {
							site: site,
							status: status
						};
						return SYSTEMMETRIC.aggSystemInstance(jobCond, groups, filed, limit);
					}

					// Ref: jpm widget if need keep refresh the widget

					function refresh() {
						$.each($scope.list, function (i, site) {
							// status count
							countStatus(site.siteId, "active", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.systemactivenum = data.value[0];
								});
							}, function () {
								$scope.systemactivenum = -1;
							});

							countStatus(site.siteId, "warning", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.systemwarningnum = data.value[0];
								});
							}, function () {
								$scope.systemwarningnum = -1;
							});

							countStatus(site.siteId, "error", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.systemerrornum = data.value[0];
								});
							}, function () {
								$scope.systemerrornum = -1;
							});
						});
					}

					refresh();
					refreshInterval = $interval(refresh, 30 * 1000);

					$scope.$on('$destroy', function () {
						$interval.cancel(refreshInterval);
					});
				},
				template:
				'<div class="small-box hadoopMetric-widget bg-{{bgColor}}">' +
				    '<div class="inner">' +
				        '<h3>{{type}}</h3>' +
				        '<div ng-show="systemactivenum!==-1 && systemwarningnum!==-1 && systemerrornum!==-1" class="hadoopMetric-widget-detail">' +
				            '<a ui-sref="serverList({siteId: site.siteId})">' +
				            '<span>{{systemactivenum+systemwarningnum+systemerrornum || 0}}</span> Node (' +
				            '<span>{{systemactivenum || 0}}</span> Healthy / ' +
				            '<span>{{systemwarningnum || 0}}</span> Warning / ' +
				            '<span>{{systemerrornum || 0}}</span> Unhealthy)' +
				            '</a>' +
				        '</div>' +
				        '<div ng-show="systemactivenum===-1 || systemwarningnum===-1 || systemerrornum===-1" class="hadoopMetric-widget-detail">' +
				        '<span>N/A</span> Node (' +
				        '<span>N/A</span> Healthy / ' +
				        '<span>N/A</span> Warning / ' +
				        '<span>N/A</span> Unhealthy)' +
				        '</div>' +
				    '</div>' +
				    '<div class="icon">' +
				        '<i class="fa fa-taxi"></i>' +
				    '</div>' +
				'</div>',
				replace: true
			};
		});

		function withType(serviceType) {
			/**
			 * Customize the widget content. Return false will prevent auto compile.
			 * @param {{}} $element
			 * @param {function} $element.append
			 */
			return function registerWidget($element) {
				$element.append(
					$("<div system-metric-widget data-type='" + serviceType + "'>")
				);
			};
		}
		systemMetricApp.widget("availabilitySystemChart", withType('System'), true);
	});
})();
