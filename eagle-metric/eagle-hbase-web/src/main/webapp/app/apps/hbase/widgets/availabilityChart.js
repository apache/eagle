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
	register(function (hadoopMetricApp) {
		hadoopMetricApp.directive("hadoopMetricWidget", function () {
			return {
				restrict: 'AE',
				controller: function ($scope, $attrs, METRIC, Application, $interval, Site, $wrapState) {
					// Get site
					var site = $scope.site;
					var refreshInterval;

					if(!site) {
						$scope.list = $.map(Application.find("HADOOP_METRIC_MONITOR"), function (app) {
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
					$scope.bgColor = "yellow";

					function countHBaseRole(site, status, role, groups, filed, limit) {
						var jobCond = {
							site: site,
							status: status,
							role: role
						};
						return METRIC.aggHBaseInstance(jobCond, groups, filed, limit);
					}

					// Ref: jpm widget if need keep refresh the widget

					function refresh() {
						$.each($scope.list, function (i, site) {

							countHBaseRole(site.siteId, "active", "hmaster", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.hmasteractivenum = data.value[0];
								});
							}, function () {
								$scope.hmasteractivenum = -1;
							});
							countHBaseRole(site.siteId, "standby", "hmaster", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.hmasterstandbynum = data.value[0];
								});
							}, function () {
								$scope.hmasterstandbynum = -1;
							});
							countHBaseRole(site.siteId, "live", "regionserver", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.regionserverhealtynum = data.value[0];
								});
							}, function () {
								$scope.regionserverhealtynum = -1;
							});
							countHBaseRole(site.siteId, "dead", "regionserver", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.regionserverunhealtynum = data.value[0];
								});
							}, function () {
								$scope.regionserverunhealtynum = -1;
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
				        '<div ng-show="hmasteractivenum!==-1 && hmasterstandbynum!==-1" class="hadoopMetric-widget-detail">' +
					        '<a ui-sref="HBaseMetric({siteId: site.siteId})">' +
				            '<span>{{hmasteractivenum+hmasterstandbynum}}</span> Masters (' +
				            '<span>{{hmasteractivenum || 0}}</span> Active / ' +
				            '<span>{{hmasterstandbynum || 0}}</span> Standby)' +
					        '</a>' +
				        '</div>' +
				        '<div ng-show="hmasteractivenum===-1 || hmasterstandbynum===-1" class="hadoopMetric-widget-detail">' +
				        '<span>N/A</span> Masters (' +
				        '<span>N/A</span> Active / ' +
				        '<span>N/A</span> Standby)' +
				        '</div>' +
				        '<div ng-show="regionserverhealtynum!==-1 && regionserverunhealtynum!==-1" class="hadoopMetric-widget-detail">' +
				            '<a ui-sref="regionList({siteId: site.siteId})">' +
				            '<span>{{regionserverhealtynum+regionserverunhealtynum}}</span> RegionServers (' +
				            '<span>{{regionserverhealtynum || 0}}</span> Healthy / ' +
				            '<span>{{regionserverunhealtynum || 0}}</span> Unhealthy)' +
				            '</a>' +
				        '</div>' +
				        '<div ng-show="regionserverhealtynum===-1 || regionserverunhealtynum===-1" class="hadoopMetric-widget-detail">' +
				        '<span>N/A</span> RegionServers (' +
				        '<span>N/A</span> Healthy / ' +
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
					$("<div hadoop-metric-widget data-type='" + serviceType + "'>")
				);
			};
		}
		hadoopMetricApp.widget("availabilityHBaseChart", withType('HBase'), true);
	});
})();
