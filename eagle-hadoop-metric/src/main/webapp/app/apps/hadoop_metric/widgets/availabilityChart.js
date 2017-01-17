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
		var COLOR_MAPPING = {
			HDFS: 'orange',
			HBase: 'yellow',
			Yarn: 'green',
		};

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
					// Get type
					$scope.type = $attrs.type;

					// Customize chart color
					$scope.bgColor = COLOR_MAPPING[$scope.type];


					// Ref: jpm widget if need keep refresh the widget

					function refresh() {
						$.each($scope.list, function (i, site) {
							var hbaseservers = METRIC.hbasehostStatus({site: site.siteName});
							hbaseservers._promise.then(function (res) {
								var hmasternum = 0;
								var hmasteractivenum = 0;
								var regionserverHealthynum = 0;
								var regionservertotal = 0;
								$.each(res, function (i, server) {
									var role = server.tags.role;
									var status = server.status;
									if (role === "hmaster") {
										hmasternum++;
										if (status === "active") {
											hmasteractivenum++;
										}
									} else if (role === "regionserver") {
										regionservertotal++;
										if (status === "live") {
											regionserverHealthynum++;
										}
									}
								});
								$scope.hbaseinfo = {
									hmasternum: hmasternum,
									hmasteractivenum: hmasteractivenum,
									regionserverHealthynum: regionserverHealthynum,
									regionservertotal: regionservertotal
								};
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
				        '<div class="hadoopMetric-widget-detail">' +
					        '<a ui-sref="HadoopMetric({siteId: site.siteName})">' +
				            '<span>{{hbaseinfo.hmasternum}}</span> Masters (' +
				            '<span>{{hbaseinfo.hmasteractivenum}}</span> Active / ' +
				            '<span>{{hbaseinfo.hmasternum - hbaseinfo.hmasteractivenum}}</span> Standby)' +
					        '</a>' +
				        '</div>' +
				        '<div class="hadoopMetric-widget-detail">' +
				            '<a ui-sref="regionList({siteId: site.siteName})">' +
				            '<span>{{hbaseinfo.regionservertotal}}</span> RegionServers (' +
				            '<span>{{hbaseinfo.regionserverHealthynum}}</span> Healthy / ' +
				            '<span>{{hbaseinfo.regionservertotal - hbaseinfo.regionserverHealthynum}}</span> Unhealthy)' +
				            '</a>' +
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
//# sourceURL=availabilityChart.js
