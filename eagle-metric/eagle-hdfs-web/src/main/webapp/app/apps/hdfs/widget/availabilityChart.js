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
	register(function (hdfsMetricApp) {
		hdfsMetricApp.directive("hdfsMetricWidget", function () {
			return {
				restrict: 'AE',
				controller: function ($scope, $attrs, HDFSMETRIC, Application, $interval) {
					// Get site
					var site = $scope.site;
					var refreshInterval;

					if(!site) {
						$scope.list = $.map(Application.find("HDFS_METRIC_WEB_APP"), function (app) {
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
					$scope.bgColor = "orange";

					function refresh() {
						$.each($scope.list, function (i, site) {

							HDFSMETRIC.countHadoopRole("HdfsServiceInstance", site.siteId, "active", "namenode", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.namenodeactivenum = data.value[0];
								});
							}, function () {
								$scope.namenodeactivenum = -1;
							});
							HDFSMETRIC.countHadoopRole("HdfsServiceInstance", site.siteId, "standby", "namenode", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.namenodestandbynum = data.value[0];
								});
							}, function () {
								$scope.namenodestandbynum = -1;
							});
							HDFSMETRIC.countHadoopRole("HdfsServiceInstance", site.siteId, "live", "datanode", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.datanodehealtynum = data.value[0];
								});
							}, function () {
								$scope.datanodehealtynum = -1;
							});
							HDFSMETRIC.countHadoopRole("HdfsServiceInstance", site.siteId, "dead", "datanode", ["site"], "count")._promise.then(function (res) {
								$.map(res, function (data) {
									$scope.datanodeunhealtynum = data.value[0];
								});
							}, function () {
								$scope.datanodeunhealtynum = -1;
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
				'<div ng-show="namenodeactivenum!==-1 && namenodestandbynum!==-1" class="hadoopMetric-widget-detail">' +
				'<a ng-show="namenodeactivenum || namenodestandbynum" ui-sref="namenodeList({siteId: site.siteId})">' +
				'<span>{{namenodeactivenum+namenodestandbynum}}</span> Namenodes (' +
				'<span>{{namenodeactivenum||0}}</span> Active / ' +
				'<span>{{namenodestandbynum||0}}</span> Standby)' +
				'</a>' +
				'<div ng-show="!namenodeactivenum && !namenodestandbynum">' +
				'<span>0</span> Namenodes (' +
				'<span>0</span> Active / ' +
				'<span>0</span> Standby)' +
				'</div>' +
				'</div>' +
				'<div ng-show="namenodeactivenum===-1 || namenodestandbynum===-1" class="hadoopMetric-widget-detail">' +
				'<span>N/A</span> Namenodes (' +
				'<span>N/A</span> Active / ' +
				'<span>N/A</span> Standby)' +
				'</div>' +
				'<div ng-show="datanodehealtynum!==-1 && datanodeunhealtynum!==-1" class="hadoopMetric-widget-detail">' +
				'<a ng-show="datanodehealtynum || datanodeunhealtynum" ui-sref="datanodeList({siteId: site.siteId})">' +
				'<span>{{datanodehealtynum+datanodeunhealtynum}}</span> Datanodes (' +
				'<span>{{datanodehealtynum||0}}</span> Healthy / ' +
				'<span>{{datanodeunhealtynum||0}}</span> Unhealthy)' +
				'</a>' +
				'<div ng-show="!datanodehealtynum && !datanodehealtynum">' +
				'<span>0</span> Datanodes (' +
				'<span>0</span> Active / ' +
				'<span>0</span> Standby)' +
				'</div>' +
				'</div>' +
				'<div ng-show="datanodehealtynum===-1 || datanodeunhealtynum===-1" class="hadoopMetric-widget-detail">' +
				'<span>N/A</span> Datanodes (' +
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
					$("<div hdfs-metric-widget data-type='" + serviceType + "'>")
				);
			}
		}

		hdfsMetricApp.widget("availabilityHdfsChart", withType('HDFS'), true);
	});
})();
