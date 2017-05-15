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

(function() {
	'use strict';

	var eagleControllers = angular.module('eagleControllers');

	// ======================================================================================
	// =                                        Alert                                       =
	// ======================================================================================
	eagleControllers.controller('metricPreviewCtrl', function ($scope, $wrapState, PageConfig, CompatibleEntity, Time, Site) {
		PageConfig.title = "Metric Preview";

		$scope.series = [];
		$scope.loading = false;

		$scope.site = $wrapState.param.site || Site.list[0].siteId;
		$scope.metricName = $wrapState.param.metric;
		$scope.groups = $wrapState.param.groups;
		$scope.fields = $wrapState.param.fields;

		$scope.commonOption = {};

		$scope.metricList = [$scope.metricName];
		CompatibleEntity.groups({
			query: 'MetricSchemaService',
			groups: 'metricName',
			fields: 'count',
			limit: 9999,
		})._promise.then(function (res) {
			$scope.metricList = $.map(res.data.obj, function (obj) {
				return obj.key[0];
			}).sort();
		});

		$scope.loadSeries = function() {
			$scope.series = CompatibleEntity.timeSeries({
				condition: { site: $scope.site },
				groups: $scope.groups,
				fields: $scope.fields,
				top: 10,
				metric: $scope.metricName,
				limit: 100000,
			});

			// Update URL
			$wrapState.go(".", {
				site: $scope.site,
				metric: $scope.metricName,
				groups: $scope.groups,
				fields: $scope.fields,
			});
		};

		if ($scope.metricName && $scope.groups && $scope.fields) {
			$scope.loadSeries();
		}

		Time.onReload($scope.loadSeries, $scope);
	});
}());
