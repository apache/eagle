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
		hadoopMetricApp.controller("overviewCtrl", function ($q, $wrapState, $scope, PageConfig, METRIC) {
			var startTime = Number($wrapState.param.startTime);
			var endTime = Number($wrapState.param.endTime);
			$scope.site = $wrapState.param.siteId;
			var jobCond = {
				site: $scope.site
			};

			var METRIC_NAME = [
				"hadoop.memory.nonheapmemoryusage.used",
				"hadoop.memory.heapmemoryusage.used",
				"hadoop.hbase.master.server.averageload",
				"hadoop.hbase.master.assignmentmanger.ritcount",
				"hadoop.hbase.master.assignmentmanger.ritcountoverthreshold"
			];

			PageConfig.title = 'Overview';
			$scope.commonOption = {};
			$scope.metricList = {};

			// Mock series data
			function mockMetric(name, option, count) {
				count = count || 1;
				var now = +new Date();

				var series = [];
				for (var i = 0; i < count; i += 1) {
					var data = [];

					for (var j = 0; j < 30; j += 1) {
						data.push({x: now + j * 1000 * 60, y: Math.random() * 100});
					}
					series.push($.extend({
						name: name + '_' + i,
						type: 'line',
						data: data,
						showSymbol: false
					}, option));
				}
				return {
					title: name,
					series: series
				};
			}


			function generateHbaseMetric(name, option, limit) {
				limit = limit || 20;
				var hbaseMetric;
				var series = [];
				$scope.site = $wrapState.param.siteId;
				var jobCond = {
					site: $scope.site
				};
				var data = [];
				hbaseMetric = METRIC.hbaseMetrics(jobCond, name, limit);
				return hbaseMetric._promise.then(function () {
					data = $.map(hbaseMetric, function (metric) {
						return {
							x: metric.timestamp,
							y: metric.value[0]
						};
					});

					series.push($.extend({
						name: name,
						type: 'line',
						data: data,
						showSymbol: false
					}, option));
					return {
						title: name,
						series: series
					};
				});
			}

			$q.all([
				generateHbaseMetric(METRIC_NAME[0], {areaStyle: {normal: {}}}),
				generateHbaseMetric(METRIC_NAME[1], {areaStyle: {normal: {}}}),
				generateHbaseMetric(METRIC_NAME[2], {areaStyle: {normal: {}}}),
				generateHbaseMetric(METRIC_NAME[3], {areaStyle: {normal: {}}})
			]).then(function (res) {
				console.log(res[0]);

				$scope.metricList = [
					res[0],
					res[1],
					res[2],
					res[3],
					mockMetric('name1', {}, 2),
					mockMetric('name2', {smooth: true}, 2),
					mockMetric('name3', {areaStyle: {normal: {}}, stack: 'one'}, 2),
					mockMetric('name4', {type: 'bar', stack: 'one'}, 2),
					mockMetric('name1', {}, 3),
					mockMetric('name2', {smooth: true}, 3),
					mockMetric('name3', {areaStyle: {normal: {}}, stack: 'one'}, 3),
					mockMetric('name4', {type: 'bar', stack: 'one'}, 3)
				];
			});
		});
	});
})();
//@ sourceURL=overview.js
