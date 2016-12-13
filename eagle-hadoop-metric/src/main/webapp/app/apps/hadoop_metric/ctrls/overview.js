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
		hadoopMetricApp.controller("overviewCtrl", function ($scope, PageConfig) {
			PageConfig.title = 'Overview';

			$scope.commonOption = {};

			// Mock series data
			function mockMetric(name, option, count) {
				count = count || 1;
				var now = +new Date();

				var series = [];
				for (var i = 0 ; i < count ; i += 1) {
					var data = [];

					for(var j = 0 ; j < 30 ; j += 1) {
						data.push({x: now + j * 1000 * 60, y: Math.random() * 100});
					}

					series.push($.extend({
						name: name + '_' + i,
						type: 'line',
						data: data,
						showSymbol: false,
					}, option));
				}

				return {
					title: name,
					series: series
				};
			}

			$scope.metricList = [
				mockMetric('name1', {}),
				mockMetric('name2', {smooth:true}),
				mockMetric('name3', {areaStyle: {normal: {}}}),
				mockMetric('name4', {type: 'bar'}),
				mockMetric('name1', {}, 2),
				mockMetric('name2', {smooth:true}, 2),
				mockMetric('name3', {areaStyle: {normal: {}}, stack: 'one'}, 2),
				mockMetric('name4', {type: 'bar', stack: 'one'}, 2),
				mockMetric('name1', {}, 3),
				mockMetric('name2', {smooth:true}, 3),
				mockMetric('name3', {areaStyle: {normal: {}}, stack: 'one'}, 3),
				mockMetric('name4', {type: 'bar', stack: 'one'}, 3),
			];
		});
	});
})();
