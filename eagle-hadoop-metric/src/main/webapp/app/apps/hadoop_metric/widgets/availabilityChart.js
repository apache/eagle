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

		var widgetChartOption = {
			color: ['#FFFFFF'],
			grid: {
				top: 0,
				right: 0,
				bottom: 0,
				left: 0,
				containLabel: false,
			},
			xAxis: {
				axisLine: {show: false},
				axisLabel: {show: false},
			},
			yAxis: [{
				axisLine: {show: false},
				axisLabel: {show: false},
				axisTick: {show: false},
				splitLine: {show: false},
			}],
		};

		hadoopMetricApp.directive("hadoopMetricWidget", function () {
			return {
				restrict: 'AE',
				controller: function($scope, $attrs) {
					// Get site
					var site = $scope.site;

					// Get type
					$scope.type = $attrs.type;

					// Customize chart color
					$scope.bgColor = COLOR_MAPPING[$scope.type];

					$scope.chartOption = widgetChartOption;

					// Mock fetch data
					var now = +new Date();
					var data = [];
					for(var j = 0 ; j < 30 ; j += 1) {
						data.push({x: now + j * 1000 * 60, y: Math.random() * 100});
					}
					$scope.series = [{
						name: '',
						type: 'line',
						data: data,
						showSymbol: false,
					}];

					// Ref: jpm widget if need keep refresh the widget
				},
				template:
				'<div class="hadoopMetric-widget bg-{{bgColor}}">' +
					'<h3>{{type}}</h3>' +
					'<div chart class="hadoopMetric-chart-container" series="series" option="chartOption"></div>' +
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

		hadoopMetricApp.widget("availabilityHDFSChart", withType('HDFS'), true);
		hadoopMetricApp.widget("availabilityHBaseChart", withType('HBase'), true);
		hadoopMetricApp.widget("availabilityYarnChart", withType('Yarn'), true);
	});
})();
