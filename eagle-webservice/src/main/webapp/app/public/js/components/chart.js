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

	var eagleComponents = angular.module('eagle.components');


	eagleComponents.directive('chart', function($compile) {
		var charts = {};

		function chartResize() {
			setTimeout(function () {
				$.each(charts, function (id, chart) {
					chart.resize();
				});
			}, 310);
		}

		$(window).resize(chartResize);
		$("body").on("expanded.pushMenu collapsed.pushMenu", chartResize);


		return {
			restrict: 'AE',
			scope: {
				title: "@",
				series: "="
			},
			controller: function ($scope, $element, $attrs, Time) {
				var i;
				var chart = echarts.init($element[0]);
				charts[chart.id] = chart;

				function refreshChart() {
					console.log("!!!!! Refresh chart", $scope.series);

					var maxYAxis = 0;
					var legendList = [];
					var categoryList = [];
					var seriesList = $.map($scope.series || [], function (series, id) {
						if(id === 0) {
							categoryList = $.map(series.data, function (point) {
								return Time.format(point.x, "YY/MM/DD HH:mm");
							});
						}

						legendList.push(series.name);
						if(series.yAxisIndex) maxYAxis = Math.max(series.yAxisIndex, maxYAxis);

						return $.extend({}, series, {
							data: $.map(series.data, function (point) {
								return point.y;
							})
						});
					});

					var yAxis = [];
					for(i = 0 ; i <= maxYAxis ; i += 1) {
						yAxis.push({type: "value"});
					}

					var option = {
						title: [{text: $scope.title}],
						tooltip: {trigger: 'axis'},
						legend: [{
							data: legendList
						}],
						grid: {
							top: '30',
							left: '0',
							right: '0',
							bottom: '0',
							containLabel: true
						},
						xAxis: {
							type: 'category',
							data: categoryList,
							axisTick: { show: false }
						},
						yAxis: yAxis,
						series: seriesList
					};

					chart.setOption(option);
				}

				refreshChart();
				$scope.$watch("series", refreshChart);

				$scope.$on('$destroy', function() {
					delete charts[chart.id];
				});
			},
			template: '<div>Loading...</div>',
			replace: true
		};
	});
})();
