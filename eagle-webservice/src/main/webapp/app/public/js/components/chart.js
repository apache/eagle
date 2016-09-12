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
		var charts = window.charts = {};

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
				title: "@?title",
				series: "=",
				category: "=?category",
				xTitle: "@?xTitle",
				yTitle: "@?yTitle",

				option: "=?option",

				click: "=?ngClick",

				chart: "@?chart"
			},
			controller: function ($scope, $element, $attrs, Time) {
				var i;
				var lastTooltipEvent;
				var chart = echarts.init($element[0]);
				charts[chart.id] = chart;

				function refreshChart() {
					var maxYAxis = 0;
					var legendList = [];
					var categoryList = $scope.category ? $scope.category : [];

					var seriesList = $.map($scope.series || [], function (series, id) {
						if(id === 0 && !$scope.category) {
							var preDate = -1;
							categoryList = $.map(series.data, function (point) {
								var time = new Time(point.x);
								if(preDate !== time.date()) {
									preDate = time.date();
									return Time.format(point.x, "YY/MM/DD HH:mm");
								}
								return Time.format(point.x, "HH:mm");
							});
						}

						legendList.push(series.name);
						if(series.yAxisIndex) maxYAxis = Math.max(series.yAxisIndex, maxYAxis);

						return $.extend({}, series, {
							data: $scope.category ? series.data : $.map(series.data, function (point) {
								return point.y;
							})
						});
					});

					var yAxis = [];
					for(i = 0 ; i <= maxYAxis ; i += 1) {
						yAxis.push({
							name: $scope.yTitle,
							type: "value"
						});
					}

					var option = {
						color: [ "#0073b7", "#dd4b39", "#00a65a", "#f39c12", "#605ca8", "#001F3F", "#39CCCC", "#D81B60", "#3c8dbc", "#f56954", "#00c0ef", "#3D9970", "#FF851B"  , "#01FF70", "#F012BE"],
						//color: ['#4285f4', '#c23531','#2f4554', '#61a0a8', '#d48265', '#91c7ae','#749f83',  '#ca8622', '#bda29a','#6e7074', '#546570', '#c4ccd3'],
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
							name: $scope.xTitle,
							type: 'category',
							data: categoryList,
							axisTick: { show: false }
						},
						yAxis: yAxis,
						series: seriesList
					};

					if($scope.option) {
						option = common.merge(option, $scope.option);
					}

					chart.setOption(option);
				}

				// Event handle
				var chartClick = false;
				chart.on("click", function (e) {
					if($scope.click) {
						$scope.click(e);
					}
					chartClick = true;
				});

				chart.getZr().on('click', function () {
					if(!chartClick && $scope.click) {
						$scope.click($.extend({
							componentType: "tooltip"
						}, lastTooltipEvent));
					}
					chartClick = false;
				});

				chart.on('showtip', function (e) {
					lastTooltipEvent = e;
				});

				// Insert chart object to parent scope
				if($attrs.chart) {
					$scope.$parent.$parent[$attrs.chart] = chart;
				}

				chart.refresh = function () {
					refreshChart();
				};

				// Render
				refreshChart();
				$scope.$watch("series", refreshChart);

				$scope.$on('$destroy', function() {
					delete charts[chart.id];
					chart.dispose();

					delete $scope.$parent.$parent[$attrs.chart];
				});
			},
			template: '<div>Loading...</div>',
			replace: true
		};
	});
})();
