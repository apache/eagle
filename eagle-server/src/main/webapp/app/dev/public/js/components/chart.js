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

	eagleComponents.service('Chart', function () {
		return {
			color: [ "#0073b7", "#dd4b39", "#00a65a", "#f39c12", "#605ca8", "#001F3F", "#39CCCC", "#D81B60", "#3c8dbc", "#f56954", "#00c0ef", "#3D9970", "#FF851B"  , "#01FF70", "#F012BE"],
			//color: ['#4285f4', '#c23531','#2f4554', '#61a0a8', '#d48265', '#91c7ae','#749f83',  '#ca8622', '#bda29a','#6e7074', '#546570', '#c4ccd3'],
			charts: {}
		};
	});

	/**
	 * @param {{}?} chart Set chart reference
	 * @param {function} chart.refresh Refresh current chart
	 */
	eagleComponents.directive('chart', function(Chart) {
		var charts = Chart.charts;

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
				categoryFunc: "=?categoryFunc",
				xTitle: "@?xTitle",
				yTitle: "@?yTitle",

				option: "=?option",

				click: "=?ngClick",

				chart: "@?chart"
			},
			controller: function ($scope, $element, $attrs, Time) {
				var i;
				var lastSeriesCount = 0;
				var lastTooltipEvent;
				var chart = echarts.init($element[0]);
				charts[chart.id] = chart;

				function wrapChart() {
					chart.refresh = function () {
						refreshChart();
					};

					chart.forceRefresh = function () {
						delete charts[chart.id];
						chart.dispose();

						chart = echarts.init($element[0]);
						charts[chart.id] = chart;
						lastSeriesCount = 0;

						refreshChart();

						wrapChart();
					};

					refreshEventHandler();
				}

				function refreshChart() {
					var maxYAxis = 0;
					var legendList = [];
					var categoryList = $scope.category ? $scope.category : [];
					var currentSeriesCount = ($scope.series || []).length;

					if (lastSeriesCount > currentSeriesCount && chart.forceRefresh) {
						console.log('Force refresh!');
						// TODO: echart series bug. Need rebuild the chart. Ref: https://github.com/ecomfe/echarts/issues/4033
						chart.forceRefresh();
						return;
					}
					lastSeriesCount = currentSeriesCount;

					var seriesList = $.map($scope.series || [], function (series, id) {
						if(id === 0 && !$scope.category) {
							//var preDate = -1;
							var first = series.data[0].x || 0;
							var last = series.data[series.data.length - 1].x || 0;
							var crossDay = (last - first) / (1000 * 60 * 60 * 24) >= 1;

							categoryList = $.map(series.data, function (point) {
								/*ivar time = new Time(point.x);
								f(preDate !== time.date()) {
									preDate = time.date();
									return Time.format(point.x, "MMM.D HH:mm");
								}*/
								if($scope.categoryFunc) {
									return $scope.categoryFunc(point.x);
								}

								return Time.format(point.x, crossDay ? "MM-DD HH:mm" : "HH:mm").replace(' ', '\n');
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
						color: Chart.color.concat(),
						title: [{text: $scope.title}],
						tooltip: {trigger: 'axis'},
						legend: [{
							data: legendList
						}],
						grid: {
							top: '30',
							left: '0',
							right: '10',
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
				function refreshEventHandler() {
					chart.on("click", function (e) {
						if($scope.click) {
							if($scope.click(e)) {
								refreshChart();
							}
						}
						chartClick = true;
					});

					chart.getZr().on('click', function () {
						if(!chartClick && $scope.click) {
							if($scope.click($.extend({
									componentType: "tooltip"
								}, lastTooltipEvent))) {
								refreshChart();
							}
						}
						chartClick = false;
					});

					chart.on('showtip', function (e) {
						lastTooltipEvent = e;
					});
				}

				// Insert chart object to parent scope
				if($attrs.chart) {
					$scope.$parent.$parent[$attrs.chart] = chart;
				}

				wrapChart();

				// Render
				refreshChart();
				$scope.$watch("series", refreshChart);
				$scope.$watch("series.length", refreshChart);

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
