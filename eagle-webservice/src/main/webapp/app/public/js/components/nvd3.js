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

eagleComponents.service('nvd3', function() {
	var nvd3 = {
		charts: [],
		colors: [
			"#7CB5EC", "#F7A35C", "#90EE7E", "#7798BF", "#AAEEEE"
		],
		format: {
			date: "YYYY-MM-DD",
			time: "HH:mm:SS"
		}
	};

	$(window).on("resize.components.nvd3", function() {
		$.each(nvd3.charts, function(i, chart) {
			chart.update();
		});
	});

	return nvd3;
});

/**
 * config:
 * 		chart:		Defined chart type: line, column, area
 * 		xTitle:		Defined x axis title.
 * 		yTitle:		Defined y axis title.
 * 		xType:		Defined x axis label type: number, decimal, date, time
 * 		yType:		Defined y axis label type
 */
eagleComponents.directive('nvd3', function(nvd3) {
	'use strict';

	return {
		restrict: 'AE',
		scope: {
			nvd3: "=",
			title: "@?title",				// title
			chart: "@?chart",				// Same as config.chart
			config: "=?config",
			watching: "@?watching"			// Default watching data(nvd3) only. true will also watching chart & config. false do not watching.
		},
		controller: function($scope, $element, $attrs, $timeout) {
			var _config, _chartType;
			var _chart;
			var _chartCntr = $element.find("> svg")[0];

			// Destroy
			function destroy() {
				var _index = $.inArray(_chart, nvd3.charts);

				// Clean events
				d3.select(_chartCntr)
					.on("touchmove",null)
					.on("mousemove",null, true)
					.on("mouseout" ,null,true)
					.on("dblclick" ,null)
					.on("click", null);

				// Clean elements
				d3.select(_chartCntr).selectAll("*").remove();
				$element.find(".nvtooltip").remove();

				// Clean chart in nvd3 pool
				nvd3.charts[_index] = null;
				_chart = null;

				return _index;
			}

			// Setup chart environment. Will clean old chart and build new chart if recall.
			function initChart() {
				_config = $.extend({}, $scope.config);
				_chartType = $scope.chart || _config.chart;

				// Clean up if already have chart
				var _preIndex = destroy();

				switch(_chartType) {
					case "line":
						_chart = nv.models.lineChart()
							.useInteractiveGuideline(true)
							.showLegend(true)
							.showYAxis(true)
							.showXAxis(true)
							.options({
								duration: 350
							});
						break;
					case "column":
						_chart = nv.models.multiBarChart()
							.groupSpacing(0.1)
							.options({
								duration: 350
							});
						break;
					case "area":
						_chart = nv.models.stackedAreaChart()
							.useInteractiveGuideline(true)
							.showLegend(true)
							.showYAxis(true)
							.showXAxis(true)
							.options({
								duration: 350
							});
						break;
				}

				// Define title
				if(_config.xTitle) _chart.xAxis.axisLabel(_config.xTitle);
				if(_config.yTitle) _chart.yAxis.axisLabel(_config.yTitle);

				// Define label type
				function _timeTickFormat(type, d) {
					if(typeof d === "string") return d;

					var _date = app.time.offset(d);
					return _date.format(nvd3.format[type]);
				};
				var tickMultiFormat = d3.time.format.multi([
					["%-I:%M%p", function(d) { return d.getMinutes(); }], // not the beginning of the hour
					["%-I%p", function(d) { return d.getHours(); }], // not midnight
					["%b %-d", function(d) { return d.getDate() != 1; }], // not the first of the month
					["%b %-d", function(d) { return d.getMonth(); }], // not Jan 1st
					["%Y", function() { return true; }]
				]);
				_chart.xScale(d3.time.scale());
				function _defineLabelType(axis, type) {
					var _axis = _chart[axis + "Axis"];
					switch(type) {
						case "decimal":
						case "decimals":
							_axis.tickFormat(d3.format('.02f'));
							break;
						case "date":
						case "time":
							_axis.tickFormat(function(d) {
								//return _timeTickFormat(type, d);
								return tickMultiFormat(new Date(d));
							});
							break;
						default:
							_axis.tickFormat(d3.format(',r'));
					}
				}

				_defineLabelType("x", _config.xType || "number");
				_defineLabelType("y", _config.yType || "decimal");

				if(_preIndex === -1) {
					nvd3.charts.push(_chart);
				} else {
					nvd3.charts[_preIndex] = _chart;
				}

				updateData();
			}

			// Update chart data
			function updateData() {
				// Copy series to prevent Angular loop watching
				var _data = $.map($scope.nvd3 || [], function(series, i) {
					var _series = $.extend(true, {}, series);
					_series.color = _series.color || nvd3.colors[i % nvd3.colors.length];
					return _series;
				});

				// Update data
				d3.select(_chartCntr)						//Select the <svg> element you want to render the chart in.
					.datum(_data)							//Populate the <svg> element with chart data...
					.call(_chart);							//Finally, render the chart!
			}

			// ================================================================
			// =                           Watching                           =
			// ================================================================
			// Ignore initial checking
			$timeout(function() {
				if ($scope.watching !== "false") {
					$scope.$watch("nvd3", function(newValue, oldValue) {
						//noinspection JSValidateTypes
						if (newValue === oldValue) return;

						updateData();
					}, true);

					// All watching mode
					if ($scope.watching === "true") {
						$scope.$watch("[chart, config]", function(newValue, oldValue) {
							//noinspection JSValidateTypes
							if (newValue === oldValue) return;

							initChart();
						}, true);
					}
				}
			});

			// ================================================================
			// =                           Start Up                           =
			// ================================================================
			initChart();

			// ================================================================
			// =                           Clean Up                           =
			// ================================================================
			$scope.$on('$destroy', function() {
				destroy();
			});
		},
		template :
		'<div>' +
			'<h3>{{title || config.title}}</h3>' +
			'<svg style="min-height: 50px;"></svg>' +
		'</div>',
		replace: true
	};
});