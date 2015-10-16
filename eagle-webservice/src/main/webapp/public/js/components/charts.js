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
'use strict';

eagleComponents.directive('chart', function($compile) {
	return {
		restrict : 'E',
		scope: {
			title: "@",
			data: "=",
		},
		controller: function(charts, $scope, $element) {
			var _charts = charts($scope);

			_charts.gen($element.find(".chart-body"), [{
				data: "data",
			}]);
		},
		template :	'<div class="chart">' +
						'<div class="chart-header">' +
							'<h3>{{title}}</h3>' +
						'</div>' +
						'<div class="chart-body">' +
						'</div>' +
					'</div>',
		replace: true
	};
});

/*
 * config:
 * 		type:	"line"(default), "area". Chart type is the default type of all series. Will be replaced by each series configure.
 */
eagleComponents.service('charts', function() {
	/*
	 * Destroy chart
	 */
	function _destroy(ele) {
		$(ele).each(function() {
			var _plot = $(this).data("plot");
			_plot.shutdown();
			_plot.destroy();
		});
	};

	var charts = function($scope) {
		var _id = 0;

		return {
			/*
			 * Generate chart
			 */
			gen: function(ele, series, config) {
				// Initialization
				ele = $(ele);

				series = series || [];
				config = config || {};

				var _listenList = [];

				var _config = {
					grid: {
						hoverable: true,
						clickable: true,
						borderWidth: 0,
					},
					type: "line",
					colors: [
						"#7cb5ec", "#f7a35c", "#90ee7e", "#7798BF", "#aaeeee",
					],
					series: {
						shadowSize: 0,
					},
					crosshair: {
						color: "#3c8dbc"
					},
					tooltip: {
						id: "chartTooltip",
					},
					xaxis: {
						mode: "time",
					},
					yaxis: {
						min: 0,
					},
					legend: {},
				};

				$.extend(_config, config);

				// Series process
				// > Series type
				$.each(series, function(i, _series) {
					// Data source
					if(typeof _series.data === "string") {
						_listenList.push(_series.data);
						_series._key = _series.data;
						_series.data = [];
					}

					// Chart type
					switch((_series.type || _config.type || "").toLowerCase()) {
					case "area":
						common.setValueByPath(_series, "lines.show", true);
						common.setValueByPath(_series, "lines.fill", true);
					default:
						common.setValueByPath(_series, "lines.show", true);
					}

					if(_config.xaxis.mode === "time" && $.isArray(_series.data)) {
						$.each(_series.data, function(i, unit) {
							unit[0] += app.time.UTC_OFFSET * 1000 * 60;
						});
					}
				});

				// > Data source
				function _updateSeriesSource() {
					$.each(series, function(i, _series) {
						if(typeof _series._key === "string" && $scope[_series._key]) {
							_series.data = $scope[_series._key];
							delete _series._key;
						}
					});
				}

				// Chart process
				_config.type = _config.type.toLowerCase();
				if(_config.type === "line" || _config.type === "area") {
					common.setValueByPath(_config, "crosshair.mode", "x");
				}

				// Draw charts
				function _drawChart() {
					_updateSeriesSource();

					ele.each(function() {
						var _my = $(this);
						var _plot = _my.data("plot");
						if(!_plot) {
							var _plot = $.plot(this, series, _config);
							_plot._id = ++_id;
							_my.data("plot", _plot);
						} else {
							_plot.setData(series);
							_plot.setupGrid();
							_plot.draw();
						}
					});
				}

				// Watch
				$.each(_listenList, function(i, item) {
					$scope.$watchCollection(item, function() {
						_drawChart();
					});
				});

				// Destroy
				$scope.$on('$destroy', function() {
					_destroy(ele);
				});
			},

			destroy: _destroy,
		};
	};

	charts.destroy = _destroy;

	return charts;
});
