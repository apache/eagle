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

eagleComponents.directive('line3dChart', function($compile) {
	'use strict';

	return {
		restrict : 'AE',
		scope : {
			title : "@",
			data : "=",

			height : "=?height"
		},
		controller : function(line3dCharts, $scope, $element, $attrs) {
			$scope.height = $scope.height || 200;

			var _chart = line3dCharts($scope);

			var _chartBody = $element.find(".chart-body");
			_chartBody.height($scope.height);

			_chart.gen($element.find(".chart-body"), $attrs.data, {
				height : $scope.height,
			});
		},
		template : '<div class="chart">' + '<div class="chart-header">' + '<h3>{{title}}</h3>' + '</div>' + '<div class="chart-body">' + '</div>' + '</div>',
		replace : true
	};
});

eagleComponents.service('line3dCharts', function() {
	'use strict';

	var charts = function($scope) {
		return {
			gen : function(ele, series, config) {
				// ======================= Initialization =======================
				ele = $(ele);

				series = series || [];
				config = config || {};

				var _bounds = [{min:-10, max: 10},{min:-10, max: 10},{min:-10, max: 10}];
				var _scale = 1;

				// ======================= Set Up D3 View =======================
				var width = ele.innerWidth(), height = config.height;

				var color = ["#7cb5ec", "#f7a35c", "#90ee7e", "#7798BF", "#aaeeee"];

				var svg = d3.select(ele[0]).append("svg").attr("width", width).attr("height", height);

				// ========================== Function ==========================
				var yaw=0.5,pitch=0.5,drag;
				var transformPrecalc = [];

				var offsetPoint = function(point) {
					var _point = [
						+point[0] - (_bounds[0].max + _bounds[0].min) / 2,
						-point[1] + (_bounds[1].max + _bounds[1].min) / 2,
						-point[2] + (_bounds[2].max + _bounds[2].min) / 2
					];
					return [_point[0] * _scale, _point[1] * _scale, _point[2] * _scale];
				};

				var transfromPointX = function(point) {
					point = offsetPoint(point);
					return transformPrecalc[0] * point[0] + transformPrecalc[1] * point[1] + transformPrecalc[2] * point[2] + width / 2;
				};
				var transfromPointY = function(point) {
					point = offsetPoint(point);
					return transformPrecalc[3] * point[0] + transformPrecalc[4] * point[1] + transformPrecalc[5] * point[2] + height / 2;
				};
				var transfromPointZ = function(point) {
					point = offsetPoint(point);
					return transformPrecalc[6] * point[0] + transformPrecalc[7] * point[1] + transformPrecalc[8] * point[2];
				};
				var transformPoint2D = function(point) {
					var _point = [point[0], point[1], point[2]];
					return transfromPointX(_point).toFixed(10) + "," + transfromPointY(_point).toFixed(10);
				};

				var setTurtable = function(yaw, pitch, update) {
					var cosA = Math.cos(pitch);
					var sinA = Math.sin(pitch);
					var cosB = Math.cos(yaw);
					var sinB = Math.sin(yaw);
					transformPrecalc[0] = cosB;
					transformPrecalc[1] = 0;
					transformPrecalc[2] = sinB;
					transformPrecalc[3] = sinA * sinB;
					transformPrecalc[4] = cosA;
					transformPrecalc[5] = -sinA * cosB;
					transformPrecalc[6] = -sinB * cosA;
					transformPrecalc[7] = sinA;
					transformPrecalc[8] = cosA * cosB;

					if(update) _update();
				};
				setTurtable(0.4,0.4);

				// =========================== Redraw ===========================
				var _coordinateList = [];
				var _axisText = [];
				var coordinate = svg.selectAll(".axis");
				var axisText = svg.selectAll(".axis-text");
				var lineList = svg.selectAll(".line");

				function _redraw(series) {
					var _desX, _desY, _desZ, _step;

					// Bounds
					if(series) {
						_bounds = [{},{},{}];
						$.each(series, function(j, series) {
							// Points
							$.each(series.data, function(k, point) {
								for(var i = 0 ; i < 3 ; i += 1) {
									// Minimum
									if(_bounds[i].min === undefined || point[i] < _bounds[i].min) {
										_bounds[i].min = point[i];
									}
									// Maximum
									if(_bounds[i].max === undefined || _bounds[i].max < point[i]) {
										_bounds[i].max = point[i];
									}
								}
							});
						});
					}

					_desX = _bounds[0].max - _bounds[0].min;
					_desY = _bounds[1].max - _bounds[1].min;
					_desZ = _bounds[2].max - _bounds[2].min;

					// Step
					(function() {
						var _stepX = _desX / 10;
						var _stepY = _desY / 10;
						var _stepZ = _desZ / 10;

						_step = Math.min(_stepX, _stepY, _stepZ);
						_step = Math.max(_step, 0.5);
						_step = 0.5;
					})();

					// Scale
					(function() {
						var _scaleX = width / _desX;
						var _scaleY = height / _desY / 2;
						var _scaleZ = width / _desZ;
						_scale = Math.min(_scaleX, _scaleY, _scaleZ) / 2;
					})();

					// Coordinate
					// > Basic
					_coordinateList = [
						{color: "rgba(0,0,0,0.1)", data: [[0,0,-100],[0,0,100]]},
						{color: "rgba(0,0,0,0.1)", data: [[0,-100,0],[0,100,0]]},
						{color: "rgba(0,0,0,0.1)", data: [[-100,0,0],[100,0,0]]},

						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].min,_bounds[2].min],[_bounds[0].max,_bounds[1].min,_bounds[2].min]]},
						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].min,_bounds[2].min],[_bounds[0].min,_bounds[1].min,_bounds[2].max]]},

						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].min,_bounds[2].max],[_bounds[0].max,_bounds[1].min,_bounds[2].max]]},
						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].max,_bounds[1].min,_bounds[2].min],[_bounds[0].max,_bounds[1].min,_bounds[2].max]]},

						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].max,_bounds[2].min],[_bounds[0].max,_bounds[1].max,_bounds[2].min]]},
						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].max,_bounds[2].min],[_bounds[0].min,_bounds[1].max,_bounds[2].max]]},

						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].min,_bounds[2].min],[_bounds[0].min,_bounds[1].max,_bounds[2].min]]},
						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].max,_bounds[1].min,_bounds[2].min],[_bounds[0].max,_bounds[1].max,_bounds[2].min]]},
						{color: "rgba(0,0,255,0.3)", data: [[_bounds[0].min,_bounds[1].min,_bounds[2].max],[_bounds[0].min,_bounds[1].max,_bounds[2].max]]},
					];

					_axisText = [];
					function _axisPoint(point, dimension, number) {
						// Coordinate
						if(dimension === 1) {
							_coordinateList.push({
								color: "rgba(0,0,0,0.2)",
								data:[[_step/5,point[1],0],[0,point[1],0],[0,point[1],_step/5]]
							});
						} else {
							_coordinateList.push({
								color: "rgba(0,0,0,0.2)",
								data:[[point[0],-_step/8,point[2]], [point[0],_step/8,point[2]]]
							});
						}

						// Axis Text
						if(number.toFixed(0) == number + "") {
							point.text = number;
							point.dimension = dimension;
							_axisText.push(point);
						}
					}
					function _axisPoints(dimension, bound) {
						var i, _unit;
						for(i = _step ; i < bound.max + _step ; i += _step) {
							_unit = [0,0,0];
							_unit[dimension] = i;
							_axisPoint(_unit, dimension, i);
						}
						for(i = -_step ; i > bound.min - _step ; i -= _step) {
							_unit = [0,0,0];
							_unit[dimension] = i;
							_axisPoint(_unit, dimension, i);
						}
					}
					// > Steps
					_axisPoint([0,0,0],1,0);
					_axisPoints(0, _bounds[0]);
					_axisPoints(1, _bounds[1]);
					_axisPoints(2, _bounds[2]);

					// > Draw
					coordinate = coordinate.data(_coordinateList);
					coordinate.enter()
					.append("path")
						.attr("fill", "none")
						.attr("stroke", function(d) {return d.color;});
					coordinate.exit().remove();

					// Axis Text
					axisText = axisText.data(_axisText);
					axisText.enter()
						.append("text")
						.classed("noSelect", true)
						.attr("fill", "rgba(0,0,0,0.5)")
						.attr("text-anchor", function(d) {return d.dimension === 1 ? "start" : "middle";})
						.text(function(d) {return d.text;});
					axisText.transition()
						.attr("text-anchor", function(d) {return d.dimension === 1 ? "end" : "middle";})
						.text(function(d) {return d.text;});
					axisText.exit().remove();

					// Lines
					lineList = lineList.data(series || []);
					lineList.enter()
						.append("path")
							.attr("fill", "none")
							.attr("stroke", function(d) {return d.color;});
					lineList.exit().remove();

					_update();
				}

				function _update() {
					coordinate
						.attr("d", function(d) {
						var path = "";
						$.each(d.data, function(i, point) {
							path += (i === 0 ? "M" : "L") + transformPoint2D(point);
						});
						return path;
						});

					axisText
						.attr("x", function(d) {return transfromPointX(d) + (d.dimension === 1 ? -3 : 0);})
						.attr("y", function(d) {return transfromPointY(d) + (d.dimension === 1 ? 0 : -5);});

					lineList
						.attr("d", function(d, index) {
							var path = "";
							$.each(d.data, function(i, point) {
								path += (i === 0 ? "M" : "L") + transformPoint2D(point);
							});
							return path;
						});
				}


				svg.on("mousedown", function() {
					drag = [d3.mouse(this), yaw, pitch];
				}).on("mouseup", function() {
					drag = false;
				}).on("mousemove", function() {
					if (drag) {
						var mouse = d3.mouse(this);
						yaw = drag[1] - (mouse[0] - drag[0][0]) / 50;
						pitch = drag[2] + (mouse[1] - drag[0][1]) / 50;
						pitch = Math.max(-Math.PI / 2, Math.min(Math.PI / 2, pitch));
						setTurtable(yaw, pitch, true);
					}
				});

				// =========================== Render ===========================
				_redraw();

				function _render() {
					// ======== Parse Data ========
					var _series = typeof series === "string" ? $scope.data : series;
					if(!_series) return;

					// Clone
					_series = $.map(_series, function(series) {
						return {
							name: series.name,
							color: series.color,
							data: series.data
						};
					});

					// Colors
					$.each(_series, function(i, series) {
						series.color = series.color || color[i % color.length];
					});

					// Render
					_redraw(_series);
				}

				// ======================= Dynamic Detect =======================
				if(typeof series === "string") {
					$scope.$parent.$watch(series, function() {
						_render();
					}, true);
				} else {
					_render();
				}


				// ========================== Clean Up ==========================
				$scope.$on('$destroy', function() {
					svg.remove();
				});
			},
		};
	};
	return charts;
});