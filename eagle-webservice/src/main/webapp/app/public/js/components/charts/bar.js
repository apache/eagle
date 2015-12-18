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

eagleComponents.directive('barChart', function($compile) {
	'use strict';

	return {
		restrict : 'AE',
		scope : {
			title : "@",
			data : "=",

			height : "=?height"
		},
		controller : function(barCharts, $scope, $element, $attrs) {
			$scope.height = $scope.height || 200;

			var _chart = barCharts($scope);

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

eagleComponents.service('barCharts', function() {
	'use strict';

	/*$(window).resize(function() {
	 });
	 $("body").on("collapsed.pushMenu", function() {
	 });
	 $("body").on("expanded.pushMenu", function() {
	 });*/

	var charts = function($scope) {
		return {
			gen : function(ele, series, config) {
				// ======================= Initialization =======================
				ele = $(ele);

				series = series || [];
				config = config || {};

				// ========================== ToolTips ==========================
				var $tooltip = $("<div>").css({
					display: "inline-block",
					background: "rgba(0,0,0,0.7)",
					padding: "3px 5px",
					color: "#FFFFFF",
					position: "fixed",
					"z-index": 3,
					"border-radius": "3px",
					"font-size": "12px",
				}).appendTo("body");

				// ======================= Set Up D3 View =======================
				var margin = {
					top : 20,
					right : 20,
					bottom : 50,
					left : 40
				}, width = ele.innerWidth() - margin.left - margin.right, height = config.height - margin.top - margin.bottom;

				var x0 = d3.scale.ordinal().rangeRoundBands([0, width], 0.1);
				var x1 = d3.scale.ordinal();
				var y = d3.scale.linear().range([height, 0]);

				var color = d3.scale.ordinal().range(["#7cb5ec", "#f7a35c", "#90ee7e", "#7798BF", "#aaeeee"]);

				var xAxis = d3.svg.axis().scale(x0).orient("bottom");
				var yAxis = d3.svg.axis().scale(y).orient("left").tickFormat(d3.format("0.2f"));

				var cntr = d3.select(ele[0]).append("svg").attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom);
				var svg = cntr.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

				// =========================== Render ===========================
				function _render() {
					// ======== Parse Data ========
					var _series = typeof series === "string" ? $scope.data : series;
					if(!_series) return;

					var _data = [];
					// > Detect category
					var _categoryList = [];
					_series = $.grep(_series, function(unit) {
						if(unit.type === "category") _categoryList = unit.data;
						return !unit.type;
					});

					// > Keys
					var _keys = $.map(_series, function(unit) {
						return unit.name;
					});

					// > Merge values
					var _maxLen = 0;
					$.each(_series, function(i, unit) {
						_maxLen = Math.max(_maxLen, unit.data.length);
					});
					function _fillValue(id) {
						$.each(_keys, function(j, key) {
							_data[id][key] = _series[j].data[id] || 0;
						});
					}
					for(var i = 0 ; i < _maxLen ; i += 1) {
						_data[i] = {};

						// Category
						if(_categoryList[i]) {
							_data[i]._category = _categoryList[i];
						} else {
							_data[i]._category = "CAT" + i;
						}

						// Value
						_fillValue(i);
					}

					// ====== Convert Format ======
					var ageNames = d3.keys(_data[0]).filter(function(key) {
						return key !== "_category";
					});

					_data.forEach(function(d) {
						d.items = ageNames.map(function(name) {
							return {
								name : name,
								value : +d[name]
							};
						});
					});

					x0.domain(_data.map(function(d) {
						return d._category;
					}));
					x1.domain(ageNames).rangeRoundBands([0, x0.rangeBand()]);
					y.domain([0, d3.max(_data, function(d) {
						return d3.max(d.items, function(d) {
							return d.value;
						});
					})]);

					// Axis
					svg.append("g").attr("class", "y axis").call(yAxis).append("text").attr("transform", "rotate(-90)").attr("y", 6).attr("dy", ".71em").style("text-anchor", "end");
					svg.append("g").attr("class", "x axis").attr("transform", "translate(0," + height + ")").call(xAxis)
					.selectAll("text")
						.attr("transform", "rotate(15)")
						.attr("x", -x1.rangeBand() * 0.3)
						.style("text-anchor", "start");

					// Bar
					var barGroup = svg.selectAll(".barGroup").data(_data).enter().append("g").attr("class", "g").attr("transform", function(d) {
						return "translate(" + x0(d._category) + ",0)";
					});

					barGroup.selectAll("rect").data(function(d) {
						return d.items;
					}).enter().append("rect").attr("width", x1.rangeBand()).attr("x", function(d) {
						return x1(d.name);
					}).attr("y", function(d) {
						return y(d.value);
					}).attr("height", function(d) {
						return height - y(d.value);
					}).style("fill", function(d) {
						return color(d.name);
					});

					// Legend
					var legend = svg.selectAll(".legend").data(ageNames.slice().reverse()).enter().append("g").attr("class", "legend").attr("transform", function(d, i) {
						return "translate(0," + (30 + i * 20) + ")";
					});

					legend.append("rect").attr("x", width - 18).attr("width", 18).attr("height", 18).style("fill", color);
					legend.append("text").attr("x", width - 24).attr("y", 9).attr("dy", ".35em").style("text-anchor", "end").text(function(d) {
						return d;
					});

					// Tool tip
					var OFFSET_X = 15;
					var OFFSET_Y = 20;
					var OFFSET_DES = 20;

					var _tooltipId;
					var _tooltipPrev;
					var _xCells = $.map(_data, function(d) {
						return [[x0(d._category), x0(d._category) + x0.rangeBand(), d]];
					});

					cntr.on("mousemove", function () {
						var mouseX = d3.mouse(this.parentNode)[0] - margin.left;
						var d;
						for(var i = 0 ; i < _xCells.length ; i += 1) {
							if(_xCells[i][0] <= mouseX && mouseX <= _xCells[i][1]) {
								d= _xCells[i][2];
								break;
							}
						}

						if(!d && _tooltipPrev) {
							_tooltipId = setTimeout(function() {
								$tooltip.fadeOut('fast');
							}, _xCells[0] && mouseX < _xCells[0][0] ? 100 : 500);
						} else if(d) {
							if(_tooltipPrev !== d) {
								clearTimeout(_tooltipId);

								$tooltip.empty()
								.stop().fadeIn('fast');

								var $cntr = $("<div>").appendTo($tooltip);
								$("<span>").css("display", "block").text(d._category)
								.appendTo($cntr);
								$.each(d.items, function(i, item) {
									$("<span>").css("display", "block")
									.append($("<span>").html("\u25CF").css("color", color(item.name))).append(" ")
									.append(item.name)
									.append(": ")
									.append($("<b>").text(item.value))
									.appendTo($cntr);
								});
							}

							// Position
							var _x = event.pageX + OFFSET_X;
							var _y = event.pageY + OFFSET_Y;
							var _width = $tooltip.outerWidth();
							var _height = $tooltip.outerHeight();
							var _winWidth = $(window).width();
							var _winHeight = $(window).height();
							var _winLeft = $(window).scrollLeft();
							var _winTop = $(window).scrollTop();

							if(_x + _width + OFFSET_DES - _winLeft > _winWidth) {
								_x = event.pageX - _width - OFFSET_X;
							}
							if(_y + _height + OFFSET_DES - _winTop > _winHeight) {
								_y = event.pageY - _height - OFFSET_Y;
							}

							$tooltip.offset({
								left: _x,
								top: _y
							});
						}
						_tooltipPrev = d;
					});
					cntr.on("mouseleave", function () {
						_tooltipId = setTimeout(function() {
							$tooltip.fadeOut('fast');
						}, 100);
					});
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
					cntr.remove();
					$tooltip.remove();
				});
			},
		};
	};

	return charts;
});
