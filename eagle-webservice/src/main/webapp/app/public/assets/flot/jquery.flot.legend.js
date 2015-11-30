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

(function($) {
	"use strict";

	var defaultOptions = {
		legend : {
			clickSelect: false,
			autoAdjust: true,
		}
	};

	var init = function(plot) {
		var $cntr = $(plot.getPlaceholder());
		var _statusList = [];
		var _init = false;

		function getIndex(series) {
			var _index;
			$.each(plot.getData(), function(index, _series) {
				if(_series === series) {
					_index = index;
					return false;
				}
			});
			return _index;
		}

		function isShow(index) {
			var _data = plot.getData();
			if(!_data) return null;

			var _series = _data[index];
			var _oriSeries = _statusList[index];

			return	(_series.bars.show && _oriSeries.bars) || 
					(_series.lines.show && _oriSeries.lines) || 
					(_series.points.show && _oriSeries.points);
		}
		function show(index, display) {
			var _data = plot.getData();
			var series = _data[index];
			series.lines.show = display && _statusList[index].lines;
			series.bars.show = display && _statusList[index].bars;
			series.points.show = display && _statusList[index].points;
		}

		$cntr.off("click.legend").on("click.legend", ".legend table tbody tr", function() {
			var _my = $(this);
			var _index = _my.index();
			var _data = plot.getData();

			var _myShow = isShow(_index);

			if(plot.getOptions().legend.clickSelect === true) {
				var _showAll = _myShow && $.grep(_data, function(_series, i) {
					return !isShow(i);
				}).length !== 0;
				$.each(_data, function(i, _series) {
					show(i, _showAll);
				});
				show(_index, true);
			} else {
				show(_index, !_myShow);
			}
			plot.setData(_data);

			plot.setupGrid();
			plot.draw();
		});

		plot.hooks.bindEvents.push(function(plot, eventHolder) {
			var _data = plot.getData();
			$.each(_data, function(i, series) {
				_statusList[i] = {
					bars: series.bars.show,
					lines: series.lines.show,
					points: series.points.show,
				};
			});

			_init = true;
		});

		plot.hooks.processDatapoints.push(function(plot, series, dataPoints) {
			var _option = plot.getOptions();
			// Auto adjust view size
			if(_init && (_option.series && !_option.series.stack) && _option.legend.autoAdjust === true) {
				var index = getIndex(series);

				if(isShow(index) === false) {
					dataPoints.points = [];
				}
			}
		});
	};

	// define Flot plugin
	$.plot.plugins.push({
		init : init,
		options : defaultOptions,
		name : 'legend',
		version : '0.8.3'
	});
})(jQuery);
