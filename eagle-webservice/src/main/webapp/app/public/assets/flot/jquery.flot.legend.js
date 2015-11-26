"use strict";

/* Flot Plugin: Tooltip
 * Author: Jinlin, Jiang
 */

(function($) {
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
		function show(index, show) {
			var _data = plot.getData();
			var series = _data[index];
			series.lines.show = show && _statusList[index].lines;
			series.bars.show = show && _statusList[index].bars;
			series.points.show = show && _statusList[index].points;
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
