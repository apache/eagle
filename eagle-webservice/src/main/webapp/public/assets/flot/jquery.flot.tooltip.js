"use strict";

/* Flot Plugin: Tooltip
 * Author: Jinlin, Jiang
 */

(function($) {
	// Common
	function getValueByPath(unit, path, defaultValue) {
		if(unit === null || unit === undefined) throw "Unit or path can't be empty!";
		if(path === "" || path === null || path === undefined) return unit;

		path = path.split(/\./);
		$.each(path, function(i, path) {
			unit = unit[path];
			if(unit == null) {
				unit = null;
				return false;
			}
		});
		if(unit === null && defaultValue !== undefined) {
			unit = defaultValue;
		}
		return unit;
	};

	var OFFSET_X = 15;
	var OFFSET_Y = 20;
	var OFFSET_DES = 20;

	var defaultOptions = {
		tooltip : {
			show: true,
			share: true,
			id: null,						// Define the tool tip id. Will share tool tip if use same id
			className: "flotToolTip",
			container: "body",
			useCSS: true,
			css: {
				display: "inline-block",
				background: "rgba(0,0,0,0.7)",
				padding: "3px 5px",
				color: "#FFFFFF",
				position: "fixed",
				"z-index": 3,
				"border-radius": "3px",
				"font-size": "12px",
			},
			chartVisibleCheck: false,		// True will create timer to loop check the chart exist or not. Always suggest use 'plot.destroy' to destroy chart.
			//xFormat: "YYYY-MM-DD HH:mm:ss",
			xFormat: "%Y-%M-%D %H:%m:%s",	// %W:weekday, %MS:millionSecond
			yFormat: "%value",				// %value.number: support to fixed the value
			formatter: null,				// Customize formatter function. Return DOM element as tooltip content. Return null will not display tool tip.
		}
	};

	// ================================================
	// =                    Format                    =
	// ================================================
	function _fillZero(num, len) {
		if(len === undefined) len = 2;
		var str = num + "";
		while(str.length < len) {
			str = "0" + str;
		}
		return str;
	}

	function _parseValue(value, format, plot) {
		if(typeof format === "function") {
			return format(value, plot);
		} else {
			var _date = new Date(value);

			return format
			.replace(/(%value)((\.)(\w+))?/, function() {
				var _decimal = arguments[4];
				if(_decimal !== undefined) {
					return value.toFixed(Number(_decimal));
				} else {
					return value;
				}
			})

			.replace(/%MS/g, _fillZero(_date.getUTCMilliseconds(), 3))
			.replace(/%Y/g, _fillZero(_date.getUTCFullYear(), 4))
			.replace(/%M/g, _fillZero(_date.getUTCMonth() + 1))
			.replace(/%W/g, _date.getUTCDay() + 1)
			.replace(/%D/g, _fillZero(_date.getUTCDate()))
			.replace(/%H/g, _fillZero(_date.getUTCHours()))
			.replace(/%m/g, _fillZero(_date.getUTCMinutes()))
			.replace(/%s/g, _fillZero(_date.getUTCSeconds()));
		}
	}

	function parseContent(plot, dataList, item, pos, closestSeriesIndex) {
		var _options = plot.getOptions();
		var _data = plot.getData();
		var _tooltipOps = _options.tooltip;

		var $cntr = $("<div>");

		if(getValueByPath(_options, "series.pie.show")) {
			// Pie Chart
			if(!item) return null;

			var _series = item.series;
			var _title = _series.label;
			if(typeof _title === "number") {
				_title = _parseValue(_title, _tooltipOps.xFormat, plot);
			}

			$("<span>")
			.append($("<span>").html("\u25CF").css("color", _series.color)).append(" ")
			.append(_title)
			.append(": ")
			.append($("<b>").text(_parseValue(item.datapoint[1][0][1], _tooltipOps.yFormat, plot)))
			.appendTo($cntr);
		} else {
			// Other Chart
			if(!dataList.length) return null;

			var _share = _tooltipOps.share !== false;

			// Check x-axis is same or not
			var _time = getValueByPath(dataList, "0.0", null);
			var _multiTime = false;
			if(_share) {
				$.each(dataList, function(index, point) {
					if(point[0] !== _time) {
						_multiTime = true;
						return false;
					}
				});
			} else {
				_multiTime = true;
			}

			// Display Content
			$.each(dataList, function(index, point) {
				if(!_share && closestSeriesIndex !== index) {
					return;
				}

				if(point[1] === null) {
					return;
				}

				// X-Axis
				if(index === 0 || _multiTime) {
					if(index !== 0) $cntr.append("<br/>");
					$("<span>").text(_parseValue(point[0], _tooltipOps.xFormat, plot)).appendTo($cntr);
				}

				var _series = _data[index];

				$cntr.append("<br/>");
				var $row = $("<span>")
				.append($("<span>").html("\u25CF").css("color", _series.color)).append(" ")
				.append(_series.label)
				.append(": ")
				.append($("<b>").text(_parseValue(point[1], _tooltipOps.yFormat, plot)))
				.appendTo($cntr);
			});

			if($($cntr.children()[0]).is("br")) {
				$($cntr.children()[0]).remove();
			}
		}

		return $cntr;
	}

	// ================================================
	// =                      UI                      =
	// ================================================
	var init = function(plot) {
		var $cntr = $(plot.getPlaceholder());
		var _lastToolTip = null;
		var _checkID = null;
		var _showID;
		var _lastPos;
		var _lastItem;
		var _elementID;

		function tooltip(ele) {
			if(ele === null) {
				$cntr.removeData("flot-tooltip");
			} else if(ele) {
				_lastToolTip = ele;
				$cntr.data("flot-tooltip", ele);
			}
			if(_elementID) {
				ele = $("#" + _elementID);
				return ele.length ? ele : null;
			} else {
				return $cntr.data("flot-tooltip");
			}
		}

		function removeTooltip(immediately) {
			var _timeoutID = $cntr.data("flot-tooltip-timeout");
			clearTimeout(_timeoutID);

			_timeoutID = setTimeout(function() {
				_lastToolTip = _elementID ? tooltip() : (_lastToolTip || tooltip());
				if(!_lastToolTip) return;

				_lastToolTip.fadeOut(function() {
					$(this).remove();
				});
				tooltip(null);
			}, immediately === true ? 0 : 250);

			$cntr.data("flot-tooltip-timeout", _timeoutID);
			if(tooltip()) tooltip().data("flot-tooltip-timeout", _timeoutID);
		}

		// Show tool tip
		function tracker(plot, eventHolder) {
			var _options = plot.getOptions();
			var _tooltipOps = _options.tooltip;
			var _className = _tooltipOps.className;
			_elementID = _tooltipOps.id;

			if(!_options.grid.hoverable) return;

			// Loop checker
			if(_tooltipOps.chartVisibleCheck) {
				_checkID = setInterval(function() {
					if(_lastToolTip && !$cntr.is(":visible")) {
						tooltip(null);
						_lastToolTip.remove();
						_lastToolTip = null;
					}
				}, 1000);
			}

			eventHolder.on("mousemove.flot-tooltip", function(event) {
				clearTimeout($cntr.data("flot-tooltip-timeout"));
				var _content;

				var $tooltip = tooltip();
				if(!$tooltip) {
					_content = getContent();
					if(_content === null) return;

					$tooltip = $("<div>").addClass(_className).appendTo(_tooltipOps.container);
					if(_elementID) $tooltip.attr("id", _elementID);

					if(_tooltipOps.useCSS) {
						$.each(_tooltipOps.css, function(key, value) {
							$tooltip.css(key, value);
						});
					}
					tooltip($tooltip);
					showContent(_content);
				}
				if(_elementID) {
					$tooltip.stop().css("opacity", 1);
					clearTimeout($tooltip.data("flot-tooltip-timeout"));
				}
				

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

				if(!_showID) {
					_showID = setTimeout(showContent, tooltip() ? 50 : 0);
				}
			});
			eventHolder.on("mouseout.flot-tooltip", function(event) {
				removeTooltip();
			});

			// Tool tip Content
			function getContent() {
				var _x = _lastPos.x;
				var _y = _lastPos.y;

				var axes = plot.getAxes();
				if(_x < axes.xaxis.min) _x = axes.xaxis.min;
				if(_x > axes.xaxis.max) _x = axes.xaxis.max;
				if(_y < axes.yaxis.min) _y = axes.yaxis.min;
				if(_y > axes.yaxis.max) _y = axes.yaxis.max;

				var _dataList = [];
				var _closestSeriesIndex = 0;
				var _closestSeriesDist;
				$.each(plot.getData(), function(index, series) {
					var _hasPoint = false;
					for(var i = 0 ; i < series.data.length - 1 ; i += 1) {
						var _unitL = series.data[i];
						var _unitR = series.data[i + 1];
						var _unitClose;

						if(_unitL[0] <= _x && _x <= _unitR[0]) {
							// Find Closest
							if(getValueByPath(series, "bars.show") || Math.abs(_unitL[0] - _x) < Math.abs(_x - _unitR[0])) {
								_unitClose = _unitL;
							} else {
								_unitClose = _unitR;
							}
							_dataList.push(_unitClose);

							// Calculate Closest Series
							var _desX = _x - _unitClose[0];
							var _desY = _y - _unitClose[1];
							var _dist = Math.sqrt(_desX * _desX + _desY * _desY);
							if(_closestSeriesDist === undefined || _dist < _closestSeriesDist) {
								_closestSeriesIndex = index;
								_closestSeriesDist = _dist;
							}

							_hasPoint = true;
							break;
						}
					}
					if(!_hasPoint) _dataList.push([null, null]);
				});

				return _tooltipOps.formatter ? _tooltipOps.formatter(plot, _dataList, _lastItem, _lastPos, _closestSeriesIndex) : parseContent(plot, _dataList, _lastItem, _lastPos, _closestSeriesIndex);
			}
			function showContent(content) {
				_showID = 0;

				if(!content) content = getContent();

				if(tooltip()) {
					if(content) {
						tooltip().html(content);
					} else {
						removeTooltip(true);
					}
					return content;
				}
				return false;
			}
			$cntr.on("plothover.flot-tooltip", function (event, pos, item) {
				_lastPos = pos;
				_lastItem = item;
			});
		}

		plot.hooks.bindEvents.push(tracker);
		plot.hooks.shutdown.push(function (plot, eventHolder) {
			eventHolder.off("mousemove.flot-tooltip");
			eventHolder.off("mouseout.flot-tooltip");
			$(plot.getPlaceholder()).off("plothover.flot-tooltip");

			removeTooltip();
			clearInterval(_checkID);
		});
	};

	// define Flot plugin
	$.plot.plugins.push({
		init : init,
		options : defaultOptions,
		name : 'tooltip',
		version : '0.8.3.2'
	});
})(jQuery);
