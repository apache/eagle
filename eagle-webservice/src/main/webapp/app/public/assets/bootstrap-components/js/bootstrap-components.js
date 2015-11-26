/*Bootstrap Components 3.1 - Created By ZombieJ*/
$.extend({_bc: new Object()});
// init vars for bootstrap-component use
$._bc.vals = new Object();

// get the setting & callback
$._bc.vars = function(options, callback){
	var vars = new Object();
	if(typeof(options) == 'object') {
		vars.options = options;
	} else if(typeof(options) == 'function') {
		vars.callback = options;
	} else if(typeof(options) == 'string') {
		vars.key = options;
	}
	if(typeof(callback) == 'function') {
		vars.callback = callback;
	}
	return vars;
}
// get the option by key and return default if not set
$._bc.get = function(options, key, defaultValue) {
	if(options != null) {
		if(options[key] != null) {
			return options[key];
		} else {
			return defaultValue;
		}
	} else {
		return defaultValue;
	}
}

// get a simple list to add / remove element
$._bc.list = function() {
	var list = new Array();
	list.add = function(obj) {
		list.push(obj);
	}
	list.remove = function(obj) {
		var loc = null;
		for (var i = 0; i < list.length; i++) {
			var _o = list[i];
			if(obj == _o) {
				loc = i;
				break;
			}
		}
		if(loc != null) list.splice(loc, 1);
	}
	return list;
}

// get the broswer version
var userAgent = navigator.userAgent.toLowerCase();
$._bc.browser = { 
version: (userAgent.match( /.+(?:rv|it|ra|ie)[\/: ]([\d.]+)/ ) || [])[1], 
safari: /webkit/.test( userAgent ), 
opera: /opera/.test( userAgent ), 
msie: /msie/.test( userAgent ) && !/opera/.test( userAgent ), 
mozilla: /mozilla/.test( userAgent ) && !/(compatible|webkit)/.test( userAgent ) 
};/* options:
				boolean			default false. true to open auto tooltips else to close it.
*/

!function ($) {
	var _on = false;

	$.extend({
		autotooltip:function(option){
			if(option === true && _on == false) {
				$(document).on("mouseover.bs.autotooltip", "[data-toggle='tooltip']", tooltipHandler);
				_on = true;
			} else if(option === false && _on == true) {
				$(document).off("mouseover.bs.autotooltip");
				_on = false;
			}
		}
	});

	function tooltipHandler() {
		var _my = $(this);
		if(_my.attr("data-original-title") == null) {
			_my.tooltip('show');
		}
	};
}(window.jQuery);/* options:
	to:			element			set the value of target element(only for checkbox)

	checked:	boolean			set checkbox checked or not
*/

!function ($) {
	$.fn.extend({
		checkbox:function(options){
			// get options
			var _my = $(this);
			var vars = $._bc.vars(options);
			var _options = vars.options;
			var _checked = $._bc.get(_options, "checked", null);
			var _to = $._bc.get(_options, "to", null);
				var _target = _to != null ? $(_to) : $(_my.attr("data-to"));

			// set target element
			if(_to != null) {
				_my.attr("data-to", _to);
			}

			// set the value of checkbox and it will change target element too.
			if(_checked != null) {
				updateStatus(_my, _checked);
				updateTarget(_target, _checked);
			}
		}
	});

	// change checkbox status
	function updateStatus(_instance, _checked) {
		if(_checked) {
			_instance.attr("checked", "checked");
		} else {
			_instance.removeAttr("checked", "checked");
		}
	}

	// update data to target status
	function updateTarget(_target, _checked) {
		if(_target != null) {
			if(_target.is("[type='checkbox']") || _target.is("[type='radio']")) {
				_target.prop("checked", _checked);
			} else {
				_target.val(_checked);
			}
		}
	}

	// click on the label
	$(document).on("click.bs.checkbox.label", "label", function(event) {
		var _label = $(this);
		var _my = _label.find(".checkbox[data-toggle='checkbox']");
		var _checked = null;
		if(_my.length != 0) {			// find checkbox to go on
			var _disabled = _my.attr("disabled") != null;
			var _target = $(_my.attr("data-to"));
			var _checkbox = _label.find("input[type='checkbox']");
			if(_disabled) {
				if(_checkbox.length != 0) {
					_checked = _my.attr("checked") != null;
					_checkbox.prop("checked", _checked);
				}
			} else {
				if(_checkbox.length != 0) {
					_checked = _checkbox.prop("checked");
				} else {
					_checked = !(_my.attr("checked") != null);
					updateTarget(_target, _checked);
				}
			}
			updateStatus(_my, _checked);

			// change event
			_my.add(_target).change();
		}
	});

	// click on the checkbox without label
	$(document).on("click.bs.checkbox", ".checkbox[data-toggle='checkbox']", function(event){
		var _my = $(this);
		var _target = $(_my.attr("data-to"));
		var _checked = null;
		if(_my.closest("label").length == 0) {
			_checked = !(_my.attr("checked") != null);
			updateStatus(_my, _checked);
			updateTarget(_target, _checked);

			// change event
			_my.add(_target).change();
		}
	});

	// select all
	function elementValue(ele, val) {
		var _my = $(ele);
		if(_my.is("input[type='checkbox']")) {
			if(val != null) _my.prop("checked", val);
			return _my.prop("checked");
		} else {
			if(val != null) _my.checkbox({checked: val});
			return _my.attr("checked") == "checked";
		}
	}
	$(document).on("change.bs.checkbox_selectAll", "[data-checkbox-all]", function(event){
		var _name = $(this).attr("data-checkbox-all");
		var _value = elementValue(this);
		$("[data-checkbox-entity='" + _name + "']").each(function() {
			elementValue(this, _value);
		});
	});
	$(document).on("change.bs.checkbox_selectAll", "[data-checkbox-entity]", function(event){
		var _name = $(this).attr("data-checkbox-entity");
		var _checked = true;

		$("[data-checkbox-entity='" + _name + "']").each(function() {
			if(!elementValue(this)) {
				_checked = false;
				return false;
			}
		});
		$("[data-checkbox-all='" + _name + "']").each(function() {
			elementValue(this, _checked);
		});
	});
}(window.jQuery);/* options:
	target:		all(default)	contains date & time picker
				time			time picker only
				date			date picker only
				month			month picker only
				year			year picker only
	to:			element			set the value of target element
	container:	string			set datepicker component container
	before:		string			set the date/time picker can't pass
	after:		string			set the date/time picker can't before
	goon:		boolean			default false. True will change value immediately when click. - TODO
*/

// init env
$._bc.vals.datepicker = new Object();
$._bc.vals.datepicker.index = 1;

// init function
!function ($) {
	$.extend({
		datepicker: {
			toDate: function(str, format) {
				var date = new Date();
				try {
					var lst = ["d", "M", "y", "H", "m", "s"];
					var dc = {};

					$.each(lst, function(i, c) {
						var _fmt = format.replace(new RegExp(c + "+"), "[TARGET]");
						$.each(lst, function(i, c) {
							_fmt = _fmt.replace(new RegExp(c + "+"), "\\d+");
						});
						_fmt = _fmt.replace("[TARGET]", "(\\d+)");
						var reg = new RegExp(_fmt);
						dc[c] = Number(str.match(reg)[1]);
					});

					function val(val, def) {
						if(val === undefined || val === null || isNaN(val))
							return def;
						return val;
					}

					date.setFullYear(val(dc["y"], 1990), (val(dc["M"], 1)) - 1, val(dc["d"], 1));
					date.setHours(val(dc["H"], 0), val(dc["m"], 0), val(dc["s"], 0));
				} catch(err) {
					return new Date();
				}
				if(date == null || isNaN(date)) return new Date();
				return date;
			},
			dateToStrng: function(date, _format) {
				var lst = ["d", "M", "y", "H", "m", "s"];
				var _val = _format.replace(/y+/,date.getFullYear()).replace(/M+/, fillZero(date.getMonth() + 1)).replace(/d+/, fillZero(date.getDate()))
				.replace(/H+/, fillZero(date.getHours())).replace(/m+/, fillZero(date.getMinutes())).replace(/s+/, fillZero(date.getSeconds()));
				return _val;
			},
			monthName: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
			yearMonthTitle: "${month}-${year}",
		},
	});

	// var
	var _instance = null;
	var _preventEvent = false;

	// Functions
	function refreshInstance(instance) {
		if(_instance != null) {
			var _type = _instance.getType();
			var _date = _instance.getDate();
			var _target = _instance.getTarget();
			var _format = _instance.getFormat();
			var _preVal = _target.val();
			var _val = null;
			_val = $.datepicker.dateToStrng(_date, _format);
			_target.val(_val);
			_instance.remove();
			_instance = null;

			// trigger event
			if(_val != _preVal) {
				_target.change();
			}
		}
		_instance = instance;
	}
	function getDaysOfMonth(date) {
		var _startDay, _days;
		var _date = new Date(date.getTime());
		_date.setMonth(_date.getMonth() + 1, 0);
		_days =  _date.getDate();
		_date.setDate(1);
		_startDay = _date.getDay();
		return [_startDay, _days];
	}
	function fillZero(str, len) {
		if(len == null) {
			len = 2;
		}
		var ret = str + "";
		while(ret.length < len) {
			ret = "0" + ret;
		}
		return ret;
	}
	function toString(date) {
		return	date.getFullYear() + "-" + fillZero(date.getMonth() + 1) + "-" + fillZero(date.getDate()) + " " +
				fillZero(date.getHours()) + ":" + fillZero(date.getMinutes()) + ":" + fillZero(date.getSeconds())
	}
	function plusDays(date, year, month, day) {
		if(month == null) {
			month = 0;
		}
		if(day == null) {
			day = 0;
		}
		var _ret = new Date(date.getTime());
		var _year = _ret.getFullYear();
		var _month = _ret.getMonth() + month + year * 12;
		var _date = _ret.getDate();

		_ret.setMonth(_month, 1);
		var _days = getDaysOfMonth(_ret)[1];
		if(_date <= _days) {
			_ret.setDate(_date);
		} else {
			_ret.setDate(_days);
		}

		_date = _ret.getDate() + day;
		_ret.setDate(_date);
		return _ret;
	}
	function setDays(date, year, month, day) {
		if(year == null) {
			year = date.getFullYear();
		}
		if(month == null) {
			month = date.getMonth();
		}
		if(day == null) {
			day = date.getDate();
		}

		var _ret = new Date(date.getTime());
		_ret.setFullYear(year, month, day);
		_ret.setFullYear(year, month);
		_ret.setFullYear(year);
		return _ret;
	}
	function digitization(str, mix, max) {
		var _str = str + "";
		var _val = Number(_str);
		if(isNaN(_val) || _val == null) {
			_val = 0;
		}
		if(_val < mix) {
			_val = mix;
		}
		if(_val > max) {
			_val = max;
		}
		return _val;
	}
	// set the current date
	function getDateInRange(current, target, before, after) {
		if(before != null && target > before) {
			return new Date(before.getTime());
		}
		if(after != null && target < after) {
			return new Date(after.getTime());
		}
		return target;
	}

	// Datepicker Handler
	$(document).on("click.bs.datepicker", "[data-toggle='datepicker']", function(event){
		_preventEvent = true;
		var my = $(this);

		// Skip refresh if container work for the same element
		if($(_instance).data("trigger") === this) return;

		var _autoclose = my.attr("data-autoclose") === "true";
		var _target = $(my.attr("data-to"));
			var target = _target.length != 0 ? _target : my;
		var _format = my.attr("data-format");
		var _container = $(my.attr("data-container"));
			var container = target.parent();
			if(container.hasClass("input-group-btn")) {
				container = container.parent();
			}
			if(container.hasClass("input-group")) {
				container = container.parent();
			}
			container = _container.length != 0 ? _container : container;
		var _type = my.attr("data-type");
			var enable_yearpicker = false;
			var enable_monthpicker = false;
			var enable_datepicker = false;
			var enable_timepicker = false;
			switch(_type) {
			case "year":
				enable_yearpicker = true;
				_format = _format || "yyyy";
				break;
			case "month":
				enable_monthpicker = true;
				_format = _format || "yyyy-MM";
				break;
			case "date":
				enable_datepicker = true;
				_format = _format || "yyyy-MM-dd";
				break;
			case "time":
				enable_timepicker = true;
				_format = _format || "HH:mm:ss";
				break;
			default:
				_type = "all";
				enable_datepicker = true;
				enable_timepicker = true;
				_format = _format || "yyyy-MM-dd HH:mm:ss";
			}
		var _date = target.val();
			var date = $.datepicker.toDate(_date, _format);
			var dateShadow = new Date(date.getTime());		// an date which is display the current view
			var dateCurrent = new Date(date.getTime());		// an date which is mark as current date
		var _before = my.attr("data-before");
			var before = _before == null ? null : $.datepicker.toDate(_before, _format);
		var _after = my.attr("data-after");
			var after = _after == null ? null : $.datepicker.toDate(_after, _format);

		// generate datepicker component
		var $container = $('<div class="bsc-datepicker">');
			$container.data("trigger", this);
		var $yearpicker = $('<div class="yearpicker picker-group">');
			var $yearpicker_header = $('<div class="picker-header">');
				var $yearpicker_header_year_minus = $('<button class="btn btn-default minus" type="button">');
					$yearpicker_header_year_minus.html('<span class="glyphicon glyphicon-chevron-left"></span>');
				var $yearpicker_header_title = $('<h4>&nbsp;</h4>');
				var $yearpicker_header_year_plus = $('<button class="btn btn-default plus" type="button">');
					$yearpicker_header_year_plus.html('<span class="glyphicon glyphicon-chevron-right"></span>');
			var $yearpicker_body = $('<div class="picker-body picker-selectable clearfix">');
		var $monthpicker = $('<div class="monthpicker picker-group">');
			var $monthpicker_header = $('<div class="picker-header">');
				var $monthpicker_header_year_minus = $('<button class="btn btn-default minus" type="button">');
					$monthpicker_header_year_minus.html('<span class="glyphicon glyphicon-chevron-left"></span>');
				var $monthpicker_header_title = $('<h4>&nbsp;</h4>');
				var $monthpicker_header_year_plus = $('<button class="btn btn-default plus" type="button">');
					$monthpicker_header_year_plus.html('<span class="glyphicon glyphicon-chevron-right"></span>');
			var $monthpicker_body = $('<div class="picker-body picker-selectable clearfix">');
		var $datepicker = $('<div class="datepicker picker-group">');
			var $datepicker_header = $('<div class="picker-header">');
				var $datepicker_header_month_minus = $('<button class="btn btn-default minus" type="button">');
					$datepicker_header_month_minus.html('<span class="glyphicon glyphicon-step-backward"></span>');
				var $datepicker_header_title = $('<h4>&nbsp;</h4>');
				var $datepicker_header_month_plus = $('<button class="btn btn-default plus" type="button">');
					$datepicker_header_month_plus.html('<span class="glyphicon glyphicon-step-forward"></span>');
			var $datepicker_body = $('<div class="picker-body">');
				var $datepicker_body_description = $('<div class="datepicker-body-description clearfix">');
				var $datepicker_body_date = $('<div class="datepicker-body-value picker-selectable clearfix">');
		var $timepicker = $('<div class="timepicker picker-group clearfix">');
			var $timepicker_group_hours = $('<div class="timepicker-group">');
				var $timepicker_group_hours_input = $('<input class="form-control" type="text" />');
				var $timepicker_group_hours_minus = $('<button type="button" class="btn btn-default time-minus">');
					$timepicker_group_hours_minus.html('<span class="glyphicon glyphicon-minus"></span>');
				var $timepicker_group_hours_plus = $('<button type="button" class="btn btn-default time-plus">');
					$timepicker_group_hours_plus.html('<span class="glyphicon glyphicon-plus"></span>');
			var $timepicker_group_minutes = $('<div class="timepicker-group">');
				var $timepicker_group_minutes_input = $('<input class="form-control" type="text" />');
				var $timepicker_group_minutes_minus = $('<button type="button" class="btn btn-default time-minus">');
					$timepicker_group_minutes_minus.html('<span class="glyphicon glyphicon-minus"></span>');
				var $timepicker_group_minutes_plus = $('<button type="button" class="btn btn-default time-plus">');
					$timepicker_group_minutes_plus.html('<span class="glyphicon glyphicon-plus"></span>');
			var $timepicker_group_seconds = $('<div class="timepicker-group">');
				var $timepicker_group_seconds_input = $('<input class="form-control" type="text" />');
				var $timepicker_group_seconds_minus = $('<button type="button" class="btn btn-default time-minus">');
					$timepicker_group_seconds_minus.html('<span class="glyphicon glyphicon-minus"></span>');
				var $timepicker_group_seconds_plus = $('<button type="button" class="btn btn-default time-plus">');
					$timepicker_group_seconds_plus.html('<span class="glyphicon glyphicon-plus"></span>');

		$container.appendTo(container);
		$container.append($yearpicker);
			$yearpicker.append($yearpicker_header);
				$yearpicker_header.append($yearpicker_header_year_minus);
				$yearpicker_header.append($yearpicker_header_title);
				$yearpicker_header.append($yearpicker_header_year_plus);
			$yearpicker.append($yearpicker_body);
		$container.append($monthpicker);
			$monthpicker.append($monthpicker_header);
				$monthpicker_header.append($monthpicker_header_year_minus);
				$monthpicker_header.append($monthpicker_header_title);
				$monthpicker_header.append($monthpicker_header_year_plus);
			$monthpicker.append($monthpicker_body);
				$.each(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec'], function(i, _month) {
					var $element = $('<span>');
					$element.attr("data-month", i);
					$element.text(_month);
					$monthpicker_body.append($element);
				});
		$container.append($datepicker);
			$datepicker.append($datepicker_header);
				$datepicker_header.append($datepicker_header_month_minus);
				$datepicker_header.append($datepicker_header_title);
				$datepicker_header.append($datepicker_header_month_plus);
			$datepicker.append($datepicker_body);
				$datepicker_body.append($datepicker_body_description);
					$.each(['Sun', 'Mon', 'Tue', 'Wed', 'Thur', 'Fri', 'Sat'], function(i, _description) {
						var $element = $('<span class="disabled">');
						$element.text(_description);
						$datepicker_body_description.append($element);
					});
				$datepicker_body.append($datepicker_body_date);
		$container.append($timepicker);
			$timepicker.append($timepicker_group_hours);
				$timepicker_group_hours.append($timepicker_group_hours_input);
				$timepicker_group_hours.append($timepicker_group_hours_minus);
				$timepicker_group_hours.append($timepicker_group_hours_plus);
			$timepicker.append('<span class="timepicker-spliter">:</span>');
			$timepicker.append($timepicker_group_minutes);
				$timepicker_group_minutes.append($timepicker_group_minutes_input);
				$timepicker_group_minutes.append($timepicker_group_minutes_minus);
				$timepicker_group_minutes.append($timepicker_group_minutes_plus);
			$timepicker.append('<span class="timepicker-spliter">:</span>');
			$timepicker.append($timepicker_group_seconds);
				$timepicker_group_seconds.append($timepicker_group_seconds_input);
				$timepicker_group_seconds.append($timepicker_group_seconds_minus);
				$timepicker_group_seconds.append($timepicker_group_seconds_plus);

		refreshInstance($container);
		var pos = target.offset();
		$container.offset({
			left: pos.left,
			top: pos.top + target.outerHeight(),
		}).click(function() {
			_preventEvent = true;
		});

		// bind data
		$container.getFormat = function() {
			return _format;
		}
		$container.getTarget = function() {
			return target;
		}
		$container.getType = function() {
			return _type;
		}
		$container.getDate = function() {
			return dateCurrent;
		}

		// page change event handler
			function refreshCurrentDate() {
				dateCurrent = getDateInRange(dateCurrent, dateShadow, before, after);
			}
			// switch picker view
			$datepicker_header_title.click(function() {
				$datepicker.slideUp();
				$monthpicker.slideDown();
			});
			$monthpicker_header_title.click(function() {
				$monthpicker.slideUp();
				$yearpicker.slideDown();
			});

			// year picker
			$yearpicker_header_year_minus.click(function() {
				dateShadow = plusDays(dateShadow, -20);
				refreshCurrentDate();
				draw();
			});
			$yearpicker_header_year_plus.click(function() {
				dateShadow = plusDays(dateShadow, 20);
				refreshCurrentDate();
				draw();
			});
			$yearpicker_body.on("click", "span:not(.disabled)", function() {
				var year = Number($(this).text());
				dateShadow = setDays(dateShadow, year);
				refreshCurrentDate();
				draw();
				if(_type === "year" && _autoclose) refreshInstance(null);

				if(!enable_yearpicker) {
					$yearpicker.slideUp();
					$monthpicker.slideDown();
				}
			});

			// month picker
			$monthpicker_header_year_minus.click(function() {
				dateShadow = plusDays(dateShadow, -1);
				refreshCurrentDate();
				draw();
			});
			$monthpicker_header_year_plus.click(function() {
				dateShadow = plusDays(dateShadow, 1);
				refreshCurrentDate();
				draw();
			});
			$monthpicker_body.on("click", "span:not(.disabled)", function() {
				var _month = Number($(this).attr("data-month"));
				dateShadow = setDays(dateShadow, null, _month);
				refreshCurrentDate();
				draw();
				if(_type === "month" && _autoclose) refreshInstance(null);

				if(!enable_monthpicker) {
					$monthpicker.slideUp();
					$datepicker.slideDown();
				}
			});

			// date picker
			$datepicker_header_month_minus.click(function() {
				dateShadow = plusDays(dateShadow, 0, -1);
				refreshCurrentDate();
				draw();
			});
			$datepicker_header_month_plus.click(function() {
				dateShadow = plusDays(dateShadow, 0, 1);
				refreshCurrentDate();
				draw();
			});
			$datepicker_body.on("click", "span:not(.inactive):not(.disabled)", function() {
				var _date = Number($(this).text());
				dateShadow = setDays(dateShadow, null, null, _date);
				refreshCurrentDate();
				draw();
				if(_type === "date" && _autoclose) refreshInstance(null);
			});

			// time picker
			// hours
			$timepicker_group_hours_minus.click(function() {
				dateShadow.setHours(dateShadow.getHours() - 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_hours_plus.click(function() {
				dateShadow.setHours(dateShadow.getHours() + 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_hours_input.change(function() {
				var _val = digitization($(this).val(), 0, 23);
				dateShadow.setHours(_val);
				refreshCurrentDate();
				draw();
			});
			// minutes
			$timepicker_group_minutes_minus.click(function() {
				dateShadow.setMinutes(dateShadow.getMinutes() - 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_minutes_plus.click(function() {
				dateShadow.setMinutes(dateShadow.getMinutes() + 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_minutes_input.change(function() {
				var _val = digitization($(this).val(), 0, 59);
				dateShadow.setMinutes(_val);
				refreshCurrentDate();
				draw();
			});
			// seconds
			$timepicker_group_seconds_minus.click(function() {
				dateShadow.setSeconds(dateShadow.getSeconds() - 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_seconds_plus.click(function() {
				dateShadow.setSeconds(dateShadow.getSeconds() + 1);
				refreshCurrentDate();
				draw();
			});
			$timepicker_group_seconds_input.change(function() {
				var _val = digitization($(this).val(), 0, 59);
				dateShadow.setSeconds(_val);
				refreshCurrentDate();
				draw();
			});

		// show needed components
		if(!enable_yearpicker) {
			$yearpicker.hide();
		}
		if(!enable_monthpicker) {
			$monthpicker.hide();
		}
		if(!enable_datepicker) {
			$datepicker.hide();
		}
		if(!enable_timepicker) {
			$timepicker.hide();
		}

		// fill date in the view
		function draw() {
			var _year = dateShadow.getFullYear();
				var year_start = _year - _year % 20;
				var year_end = year_start + 19;
			var _month = dateShadow.getMonth();
				var month = _month + 1;
			var _date = dateShadow.getDate();
				var days = getDaysOfMonth(dateShadow);
			var _hours = dateShadow.getHours();
			var _minutes = dateShadow.getMinutes();
			var _seconds = dateShadow.getSeconds();

			// year picker
			$yearpicker_header_title.text(year_start + " - " + year_end);
			$yearpicker_body.empty();
			for(var i = year_start ; i <= year_end ; i += 1) {
				var $element = $('<span>');
				$element.text(i);
				if(i == _year) {
					$element.addClass('active');
				}
				if(		(before != null && before.getFullYear() < i) || 
						( after != null && after.getFullYear() > i)) {
					$element.addClass('disabled');
				}
				$yearpicker_body.append($element);
			}

			// month picker
			$monthpicker_header_title.text(_year);
			$monthpicker_body.find("span").each(function(i, ele) {
				var $element = $(ele);
				$element.removeClass("active");
				$element.removeClass("disabled");
				if(i == _month) {
					$element.addClass("active");
				}
				if(		(before != null && before.getFullYear() * 100 + before.getMonth() < _year * 100 + i) || 
						( after != null && after.getFullYear() * 100 + after.getMonth() > _year * 100 + i)) {
					$element.addClass('disabled');
				}
			});

			// date picker
			$datepicker_header_title.text(
				$.datepicker.yearMonthTitle
				.replace(/\$\{month\}/g, $.datepicker.monthName[month - 1])
				.replace(/\$\{year\}/g, _year)
			);
			$datepicker_body_date.empty();
			for(var i = 0; i < days[0] ; i+= 1) {
				var $element = $('<span class="inactive">');
				$datepicker_body_date.append($element);
			}

			for(var i = 1; i <= days[1] ; i+= 1) {
				var $element = $('<span>');
				$element.text(fillZero(i));
				if(i == _date) {
					$element.addClass('active');
				}
				if(		(before != null && before.getFullYear() * 10000 + before.getMonth() * 100 + before.getDate() < _year * 10000 + _month * 100 + i) || 
						( after != null && after.getFullYear() * 10000 + after.getMonth() * 100 + after.getDate() > _year * 10000 + _month * 100 + i)) {
					$element.addClass('disabled');
				}
				$datepicker_body_date.append($element);
			}

			// time picker
			$timepicker_group_hours_input.val(fillZero(_hours));
			$timepicker_group_minutes_input.val(fillZero(_minutes));
			$timepicker_group_seconds_input.val(fillZero(_seconds));
		}
		draw();

		// get date by view
		function viewToDate() {
			var _year = $yearpicker_body.find("span.active");
		}
	});

	// hide datapicker if exist
	$(document).on("click.bs.datepicker", function(event){
		if(_preventEvent == true) {
			_preventEvent = false;
		} else {
			refreshInstance(null);
		}
	});
}(window.jQuery);/*	this is to help hightlight target element with dark background.
options:
	title:			string						specify title of dialog.
	content:		element						specify content of dialog.
	close:			boolean						default is true for alert window contains close button
	confirm:		boolean						default is false for dialog can check yes or no
	buttons:		array						if setted will disable confirm, e.x.
												[{name: "delete", class: "btn btn-danger", left: true},
												{name: "Not Yet", value: -1}, {name: "That's Time!"}]
	fade:			boolean						default is true for dialog fade animation
	size:			"small","normal","large"	default is "normal" for dialog size
	!other options which contains in modal

callback:			[function]			it will trigger event when user close this dialog by click the return button.
										return boolean of confirm, and false of alert and close button.
*/

// init env
$._bc.vals.dialog = new Object();
$._bc.vals.dialog.z_index = 1051;

// init function
$.extend({
	dialog:function(options, callback){
		// get options
		var vars = $._bc.vars(options, callback);
		var _options = vars.options;
		var _callback = vars.callback;

		var _title = $._bc.get(_options, "title", "");
		var _content = $._bc.get(_options, "content", "");
		var _close = $._bc.get(_options, "close", true);
		var _confirm = $._bc.get(_options, "confirm", false);
		var _buttons = $._bc.get(_options, "buttons", null);
		var _fade = $._bc.get(_options, "fade", true);
		var _size = $._bc.get(_options, "size", "");

		var _ret = null;

		// generate modal
		var $modal = $('<div class="modal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true" style="display: none;">');
		var $modal_dialog = $('<div class="modal-dialog">');
		var $modal_content = $('<div class="modal-content">');
		var $modal_header = $('<div class="modal-header">');
		var $modal_header_close = $('<button type="button" class="close" data-dismiss="modal" aria-hidden="true">x</button>');
		var $modal_header_head = $('<h4 class="modal-title" id="myModalLabel">');
		var $modal_body = $('<div class="modal-body">');
		var $modal_footer = $('<div class="modal-footer">');

		$modal.appendTo("body");
		$modal.append($modal_dialog);
		$modal_dialog.append($modal_content);
		$modal_content.append($modal_header);
			if(_close) $modal_header.append($modal_header_close);
			$modal_header.append($modal_header_head);
		$modal_content.append($modal_body);
		$modal_content.append($modal_footer);

		// fill title & content
		$modal_header_head.html(_title);
		$modal_body.html(_content);

		if (_fade) {
			$modal.addClass('fade');
		}

		switch(_size) {
		case "small":
			$modal_dialog.addClass("modal-sm");
			break;
		case "large":
			$modal_dialog.addClass("modal-lg");
			break;
		}

		// fill buttons in footer
		if(_buttons != null) {
			var _len = _buttons.length;
			for(var i = 0 ; i < _len ; i += 1) {
				var _btn = _buttons[i];
				var _name = _btn.name;
				var _class = $._bc.get(_btn, "class", "btn-default");
				var _left = _btn.left === true;
				var _value = $._bc.get(_btn, "value", _name);

				var $btn = $('<button type="button" class="btn">');
				$btn.text(_name);
				$btn.addClass(_class);
				if(_left) $btn.addClass("pull-left");
				$btn.data("value", _value);
				$modal_footer.append($btn);

				$btn.add($btn_confirm).click(function() {
				_ret = $(this).data("value");
				$modal.modal('hide');
			});
			}
		} else if(_confirm) {
			var $btn_cancel = $('<button type="button" class="btn btn-default">Cancel</button>').data("value", false);
			var $btn_confirm = $('<button type="button" class="btn btn-primary">Confirm</button>').data("value", true);
			$modal_footer.append($btn_cancel);
			$modal_footer.append($btn_confirm);

			$btn_cancel.add($btn_confirm).click(function() {
				_ret = $(this).data("value");
				$modal.modal('hide');
			});
		} else {
			var $btn_close = $('<button type="button" class="btn btn-default">Close</button>').data("value", true);
			$modal_footer.append($btn_close);

			$btn_close.click(function() {
				_ret = $(this).data("value");
				$modal.modal('hide');
			});
		}

		// show dialog with options
		$modal.modal(_options);

		// move modal-backdrop to top
		var $back = $("body div.modal-backdrop:last");
		$back.css("z-index", $._bc.vals.dialog.z_index);
		$modal.css("z-index", $._bc.vals.dialog.z_index+1);
		$._bc.vals.dialog.z_index += 2;

		// Fix dialog position if exist multi-modal
		$back.one('bsTransitionEnd', function() {
			if($(".modal.in").length > 1) {
				$modal.css("padding-right", $("body").css("padding-right"));
			}
		});

		// begin hide window, return callback
		$modal.on('hide.bs.modal', function () {
			if(_callback != null) {
				return _callback.call($modal, _ret);
			}
		});

		// when show hidden, remove it
		$modal.on('hidden.bs.modal', function () {
			$(this).remove();
			$._bc.vals.dialog.z_index -= 2;

			if($(".modal-backdrop").length) {
				$("body").addClass("modal-open");
			}
		});

		return $modal;
	}
});
/* options:
	data-editable:						enable element to be editable
							html		support html edit
*/

!function ($) {
	var _input = null;
	var _target = null;
	var _preContent = null;
	var preventPopup = false;

	// Update element content
	function updateElement() {
		// Update content
		if(_input != null && _target != null) {
			var _html = _target.attr("data-editable") == "html";
			var content = _input.val();
			if(_html)
				_target.html(content);
			else
				_target.text(content);

			if(_preContent != content) {
				_target.change();
			}
		}

		// Clean element & input
		if(_target != null) _target.show();
		_target = null;
		if(_input != null) _input.remove();
		_input = null;
	}

	// Create input for editable element
	$(document).on("click.bs.editable", "[data-editable]", function() {
		var _my = $(this);
		var _html = _my.attr("data-editable") == "html";
		var $input = _my.is("pre") ? $("<textarea>") : $("<input type='text'>");

		// Cleanup pre content
		updateElement();

		// Set input content
		var content = _html ? _my.html() : _my.text();
		$input.val(content);

		// Update CSS of input
		_my.after($input);
		$input.addClass("form-control")
		.css("margin", _my.css("margin"))
		.css("padding", _my.css("padding"))
		.css("font-size", _my.css("font-size"))
		.css("font-family", _my.css("font-family"))
		.css("line-height",  _my.css("line-height"))
		.css("display", _my.css("display"))
		.css("min-width", _my.css("width"))
		.css("height", "auto").css("width", "auto")
		.select().focus();

		// Prevent input click event
		$input.click(function() {
			preventPopup = true;
		}).keydown(function(event) {
			if(event.which == 13) {
				updateElement();
			}
		});

		_my.hide();
		preventPopup = true;

		_input = $input;
		_target = _my;
		_preContent = content;
	});

	// Remove input
	$(document).on("click.bs.removeEditable", function() {
		if(preventPopup == true) {
			preventPopup = false;
			return;
		}
		updateElement();
	});
}(window.jQuery);/*	this is to help hightlight target element with dark background.
options:
	title:			string				specify title of notification.
	content:		element				specify content of notification.
	position:		string				"left", "right", "top", "bottom". You can mix then up as "left,top" (it's same as "top,left")
	type:			string				"normal", "warning", "info", "danger", "success"
	timeout:		number				default: 4000, hiden speed.
	overtimeout:	number				default: 1000, hiden speed after mouse move out.
	queuetimeout:	number				default: 1000, hiden speed interval between 2 notification in same the region.
	region:			string				default: "", saming region notification will not keep out other notification in same region.

callback:			[function]			It will trigger when notification created.

return:				[element]			return notification element
*/

// init env
$._bc.vals.notify = new Object();
$._bc.vals.notify.region = new Object();	// record by region

// init function
$.extend({
	notify:function(options, callback){
		// get options
		var vars = $._bc.vars(options, callback);
		var _options = vars.options;
		var _callback = vars.callback;

		var _title = $._bc.get(_options, "title", "Notification");
		var _content = $._bc.get(_options, "content", "");
		var _position = $._bc.get(_options, "position", "right,top");
		var _type = $._bc.get(_options, "type", "normal");
		var _timeout = $._bc.get(_options, "timeout", 4000);
		var _overtimeout = $._bc.get(_options, "overtimeout", 1000);
		var _queuetimeout = $._bc.get(_options, "queuetimeout", 1000);
		var _region = $._bc.get(_options, "region", "");
		var _list = null;

		var _tt = null;

		var $notification = $("<div class='alert notification-body'>");
		var $btn = $("<button type='button' class='close'>x</button>");

		// get the notification list for region
		if(_region != "") {
			$notification.attr("data-region", _region);
			_list = $._bc.vals.notify.region[_region];
			if(_list == null) {
				_list = $._bc.list();
				$._bc.vals.notify.region[_region] = _list;
			}
			_list.add($notification);
		}

		// alert position
		if(_position.indexOf("left") != -1) $notification.addClass("left");
		if(_position.indexOf("right") != -1) $notification.addClass("right");
		if(_position.indexOf("top") != -1) $notification.addClass("top");
		if(_position.indexOf("bottom") != -1) $notification.addClass("bottom");

		// alert color
		if(_type == "normal") $notification.addClass("alert-normal");
		if(_type == "warning") $notification.addClass("alert-warning");
		if(_type == "danger") $notification.addClass("alert-danger");
		if(_type == "success") $notification.addClass("alert-success");
		if(_type == "info") $notification.addClass("alert-info");

		$notification.append($btn);

		if(_title == "") _title = "&nbsp;";
		if(typeof(_title) == 'string') _title = "<h5>" + _title + "</h5>";
		$notification.append(_title);
		if(_content != "") $notification.append(_content);

		// append notification
		$("body").append($notification);

		// fade in
		$notification.hide();
		$notification.fadeIn();

		// fade out
		function close() {
			if(_list != null) _list.remove($notification);
			$notification.fadeOut(function(){
				$notification.remove();
			});
		}

		$btn.click(function(){
			close();
			refreshAllRelated();
		});

		// auto fade out
		$notification.stopAutoFadeOut = function() {
			window.clearTimeout(_tt);
		}
		$notification.setAutoFadeOut = function(_delay) {
			if(_timeout > 0) {
				var _inner_delay = _delay == null ? _timeout : _delay;

				$notification.stopAutoFadeOut();
				_tt = window.setTimeout(function(){
					close();
				}, _inner_delay);
			}
		}

		// refresh timeout if is hover
		if(_timeout > 0) {
			$notification.mouseenter(function(){
				if(_list == null) {
					$notification.stopAutoFadeOut();
				} else {
					for(var i = _list.length - 1 ; i >= 0  ; i--) {
						var $element = _list[i];
						$element.stopAutoFadeOut();
					}
				}
			});
			$notification.mouseleave(function(){
				refreshAllRelated(_overtimeout);
			});
		}

		// deal with notifications in same region
		function refreshAllRelated(_delay) {
			var _inner_delay = _delay == null ? _timeout : _delay;

			if(_list == null) {
				$notification.setAutoFadeOut(_inner_delay);
			} else {
				var _istop = _position.indexOf("top") != -1;
				var _isbottom = _position.indexOf("bottom") != -1;

				var _offset = 0;
				for(var i = _list.length - 1 ; i >= 0  ; i--) {
					var $element = _list[i];

					// move element
					if(_istop) {
						$element.animate({	top: _offset,},{queue: false});
					} else if(_isbottom) {
						$element.animate({	bottom: _offset,},{queue: false});
					}
					var _marginTop = parseInt($element.css("margin-top").replace("px",""), 10);
					_offset += $element.outerHeight() + _marginTop;

					// set timeout
					$element.setAutoFadeOut(_inner_delay + i * _queuetimeout);
				}
			}
		}
		refreshAllRelated();

		// call the callback
		if(_callback != null) {
			_callback.call($notification);
		}

		return $notification;
	}
});/* options:
	to:			element			set the value of target element
*/

!function ($) {
	$.fn.extend({
		radio:function(options){
			// get options
			var _my = $(this);
			var vars = $._bc.vars(options);
			var _options = vars.options;
			var _checked = $._bc.get(_options, "checked", null);
			var _to = $._bc.get(_options, "to", null);
			var _val = $._bc.get(_options, "value", null);

			// set target element
			if(_to != null) {
				_my.attr("data-to", _to);
			}

			// set radio value
			if(_val != null) {
				_my.attr("data-value", _val);
			}

			// set the value of radio and it will change target element too.
			if(_checked === true) {
				checkRadio(_my);
			} else if(_checked === false) {
				uncheckRadio(_my);
			}
		}
	});
	// change radios status
	function checkRadio(_instance) {
		if(_instance.attr("checked") != null) return;

		updateTarget(_instance, true);
	}
	// remove radios status
	function uncheckRadio(_instance) {
		if(_instance.attr("checked") == null) return;

		updateTarget(_instance, false);
	}
	// update target input
	function updateTarget(_instance, checked) {
		var _val = _instance.attr("data-value");

		// update all the radios
		var _name = _instance.attr("name");
		var _radios = $(".radio[data-toggle='radio'][name='" + _name + "']");
		_radios.removeAttr("checked");

		// if checked, update radio check status
		if(checked) {
			_instance.attr("checked", "checked");
		} else {
			_val = "";
		}

		// update target element
		var _target = $(_instance.attr("data-to"));
		var _pre_val = _target.val();
		_target.val(_val);

		// change event
		_instance.change();
		if(_pre_val != _val) {
			_target.change();
		}
	}

	// click radio
	$(document).on("click.bs.radio", ".radio[data-toggle='radio']", function(event){
		var _my = $(this);
		checkRadio(_my);
	});

	// click label who contains radio
	$(document).on("click.bs.radio", "label", function(event){
		var _label = $(this);
		var _my = _label.find(".radio[data-toggle='radio']");
		if(_my.length != 0) {			// find checkbox to go on
			var _disabled = _my.attr("disabled") != null;
			if(!_disabled) checkRadio(_my);
		}
	});
}(window.jQuery);/* options:
	to:			element			set the value of target element
*/

!function ($) {
	$(document).on("click.bs.select", "ul.dropdown-menu[role='menu'][data-type='selector'] li a", function(event){
		var my = $(this);
		var _val = my.attr("value");
		var _text = my.text();
		if(_val == null) {
			_val = _text;
		}
		var $field = $(this).closest(".dropdown, .btn-group").find("[data-toggle='dropdown'][data-type='selector']");
		var $field_val = $field.find("[data-value]");
		var $field_target = $($field.attr("data-to"));
		var pre_val = $field_val.attr("data-value");
		var pre_tgt_val = $field_target.val();
		$field_val.val(_val).text(_text).attr("data-value", _val);
		$field_target.val(_val);
		
		if(pre_val != _val) {
			my.change();
		}
		if(pre_tgt_val != _val) {
			$field_target.change();
		}
	});
}(window.jQuery);/* options:
	to:			element			set the value of target element

	min:		number			set min value
	max:		number			set max value

	number:		number			set the number of the slider blocks
	value:		array			set initial value of sliders. If set value without number, it will trade length of them as the number.
	single:		boolean			default false. Move slider will not influence other sliders if true.
	mixed:		boolean			default false. Slider can move every where with out order if true and single will always be true in this mode.
*/

!function ($) {
	$.fn.extend({
		slider:function(options){
			if(options == null) options = {number: 1};
			$(this).each(function() {
				// get options
				var _my = $(this);
				var vars = $._bc.vars(options);
				var _options = vars.options;

				// set min / max value
				var _min = $._bc.get(_options, "min", 0);
				var _max = $._bc.get(_options, "max", 100);
				_my.attr("data-min", _min);
				_my.attr("data-max", _max);

				// get values
				var _values = $._bc.get(_options, "value", []);
				var _single = $._bc.get(_options, "single", false);
				var _mixed = $._bc.get(_options, "mixed", false);

				// set number
				var _number = _options.number;
				if(_number == null && _values.length != 0) {
					_number = _values.length;
				}
				// create sliders with given number
				if(_number != null) {
					_my.empty();
					for(var i = 0 ; i < _number ; i += 1) {
						var $slider = $("<button type='button' class='btn btn-primary slider' data-toggle='slider'>");
						_my.append($slider);
					}
				}

				// mark as enhanced slider
				_my.attr("data-slider-container", "");
				if(_single) _my.attr("data-slider-single", "");
				if(_mixed) _my.attr("data-slider-mixed", "");

				// create the background between sliders
				var _sliders = _my.find("button[data-toggle='slider']");
				{
					var _len = _sliders.length;
					for(var i = _len - 1 ; i > 0  ; i -= 1) {
						var $bac = $("<div data-toggle='slider-background'>");
						$bac.attr("data-from", i - 1);
						$bac.attr("data-to", i);
						_my.prepend($bac);
					}
				}

				// set default value & set margin-left as left
				{
					var _len = _values.length;
					var _default = _len == 0 ? _min : _values[_len - 1];
					$.each(_sliders, function(i, ele) {
						var _element = $(ele);
						var _val = i < _len ? _values[i] : _default;
						setValue(_element, _val);

						var _style = _element.attr("style");
						if(_style != null) {
							_element.attr("style", _style.replace(/margin-left/g, "left"));
						}
					});
				}

				// refresh background for user draw color
				refreshBackfround(_my);
			});
		}
	});

	// dynamic controller without handler
	function index(element) {
		return element.parent().find("button[data-toggle='slider']").index(element);
	}
	function getLeft(element) {
		var _process = element.parent();
		if(_process.attr("data-slider-container") == null) {
			return Number(element.css("margin-left").replace("px", ""));
		} else {
			return Number(element.css("left").replace("px", ""));
		}
	}
	function getWidth(element) {
		var _width = element.outerWidth();
		if(element.parent().attr("data-slider-container") != null) {
			var _mgnLeft = Number(element.css("margin-left").replace("px", ""));
			_width += _mgnLeft * 2;
		}
		return _width;
		
	}

	var _instance = null;
	var _mouseLeft = 0;
	function getValue(_instance) {
		// limit the range of min & max value
		var _min = Number(_instance.parent().attr("data-min"));
		var _max = Number(_instance.parent().attr("data-max"));
		if(_min == null || isNaN(_min)) _min = 0;
		if(_max == null || isNaN(_max)) _max = 100;

		// prepare value condition
		var _process = _instance.parent();
		var _sliders = _process.find("button[data-toggle='slider']");
		var _len = _sliders.length;
		var _index = index(_instance);
		var _mixed = _process.attr("data-slider-mixed") != null;
		var _value = 0;
		var _total_width = _process.outerWidth() - 1;
		if(_mixed) {
			_total_width -= getWidth(_instance);
		} else {
			$.each(_sliders, function(i, ele) {
				var _element = $(ele);
				_total_width -= getWidth(_element);
			});
		}

		if(_process.attr("data-slider-container") == null) {
			for(var i = 0 ; i <= _index ; i += 1) {
				var _element = $(_sliders[i]);
				var _left = getLeft(_element);
				_value += _left;
			}
		} else {
			_value = getLeft(_instance);
			if(!_mixed) {
				for(var i = 0 ; i < _index ; i += 1) {
					var _element = $(_sliders[i]);
					_value -= getWidth(_element);
				}
			}
		}

		// generate value
		var _ptg = _value / _total_width;
		var _val = _min + (_max - _min) * _ptg;
		return _val;
	}
	function setValue(_instance, value) {
		var _process = _instance.parent();

		// limit the range of min & max value
		var _min = Number(_process.attr("data-min"));
		var _max = Number(_process.attr("data-max"));
		if(_min == null || isNaN(_min)) _min = 0;
		if(_max == null || isNaN(_max)) _max = 100;

		// prepare value condition
		var _sliders = _process.find("button[data-toggle='slider']");
		var _index = index(_instance);
		var _total_width = _process.outerWidth();
		$.each(_sliders, function(i, ele) {
			var _element = $(ele);
			_total_width -= getWidth(_element);
		});
		var _my_left = _total_width * (value - _min) / (_max - _min);
		for(var i = 0 ; i < _index ; i += 1) {
			var _element = $(_sliders[i]);
			_my_left += getWidth(_element);
		}
		if(_process.attr("data-slider-container") == null) {
			_instance.css("margin-left", _my_left);
		} else {
			_instance.css("left", _my_left);
		}
		_instance.val(value).attr("data-value", value);
	}
	function doStart(_instance, event) {
		_mouseLeft = event.pageX - _instance.offset().left;

		var _process = _instance.parent();
		if(_process.attr("data-slider-container") == null) {
			var _sliders = _process.find("button[data-toggle='slider']");
			var _index = index(_instance);
			for(var i = 0 ; i < _index ; i += 1) {
				var _element = $(_sliders[i]);
				_mouseLeft += getWidth(_element) + getLeft(_element);
			}
		}
	}
	function doMove(_instance, event, recv) {
		// calculate enable range
		var _process = _instance.parent();
		var _sliders = _process.find("button[data-toggle='slider']");
		var _len = _sliders.length;
		var _index = index(_instance);
		var _total_width = _process.outerWidth() - 1;
		if(_process.attr("data-slider-container") == null) {	// run as simple mode
			$.each(_sliders, function(i, ele) {
				var _element = $(ele);
				_total_width -= getWidth(_element);
			});
			var _value_range =_total_width;
			for(var i = 0 ; i < _index ; i += 1) {
				var _element = $(_sliders[i]);
				_value_range -= getLeft(_element);
			}
			for(var i = _index + 1 ; i < _len ; i += 1) {
				var _element = $(_sliders[i]);
				_value_range -= getLeft(_element);
			}

			var _instance_left = event.pageX - _process.offset().left - _mouseLeft;
			if(_instance_left < 0) _instance_left = 0;
			if(_instance_left > _value_range) _instance_left = _value_range;
			_instance.css("margin-left", _instance_left);
		} else {												// run as enhance mode
			var _mixed = _process.attr("data-slider-mixed") != null;
			var _single = _mixed || _process.attr("data-slider-single") != null;
			var _pre_left = getLeft(_instance);

			// move slider
			var _value_start = 0;
			var _value_end =_total_width - getWidth(_instance);

			if(!_mixed) {
				if(_index > 0) {// start
					var _prev = $(_sliders[_index - 1]);
					_value_start = getLeft(_prev) + getWidth(_prev);
				}
				if(_index < _len - 1) {
					if(_single) {
						var _next = $(_sliders[_index + 1]);
						_value_end = getLeft(_next) - getWidth(_instance);
					} else {
						var _next = $(_sliders[_index + 1]);
						var _last = $(_sliders[_len - 1]);
						_value_end = _total_width - (getLeft(_last) - getLeft(_instance)) - getWidth(_last);
					}
				}
			}

			var _instance_left = event.pageX - _process.offset().left - _mouseLeft;
			if(_instance_left < _value_start) _instance_left = _value_start;
			if(_instance_left > _value_end) _instance_left = _value_end;
			_instance.css("left", _instance_left);

			// move other after sliders
			if(!_single) {
				var _dis = _instance_left - _pre_left;
				for(var i = _index + 1 ; i < _len ; i += 1) {
					var _element = $(_sliders[i]);
					_element.css("left", (getLeft(_element) + _dis));
				}
			}
		}
	}
	function doChange(_instance) {
		var _target = $(_instance.attr("data-to"));
		var _pre_val = Number(_target.val());
		var _val = getValue(_instance);
		_instance.val(_val).attr("data-value", _val);
		_target.val(_val);
		if(_pre_val != _val) {
			_target.change();
		}
	}
	function refreshBackfround(_instance) {
		var _process = $(_instance);
		if(_process.attr("data-slider-container") == null) {
			_process = _process.parent();
		}
		var _sliders = _process.find("button[data-toggle='slider']");
		var _lenSliders = _sliders.length;
		var _backgrounds = _process.find("div[data-toggle='slider-background']");
		var _lenBackgrounds = _backgrounds.length;
		for(var i = 0 ; i < _lenBackgrounds ; i += 1) {
			var _bac = $(_backgrounds[i]);
			var _prev = $(_sliders[i]);
			var _next = $(_sliders[i + 1]);
			var _left = getLeft(_prev) + getWidth(_prev) * 0.5;
			var _right = getLeft(_next) + getWidth(_next) * 0.5;
			var _width = _right - _left;

			_bac.css("left", _left);
			_bac.outerWidth(_width);
		}
	}
	$(document).on("mousedown.bs.slider", "button[data-toggle='slider']", function(event){
		if(event.button == 0) {
			_instance = $(this);
			doStart(_instance, event);
		}
	});
	$(document).on("mousemove.bs.slider", function(event){
		if(_instance != null) {
			doMove(_instance, event);
			doChange(_instance);

			// loop trigger change event
			var _process = _instance.parent();
			var _sliders = _process.find("button[data-toggle='slider']");
			var _len = _sliders.length;
			var _index = index(_instance);
			$.each(_sliders, function(i, element) {
				if(i != _index) {
					doChange($(element));
				}
			});

			// refresh background for user draw color
			refreshBackfround(_instance);
		}
	});
	$(document).on("mouseup.bs.slider", function(event){
		if(event.button == 0) {
			_instance = null;
		}
	});
}(window.jQuery);!function ($) {
	$.fn.extend({
		tree: function(data, options){
			var _my = $(this);
			options = options || {};

			var _name = data.name;
			var _list = data.list || [];
			var _enabled = data.enabled !== false;
			var _open = _enabled ? data.open !== false : data.open === true;

			var $ul = _my.is("ul") ? _my : $("<ul class='treeView'>").appendTo(_my);
			var $li = $("<li>").appendTo($ul);
			var $a = $("<a class='tree-icon glyphicon'>").appendTo($li);
			var $name = (data.url ? $("<a>").attr("href", data.url) : $("<span>")).html(_name).insertAfter($a);

			if(!_enabled) $li.addClass("disabled");

			// Generate as list
			if(_list.length) {
				var cls_folder_open = options.folderOpenIcon || 'glyphicon-folder-close';
				var cls_folder_close = options.folderCloseIcon || 'glyphicon-folder-open';

				$a.attr("data-toggle", "tree")
				.attr("data-icon-open", cls_folder_open)
				.attr("data-icon-close", cls_folder_close)
				.addClass("glyphicon")
				.addClass(_open ? cls_folder_close : cls_folder_open);

				var $sub_ul = $("<ul class='tree-list'>").appendTo($li);
				$.each(_list, function(i, data) {
					$sub_ul.tree(data, options);
				});
				if(!_open) {
					$sub_ul.hide();
				}
			} else {
				var cls_file = options.itemIcon || 'glyphicon-file';

				$a.addClass(cls_file);
			}
			return _my;
		},
	});

	$(document).on("click.bs.treeView", "[data-toggle='tree']", function(event) {
		event.preventDefault();

		var _my = $(this);
		if(_my.closest("li").hasClass("disabled")) return;

		var $list = _my.parent().find("> .tree-list");
		var clsOpen = _my.attr("data-icon-open");
		var clsClose = _my.attr("data-icon-close");

		if($list.is(":hidden")) {
			_my.removeClass(clsOpen);
			_my.addClass(clsClose);
			$list.slideDown();
		} else {
			_my.addClass(clsOpen);
			_my.removeClass(clsClose);
			$list.slideUp();
		}
	});
}(window.jQuery);