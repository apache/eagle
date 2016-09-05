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

	var UNITS = [
		["days", "day", "day"],
		["hours", "hr", "hr"],
		["minutes", "min", "min"],
		["seconds", "s", "s"]
	];

	var serviceModule = angular.module('eagle.service');

	serviceModule.service('Time', function($q, $wrapState) {
		var startTime, endTime;
		var reloadListenerList = [];

		var Time = function (time) {
			var _mom;

			if(arguments.length === 1 && time === undefined) {
				return null;
			}

			switch (time) {
				case "startTime":
					return startTime;
					break;
				case "endTime":
					return endTime;
					break;
				case "day":
					_mom = new moment();
					_mom.utcOffset(Time.UTC_OFFSET);
					_mom.hours(0).minutes(0).seconds(0);
					break;
				default:
					// Parse string number
					if(typeof time === "string") {
						if(!isNaN(+time)) {
							time = +time;
						} else {
							time = new moment(time);
							time.add(time.utcOffset(), "minutes");
						}
					}

					_mom = new moment(time);
					_mom.utcOffset(Time.UTC_OFFSET);
			}
			return _mom;
		};

		Time.TIME_RANGE_PICKER = "timeRange";
		Time.pickerType = null;

		// TODO: time zone
		Time.UTC_OFFSET = 0;

		Time.FORMAT = "YYYY-MM-DD HH:mm:ss";
		Time.SHORT_FORMAT = "MM-DD HH:mm";

		Time.format = function (time, format) {
			time = Time(time);
			return time ? time.format(format || Time.FORMAT) : "-";
		};

		Time.startTime = function () {
			return startTime;
		};

		Time.endTime = function () {
			return endTime;
		};

		Time.timeRange = function (startTimeValue, endTimeValue) {
			startTime = Time(startTimeValue);
			endTime = Time(endTimeValue);

			$.each(reloadListenerList, function (i, listener) {
				listener(Time);
			});
		};

		Time.onReload = function (func) {
			reloadListenerList.push(func);
		};

		Time.offReload = function (func) {
			reloadListenerList = $.grep(reloadListenerList, function(_func) {
				return _func !== func;
			});
		};

		Time.verifyTime = function(str, format) {
			format = format || Time.FORMAT;
			var date = Time(str);
			if(str === Time.format(date, format)) {
				return date;
			}
			return null;
		};

		Time.diff = function (from, to) {
			from = Time(from);
			to = Time(to);
			if (!from || !to) return null;
			return to.diff(from);
		};

		Time.diffStr = function (from, to) {
			var diff = from;
			if(arguments.length === 2) {
				diff = Time.diff(from, to);
			}
			if(diff === null) return "-";
			if(diff === 0) return "0s";

			var match = false;
			var rows = [];
			var duration = moment.duration(diff);
			var rest = 3;

			$.each(UNITS, function (i, unit) {
				var interval = Math.floor(duration[unit[0]]());
				if(interval > 0) match = true;

				if(match) {
					if(interval !== 0) {
						rows.push(interval + (interval > 1 ? unit[1] : unit[2]));
					}

					rest -=1;
					if(rest === 0) return false;
				}
			});

			return rows.join(", ");
		};

		Time.align = function (time, interval, ceil) {
			time = Time(time);
			if(!time) return null;

			var func = ceil ? Math.ceil : Math.floor;

			var timestamp = time.valueOf();
			return Time(func(timestamp / interval) * interval);
		};

		Time.millionFormat = function (num) {
			if(!num) return "-";
			num = Math.floor(num / 1000);
			var s = num % 60;
			num = Math.floor(num / 60);
			var m = num % 60;
			num = Math.floor(num / 60);
			var h = num % 60;
			return common.string.preFill(h, "0") + ":" +
				common.string.preFill(m, "0") + ":" +
				common.string.preFill(s, "0");
		};

		var promiseLock = false;
		Time.getPromise = function (config, state, param) {
			if(config.time === true) {
				Time.pickerType = Time.TIME_RANGE_PICKER;

				if(!promiseLock) {
					startTime = Time.verifyTime(param.startTime);
					endTime = Time.verifyTime(param.endTime);

					if (!startTime || !endTime) {
						endTime = Time();
						startTime = endTime.clone().subtract(2, "hour");

						setTimeout(function () {
							promiseLock = true;
							$wrapState.go(state.name, $.extend({}, param, {
								startTime: Time.format(startTime),
								endTime: Time.format(endTime)
							}), {location: "replace", notify: false});

							setTimeout(function () {
								promiseLock = false;
							}, 100);
						}, 50);
					}
				}
			} else {
				Time.pickerType = null;
			}

			return $q.when(Time);
		};

		return Time;
	});
})();
