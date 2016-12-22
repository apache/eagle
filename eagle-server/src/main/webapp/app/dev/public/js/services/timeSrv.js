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

	var autoGenerateTime = false;
	var keepTime = false;
	var serviceModule = angular.module('eagle.service');

	serviceModule.service('Time', function($q, $wrapState, Server) {
		var startTime, endTime;
		var reloadListenerList = [];

		var $Time = function (time) {
			var _mom;

			if(arguments.length === 1 && time === undefined) {
				return null;
			}

			switch (time) {
				case "startTime":
					return startTime;
				case "endTime":
					return endTime;
				case "month":
					_mom = new moment();
					_mom.utcOffset($Time.UTC_OFFSET);
					_mom.date(1).hours(0).minutes(0).seconds(0).millisecond(0);
					break;
				case "monthEnd":
					_mom = $Time("month").add(1, "month").subtract(1, "s");
					break;
				case "week":
					_mom = new moment();
					_mom.utcOffset($Time.UTC_OFFSET);
					_mom.weekday(0).hours(0).minutes(0).seconds(0).millisecond(0);
					break;
				case "weekEnd":
					_mom = $Time("week").add(7, "d").subtract(1, "s");
					break;
				case "day":
					_mom = new moment();
					_mom.utcOffset($Time.UTC_OFFSET);
					_mom.hours(0).minutes(0).seconds(0).millisecond(0);
					break;
				case "dayEnd":
					_mom = $Time("day").add(1, "d").subtract(1, "s");
					break;
				case "hour":
					_mom = new moment();
					_mom.utcOffset($Time.UTC_OFFSET);
					_mom.minutes(0).seconds(0).millisecond(0);
					break;
				case "hourEnd":
					_mom = $Time("hour").add(1, "h").subtract(1, "s");
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
					_mom.utcOffset($Time.UTC_OFFSET);
			}
			return _mom;
		};

		$Time.TIME_RANGE_PICKER = "timeRange";
		$Time.pickerType = null;
		$Time.autoRefresh = false;
		$Time._reloadListenerList = reloadListenerList;

		// TODO: time zone
		$Time.UTC_OFFSET = 0;

		$Time.FORMAT = "YYYY-MM-DD HH:mm:ss";
		$Time.SHORT_FORMAT = "MM-DD HH:mm";

		// UTC sync
		Server.getPromise().then(function () {
			var timezone = Server.config.service.timezone || "";
			try {
				var match = timezone.match(/^UTC([+-]\d+)?$/);
				if (match) {
					$Time.UTC_OFFSET = Number(match[1] || 0) * 60;
				} else {
					console.warn('Timezone parse failed:', timezone);
				}
			} catch (err) {
				console.error('Timezone not support:', timezone, err);
			}
		});

		$Time.format = function (time, format) {
			time = $Time(time);
			return time ? time.format(format || $Time.FORMAT) : "-";
		};

		$Time.startTime = function () {
			return startTime;
		};

		$Time.endTime = function () {
			return endTime;
		};

		$Time.timeRange = function (startTimeValue, endTimeValue) {
			startTime = $Time(startTimeValue);
			endTime = $Time(endTimeValue);

			keepTime = true;
			$wrapState.go(".", $.extend({}, $wrapState.param, {
				startTime: $Time.format(startTime),
				endTime: $Time.format(endTime)
			}), {notify: false});

			$.each(reloadListenerList, function (i, listener) {
				listener($Time);
			});
		};

		$Time.onReload = function (func, $scope) {
			reloadListenerList.push(func);

			// Clean up
			if($scope) {
				$scope.$on('$destroy', function() {
					$Time.offReload(func);
				});
			}
		};

		$Time.offReload = function (func) {
			reloadListenerList = $.grep(reloadListenerList, function(_func) {
				return _func !== func;
			});
		};

		$Time.verifyTime = function(str, format) {
			format = format || $Time.FORMAT;
			var date = $Time(str);
			if(str === $Time.format(date, format)) {
				return date;
			}
			return null;
		};

		$Time.diff = function (from, to) {
			from = $Time(from);
			to = $Time(to);
			if (!from || !to) return null;
			return to.diff(from);
		};

		$Time.diffStr = function (from, to) {
			var diff = from;
			if(arguments.length === 2) {
				diff = $Time.diff(from, to);
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

		$Time.diffInterval = function (from, to) {
			var timeDiff = $Time.diff(from, to);
			if(timeDiff <= 1000 * 60 * 60 * 6) {
				return 1000 * 60 * 5;
			} else if(timeDiff <= 1000 * 60 * 60 * 24) {
				return 1000 * 60 * 15;
			} else if(timeDiff <= 1000 * 60 * 60 * 24 * 7) {
				return 1000 * 60 * 30;
			} else if(timeDiff <= 1000 * 60 * 60 * 24 * 14) {
				return 1000 * 60 * 60;
			} else {
				return 1000 * 60 * 60 * 24;
			}
		};

		$Time.align = function (time, interval, ceil) {
			time = $Time(time);
			if(!time) return null;

			var func = ceil ? Math.ceil : Math.floor;

			var timestamp = time.valueOf();
			return $Time(func(timestamp / interval) * interval);
		};

		$Time.millionFormat = function (num) {
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

		$Time.getPromise = function (config, state, param) {
			var deferred = $q.defer();
			var timeConfig = typeof config.time === true ? {} : config.time;

			Server.getPromise().then(function () {
				// Ignore time update if customise time set.
				if(keepTime) {
					// Update auto refresh mark if time is generated from promise
					if(autoGenerateTime && timeConfig) {
						$Time.autoRefresh = timeConfig.autoRefresh;
					}

					autoGenerateTime = false;
					keepTime = false;
					deferred.resolve($Time);
				} else {
					if (timeConfig) {
						$Time.pickerType = timeConfig.type || $Time.TIME_RANGE_PICKER;

						startTime = $Time.verifyTime(param.startTime);
						endTime = $Time.verifyTime(param.endTime);

						if (!startTime || !endTime) {
							endTime = $Time();
							startTime = endTime.clone().subtract(2, "hour");

							autoGenerateTime = true;
							keepTime = true;
							$Time._innerSearch = {
								startTime: $Time.format(startTime),
								endTime: $Time.format(endTime)
							};
						}
					} else {
						$Time._innerSearch = null;
						$Time.pickerType = null;
						$Time.autoRefresh = false;
					}
					deferred.resolve($Time);
				}
			});

			return deferred.promise;
		};

		// Interval update
		setInterval(function () {
			if(!$Time.autoRefresh) return;

			var interval = $Time.diff($Time('startTime'), $Time('endTime'));
			var endTime = $Time();
			var startTime = endTime.clone().subtract(interval, "ms");
			$Time.timeRange(startTime, endTime);
		}, 1000 * 60);

		if(window.sessionStorage) {
			$Time.autoRefresh = sessionStorage.getItem("timeAutoRefresh") === "true";
		}

		$(window).unload(function(){
			if(window.sessionStorage) {
				sessionStorage.setItem("timeAutoRefresh", $Time.autoRefresh);
			}
		});

		return $Time;
	});
})();
