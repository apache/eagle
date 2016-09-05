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
		var Time = function (time) {
			var _mom;

			if(arguments.length === 1 && time === undefined) {
				return null;
			}

			switch (time) {
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

		Time.format = function (time, format) {
			time = Time(time);
			return time ? time.format(format || Time.FORMAT) : "-";
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

		Time.getPromise = function (config) {
			var deferred = $q.defer();
			var startTime, endTime;

			console.warn("Need Time Promise~");

			if(config.time === true && false) {
				console.log(">>>>>>>>>>>>", $wrapState, $wrapState.param.startTime, $wrapState.param.endTime);
				Time.pickerType = Time.TIME_RANGE_PICKER;
				startTime = Time.verifyTime($wrapState.param.startTime);
				endTime = Time.verifyTime($wrapState.param.endTime);
				if (!startTime || !endTime) {
					endTime = Time();
					startTime = endTime.clone().subtract(2, "hour");

					console.log("[[[[[[[[[[[[[[[[ Do It!!!");

					//setTimeout(function () {
						$wrapState.go("jpmOverview", {
							startTime: Time.format(startTime),
							endTime: Time.format(endTime)
						}, {location: "replace"});
					//}, 100);

					return $q.reject(Time);
				}
			} else {
				Time.pickerType = null;
			}

			deferred.resolve(Time);
			return deferred.promise;
		};

		return Time;
	});
})();
