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

	serviceModule.service('Time', function() {
		var timeSrv = function (time) {
			if(arguments.length === 1 && time === undefined) {
				return null;
			}

			// Parse string number
			if(typeof time === "string" && !isNaN(+time)) {
				time = +time;
			}

			var _mom = new moment(time);
			_mom.utcOffset(timeSrv.UTC_OFFSET);
			return _mom;
		};

		// TODO: time zone
		timeSrv.UTC_OFFSET = 0;

		timeSrv.FORMAT = "YYYY-MM-DD HH:mm:ss";

		timeSrv.format = function (time, format) {
			time = timeSrv(time);
			return time ? time.format(format || timeSrv.FORMAT) : "-";
		};

		timeSrv.diff = function (from, to) {
			from = timeSrv(from);
			to = timeSrv(to);
			if (!from || !to) return null;
			return to.diff(from);
		};

		timeSrv.diffStr = function (from, to) {
			var diff = from;
			if(arguments.length === 2) {
				diff = timeSrv.diff(from, to);
			}
			if(diff === null) return "-";

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

		return timeSrv;
	});
})();
