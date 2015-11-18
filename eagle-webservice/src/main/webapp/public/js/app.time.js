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

"use strict";

// Time Zone
(function() {
	app.time = {
		UTC_OFFSET: 0,
		now: function() {
			return app.time.offset();
		},
		offset: function(time) {
			// Parse string number
			if(typeof time === "string" && !isNaN(+time)) {
				time = +time;
			}

			var _mom = new moment(time);
			_mom.utcOffset(app.time.UTC_OFFSET);
			return _mom;
		},
		/*
		 * Return the moment object which use server time zone and keep the time.
		 */
		srvZone: function(time) {
			var _timezone = time._isAMomentObject ? time.utcOffset() : new moment().utcOffset();
			var _mom = app.time.offset(time);
			_mom.subtract(app.time.UTC_OFFSET, "m").add(_timezone, "m");
			return _mom;
		},

		refreshInterval: 1000 * 10
	};

	// Moment update
	moment.fn.toISO = function() {
		return this.format("YYYY-MM-DDTHH:mm:ss.000Z");
	};
})();