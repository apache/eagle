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

	var ALERT_FETCH_LIMIT = 1000 * 60 * 60 * 12;
	var ALERT_REFRESH_INTERVAL = 1000 * 10;
	var ALERT_TEMPLATE = '#/site/${siteId}/alert/detail/${alertId}?timestamp=${timestamp}';

	var serviceModule = angular.module('eagle.service');

	serviceModule.service('Alert', function ($notification, Time, CompatibleEntity) {
		var Alert = {
			list: null,
		};

		$notification.getPromise().then(function () {
			function queryAlerts() {
				var endTime = new Time();
				var list = CompatibleEntity.query("LIST", {
					query: "AlertService",
					startTime: endTime.clone().subtract(ALERT_FETCH_LIMIT, 'ms'),
					endTime: endTime
				});
				list._then(function () {
					if (!Alert.list) {
						Alert.list = list;
						return;
					}

					var subList = common.array.minus(list, Alert.list, ['encodedRowkey'], ['encodedRowkey']);
					Alert.list = list;
					$.each(subList, function (i, alert) {
						$notification(alert.alertSubject, common.template(ALERT_TEMPLATE, {
							siteId: alert.tags.siteId,
							alertId: alert.tags.alertId,
							timestamp: alert.timestamp,
						}));
					});
				});
			}

			queryAlerts();
			setInterval(queryAlerts, ALERT_REFRESH_INTERVAL);
		});

		return Alert;
	});
})();
