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

	var REFRESH_TIME_LIMIT = 5 * 1000;
	var PREEMPTIVE_TIME_LIMIT = 8 * 1000;
	var serviceModule = angular.module('eagle.service');

	serviceModule.service('Notification', function ($rootScope) {
		var promised = false;
		var id = +new Date();
		var lastHookTime = +new Date();
		var instanceId = 0;
		var lastInstance;

		function notification(content, url) {
			if (!promised) return;

			instanceId += 1;

			// Add notification in queue
			var config = {
				content: content,
				url: url,
				id: instanceId,
			};
			notification.list.push(config);

			// Popup notification
			var count = notification.list.length;
			var instance = new Notification((count > 1 ? '[' + count + '] ' : '') + 'Apache Eagle:', {
				tag: 'eagle',
				body: content,
				icon: 'public/images/favicon.png',
				renotify: true,
			});
			instance.onclick = function () {
				window.focus();
				notification.trigger(config);

				if (lastInstance === instance) lastInstance = null;
				instance.close();

				$rootScope.$apply();
			};

			// Close notification
			setTimeout(function () {
				if (lastInstance === instance) {
					lastInstance = null;
					instance.close();
				}
			}, 5000);
			lastInstance = instance;
		}

		function uniqueNotification() {
			function loopListener() {
				var hooker = common.parseJSON(localStorage.getItem('notificationId'), null);
				if (promised || !hooker || (+new Date()) - (hooker.lastHookTime || 0) > PREEMPTIVE_TIME_LIMIT) {
					promised = true;
					lastHookTime = +new Date();
					localStorage.setItem('notificationId', JSON.stringify({
						id: id,
						lastHookTime: lastHookTime,
					}));
				}
			}

			setInterval(loopListener, REFRESH_TIME_LIMIT);
			loopListener();

			$(window).bind("beforeunload", function() {
				var hooker = common.parseJSON(localStorage.getItem('notificationId'), {});
				if (hooker.id === id) {
					localStorage.removeItem('notificationId');
				}
				if (lastInstance) {
					lastInstance.close();
					lastInstance = null;
				}
			});
		}

		if (!'Notification' in window || !'localStorage' in window) {
			// Notification not support
			console.warn('Browser do not support Notification api. Ignore...');
		} else {
			// Check notification state;
			if (Notification.permission === 'granted') {
				// promised = true;
				uniqueNotification();
			} else if (Notification.permission !== 'denied') {
				Notification.requestPermission().then(function(permission) {
					if (permission === "granted") {
						// promised = true;
						uniqueNotification();
					} else {
						console.warn('User deny the notification.');
					}
				});
			} else {
				console.warn('Web Notification initialization denied. Ignore eagle web notification.');
			}
		}

		notification.list = [];

		notification.trigger = function (config, event) {
			notification.list = common.array.remove(config.id, notification.list, ['id']);

			if (!event || !event.ctrlKey) {
				location.href = config.url;
				if (event) event.preventDefault();
			}
		};

		return notification;
	});
})();
