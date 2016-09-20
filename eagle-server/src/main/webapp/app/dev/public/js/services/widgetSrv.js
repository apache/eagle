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

	var serviceModule = angular.module('eagle.service');

	serviceModule.service('Widget', function($wrapState, Site) {
		var Widget = {};

		var mainWidgetList = [];
		var siteWidgetList = [];
		var siteWidgets = {};

		Widget.register = function (widget, isSite) {
			(isSite ? siteWidgetList : mainWidgetList).push(widget);
		};

		Widget.refresh = function () {
			siteWidgets = {};
			$.each(Site.list, function (i, site) {
				siteWidgets[site.siteId] = $.map(siteWidgetList, function (widget) {
					var hasApp = !!common.array.find(widget.application, site.applicationList, "descriptor.type");
					if(hasApp) {
						return widget;
					}
				});
			});
		};

		Object.defineProperty(Widget, 'list', {
			get: function () {
				var site = Site.current();
				if(!site) {
					return mainWidgetList;
				} else if(site.siteId) {
					return siteWidgets[site.siteId];
				} else {
					console.warn("Can't find current site id.");
					return [];
				}
			}
		});

		// Initialization
		Site.onReload(Widget.refresh);

		Widget.refresh();

		return Widget;
	});
})();
