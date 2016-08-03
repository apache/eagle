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

(function () {
	"use strict";

	var _host = "";
	if(localStorage) {
		_host = localStorage.getItem("host") || "";
	}

	window._host = function (host) {
		if(host) {
			_host = host.replace(/[\\\/]+$/, "");
			if(localStorage) {
				localStorage.setItem("host", _host);
			}
		}
		return _host;
	};

	var _lastRegisterApp = null;
	var _providers = {};
	window.register = function (appName) {
		_lastRegisterApp = appName;
	};

	$(function () {
		console.info("[Eagle] Application initialization...");

		// Load providers
		$.get(_host + "/rest/apps/providers").then(function (res) {
			/**
			 * @param {{}} oriApp					application provider
			 * @param {string} oriApp.viewPath		path of application interface
			 */
			var promiseList = $.map(res.data || [], function (oriApp) {
				var promise = $.Deferred();
				var url = oriApp.viewPath;
				_providers[oriApp.type] = null;

				if(url) {
					url = url.replace(/^[\\\/]/, "").replace(/[\\\/]$/, "");

					$.getScript(url + "/index.js").then(function () {
						var appName = _lastRegisterApp || oriApp.type;

						try {
							angular.module(appName);
							_providers[oriApp.type] = appName;
						} catch(err) {
							console.error("Application module not exist:", oriApp.type);
						}

						_lastRegisterApp = null;
					}, function () {
						console.error("Load application failed:", oriApp.type);
					}).always(function () {
						promise.resolve();
					});
				} else {
					console.error("Path not config:", oriApp.type);
					promise.resolve();
				}

				return promise;
			});

			$.when.apply($, promiseList).always(function () {
				$(document).trigger("APPLICATION_READY", _providers);
			});
		});
	});
})();
