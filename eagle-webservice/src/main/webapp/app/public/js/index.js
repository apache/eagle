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
	var register = window.register = function (appName) {
		_lastRegisterApp = appName;
	};
	register.providers = {};

	$(function () {
		console.info("Eagle Web Start...");

		// Load providers
		$.get(_host + "/rest/apps/providers").then(function (res) {
			/**
			 * @param {{}} oriApp					application provider
			 * @param {string} oriApp.viewPath		path of application interface
			 */
			$.each(res.data || [], function (i, oriApp) {
				var url = oriApp.viewPath;
				register.providers[oriApp.type] = "";

				if(url) {
					url = url.replace(/^[\\\/]/, "").replace(/[\\\/]$/, "");

					console.log("Getting", oriApp.type, url);
					$.getScript(url + "/index.js", function () {
						register.providers[oriApp.type] = _lastRegisterApp;
						_lastRegisterApp = null;
					});
				} else {
					console.error("Path not config:", oriApp);
				}
			});
		});
	});
})();
