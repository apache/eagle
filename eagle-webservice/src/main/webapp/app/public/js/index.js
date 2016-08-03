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

	// ======================================================================================
	// =                                        Host                                        =
	// ======================================================================================
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

	// ======================================================================================
	// =                                      Register                                      =
	// ======================================================================================
	var _moduleStateId = 0;

	function Module(dependencies) {
		this.dependencies = dependencies;
		this.queueList = [];
		this.routeList = [];
		return this;
	}

	// GRUNT REPLACEMENT: Module.buildTimestamp = TIMESTAMP
	window._TRS = function() {
		return Module.buildTimestamp || Math.random();
	};

	Module.prototype.service = function () {
		this.queueList.push({type: "service", args: arguments});
	};
	Module.prototype.directive = function () {
		this.queueList.push({type: "directive", args: arguments});
	};
	Module.prototype.controller = function () {
		this.queueList.push({type: "controller", args: arguments});
	};

	/**
	 * Set application route
	 * @param {{}|string=} state				Config state. More info please check angular ui router
	 * @param {{}} config						Route config
	 * @param {string} config.url				Root url. start with '/'
	 * @param {string} config.templateUrl		Template url. Relative path of application `viewPath`
	 * @param {string} config.controller		Set page controller
	 */
	Module.prototype.route = function (state, config) {
		if(arguments.length === 1) {
			config = state;
			state = "_APPLICATION_STATE_" + _moduleStateId++;
		}

		if(!config.url) throw "Url not defined!";

		this.routeList.push({
			state: state,
			config: config
		});
	};
	Module.prototype.create = function (moduleName, path) {
		var module = angular.module(moduleName, this.dependencies);
		$.each(this.queueList, function (i, item) {
			var type = item.type;
			var args = Array.prototype.slice.apply(item.args);
			if(type === "controller") {
				args[0] = moduleName + "_" + args[0];
			}
			module[type].apply(module, args);
		});

		return $.map(this.routeList, function (route) {
			var config = route.config = $.extend({}, route.config);

			// Parse template url
			var parser = document.createElement('a');
			parser.href = path + "/" + config.templateUrl;
			parser.search = parser.search ? parser.search + "&_=" + window._TRS() : "?_=" + window._TRS();
			config.templateUrl = parser.href;

			if(typeof config.controller === "string") {
				config.controller = moduleName + "_" + config.controller;
			}

			return route;
		});
	};

	var _registerAppList = [];
	var _lastRegisterApp = null;
	window.register = function (dependencies) {
		_lastRegisterApp = new Module(dependencies);
		return _lastRegisterApp;
	};

	// ======================================================================================
	// =                                        Main                                        =
	// ======================================================================================
	$(function () {
		console.info("[Eagle] Application initialization...");
		var routeList = [];

		// Load providers
		$.get(_host + "/rest/apps/providers").then(function (res) {
			/**
			 * @param {{}} oriApp					application provider
			 * @param {string} oriApp.viewPath		path of application interface
			 */
			var promiseList = $.map(res.data || [], function (oriApp) {
				var promise = $.Deferred();
				var url = oriApp.viewPath;

				if(url) {
					url = url.replace(/^[\\\/]/, "").replace(/[\\\/]$/, "");

					$.getScript(url + "/index.js").then(function () {
						var appRouteList = [];
						if(_lastRegisterApp) {
							appRouteList = _lastRegisterApp.create(oriApp.type, url);
							routeList = routeList.concat(appRouteList);
							_registerAppList.push(oriApp.type);
						} else {
							console.error("Application not register:", oriApp.type);
						}
					}, function () {
						console.error("Load application failed:", oriApp.type);
					}).always(function () {
						_lastRegisterApp = null;
						promise.resolve();
					});
				} else {
					console.error("Path not config:", oriApp.type);
					promise.resolve();
				}

				return promise;
			});

			$.when.apply($, promiseList).always(function () {
				$(document).trigger("APPLICATION_READY", {
					appList: _registerAppList,
					routeList: routeList
				});
			});
		});
	});
})();
