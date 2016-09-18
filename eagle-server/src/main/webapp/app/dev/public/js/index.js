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
	var _app = {};
	if(localStorage) {
		_host = localStorage.getItem("host") || "";
		_app = common.parseJSON(localStorage.getItem("app") || "") || {};
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

	window._app = function (appName, viewPath) {
		if(arguments.length) {
			_app[appName] = {
				viewPath: viewPath
			};
			if(localStorage) {
				localStorage.setItem("app", JSON.stringify(_app));
			}
		}
		return _app;
	};

	// ======================================================================================
	// =                                      Register                                      =
	// ======================================================================================
	var _moduleStateId = 0;
	var _registerAppList = [];
	var _lastRegisterApp = null;
	var _hookRequireFunc = null;

	function Module(dependencies) {
		this.dependencies = dependencies;
		this.queueList = [];
		this.routeList = [];
		this.portalList = [];

		this.requireRest = 0;
		this.requireDeferred = null;

		return this;
	}

	// GRUNT REPLACEMENT: Module.buildTimestamp = TIMESTAMP
	window._TRS = function() {
		return Module.buildTimestamp || Math.random();
	};

	Module.prototype.service = function () {
		this.queueList.push({type: "service", args: arguments});
		return this;
	};
	Module.prototype.directive = function () {
		this.queueList.push({type: "directive", args: arguments});
		return this;
	};
	Module.prototype.controller = function () {
		this.queueList.push({type: "controller", args: arguments});
		return this;
	};

	/**
	 * Add portal into side navigation bar.
	 * @param {{}} portal				Config portal content
	 * @param {string} portal.name		Display name
	 * @param {string} portal.icon		Display icon. Use 'FontAwesome'
	 * @param {string=} portal.path		Route path
	 * @param {[]=} portal.list			Sub portal
	 * @param {boolean} isSite			true will show in site page or will shown in main page
	 */
	Module.prototype.portal = function (portal, isSite) {
		this.portalList.push({portal: portal, isSite: isSite});
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
		return this;
	};

	Module.prototype.require = function (scriptURL) {
		var _this = this;

		_this.requireRest += 1;
		if(!_this.requireDeferred) {
			_this.requireDeferred = $.Deferred();
		}

		setTimeout(function () {
			$.getScript(_this.baseURL + "/" + scriptURL).then(function () {
				if(_hookRequireFunc) {
					_hookRequireFunc(_this);
				} else {
					console.error("Hook function not set!", _this);
				}
			}).always(function () {
				_hookRequireFunc = null;
				_this.requireRest -= 1;
				_this.requireCheck();
			});
		}, 0);
	};

	Module.prototype.requireCSS = function (styleURL) {
		var _this = this;
		setTimeout(function () {
			$("<link/>", {
				rel: "stylesheet",
				type: "text/css",
				href: _this.baseURL + "/" + styleURL
			}).appendTo("head");
		}, 0);
	};

	Module.prototype.requireCheck = function () {
		if(this.requireRest === 0) {
			this.requireDeferred.resolve();
		}
	};

	/**
	 * Get module instance. Will init angular module.
	 * @param {string} moduleName	angular module name
	 */
	Module.prototype.getInstance = function (moduleName) {
		var _this = this;
		var deferred = $.Deferred();
		var module = angular.module(moduleName, this.dependencies);

		// Required list
		$.when(this.requireDeferred).always(function () {
			// Fill module props
			$.each(_this.queueList, function (i, item) {
				var type = item.type;
				var args = Array.prototype.slice.apply(item.args);
				if (type === "controller") {
					args[0] = moduleName + "_" + args[0];
				}
				module[type].apply(module, args);
			});

			// Render routes
			var routeList = $.map(_this.routeList, function (route) {
				var config = route.config = $.extend({}, route.config);

				// Parse url
				if(config.site) {
					config.url = "/site/:siteId/" + config.url.replace(/^[\\\/]/, "");
				}

				// Parse template url
				var parser = document.createElement('a');
				parser.href = _this.baseURL + "/" + config.templateUrl;
				parser.search = parser.search ? parser.search + "&_=" + window._TRS() : "?_=" + window._TRS();
				config.templateUrl = parser.href;

				if (typeof config.controller === "string") {
					config.controller = moduleName + "_" + config.controller;
				}

				return route;
			});

			// Portal update
			$.each(_this.portalList, function (i, config) {
				config.portal.application = moduleName;
			});

			deferred.resolve({
				application: moduleName,
				portalList: _this.portalList,
				routeList: routeList
			});
		});

		return deferred;
	};

	window.register = function (dependencies) {
		if($.isArray(dependencies)) {
			_lastRegisterApp = new Module(dependencies);
		} else if(typeof dependencies === "function") {
			_hookRequireFunc = function (module) {
				dependencies(module);
			};
		}
		return _lastRegisterApp;
	};

	// ======================================================================================
	// =                                        Main                                        =
	// ======================================================================================
	$(function () {
		console.info("[Eagle] Application initialization...");

		// Load providers
		$.get(_host + "/rest/apps/providers").then(function (res) {
			/**
			 * @param {{}} oriApp					application provider
			 * @param {string} oriApp.viewPath		path of application interface
			 */
			var promiseList = $.map(res.data || [], function (oriApp) {
				var deferred = $.Deferred();
				var viewPath = common.getValueByPath(_app, [oriApp.type, "viewPath"], oriApp.viewPath);

				if(viewPath) {
					var url = viewPath;
					url = url.replace(/^[\\\/]/, "").replace(/[\\\/]$/, "");

					$.getScript(url + "/index.js").then(function () {
						if(_lastRegisterApp) {
							_registerAppList.push(oriApp.type);
							_lastRegisterApp.baseURL = url;
							_lastRegisterApp.getInstance(oriApp.type).then(function (module) {
								deferred.resolve(module);
							});
						} else {
							console.error("Application not register:", oriApp.type);
							deferred.resolve();
						}
					}, function () {
						console.error("Load application failed:", oriApp.type, viewPath);
						deferred.resolve();
					}).always(function () {
						_lastRegisterApp = null;
					});
				} else {
					deferred.resolve();
				}

				return deferred;
			});

			common.deferred.all(promiseList).then(function (moduleList) {
				var routeList = $.map(moduleList, function (module) {
					return module && module.routeList;
				});
				var portalList = $.map(moduleList, function (module) {
					return module && module.portalList;
				});

				$(document).trigger("APPLICATION_READY", {
					appList: _registerAppList,
					routeList: routeList,
					portalList: portalList
				});
			});
		});
	});
})();
