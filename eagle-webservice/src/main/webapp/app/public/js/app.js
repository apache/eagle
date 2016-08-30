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

var app = {};

(function() {
	'use strict';

	$(document).on("APPLICATION_READY", function (event, register) {
		console.info("[Eagle] Angular bootstrap...");

		// ======================================================================================
		// =                                   Initialization                                   =
		// ======================================================================================
		var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngAnimate', 'ui.router', 'eagleControllers', 'eagle.service'].concat(register.appList));

		// ======================================================================================
		// =                                   Router config                                    =
		// ======================================================================================
		function routeResolve(config) {
			var resolve = {};
			if(config === false) return resolve;

			config = $.extend({
				auth: true,
				site: true,
				application: true
			}, config);

			if(config.auth) {
				// TODO: need auth module
			}

			//if(config.site) {
			resolve.Site = function (Site) {
				return Site.getPromise(config);
			};
			//}

			//if(config.application) {
			resolve.Application = function (Application) {
				return Application.getPromise();
			};
			//}

			return resolve;
		}

		eagleApp.config(function ($stateProvider, $urlRouterProvider, $animateProvider) {
			$urlRouterProvider.deferIntercept();
			$urlRouterProvider.otherwise("/");
			$stateProvider
			// ================================== Home ==================================
				.state('home', {
					url: "/",
					templateUrl: "partials/home.html?_=" + window._TRS(),
					controller: "homeCtrl",
					resolve: routeResolve()
				})
				.state('setup', {
					url: "/setup",
					templateUrl: "partials/setup.html?_=" + window._TRS(),
					controller: "setupCtrl",
					resolve: routeResolve({ site: false, application: false })
				})
				// ================================= Alerts =================================
				.state('alert', {
					abstract: true,
					url: "/alert/",
					templateUrl: "partials/alert/main.html?_=" + window._TRS(),
					controller: "alertCtrl",
					resolve: routeResolve(false)
				})
				.state('alert.list', {
					url: "",
					templateUrl: "partials/alert/list.html?_=" + window._TRS(),
					controller: "alertListCtrl",
					resolve: routeResolve()
				})
				.state('alert.policyList', {
					url: "policyList",
					templateUrl: "partials/alert/policyList.html?_=" + window._TRS(),
					controller: "policyListCtrl",
					resolve: routeResolve()
				})
				.state('alert.policyCreate', {
					url: "policyCreate",
					templateUrl: "partials/alert/policyEdit.html?_=" + window._TRS(),
					controller: "policyCreateCtrl",
					resolve: routeResolve()
				})
				.state('alert.policyEdit', {
					url: "policyEdit/{name}",
					templateUrl: "partials/alert/policyEdit.html?_=" + window._TRS(),
					controller: "policyEditCtrl",
					resolve: routeResolve()
				})
				// =============================== Integration ==============================
				.state('integration', {
					abstract: true,
					url: "/integration/",
					templateUrl: "partials/integration/main.html?_=" + window._TRS(),
					controller: "integrationCtrl",
					resolve: routeResolve(false)
				})
				.state('integration.siteList', {
					url: "siteList",
					templateUrl: "partials/integration/siteList.html?_=" + window._TRS(),
					controller: "integrationSiteListCtrl",
					resolve: routeResolve({ application: false })
				})
				.state('integration.site', {
					url: "site/:id",
					templateUrl: "partials/integration/site.html?_=" + window._TRS(),
					controller: "integrationSiteCtrl",
					resolve: routeResolve({ application: false })
				})
				.state('integration.streamList', {
					url: "streamList",
					templateUrl: "partials/integration/streamList.html?_=" + window._TRS(),
					controller: "integrationStreamListCtrl",
					resolve: routeResolve()
				})
				// ================================== Site ==================================
				.state('site', {
					url: "/site/:id",
					templateUrl: "partials/site/home.html?_=" + window._TRS(),
					controller: "siteCtrl",
					resolve: routeResolve()
				})
			;

			// =========================== Application States ===========================
			$.each(register.routeList, function (i, route) {
				$stateProvider.state(route.state, route.config);
			});
		});

		// ======================================================================================
		// =                                   Main Controller                                  =
		// ======================================================================================
		var STATE_NAME_MATCH = /^[^.]*/;

		eagleApp.controller('MainCtrl', function ($scope, $wrapState, $urlRouter, PageConfig, Portal, Entity, Site, Application, UI, Time) {
			window._WrapState = $scope.$wrapState = $wrapState;
			window._PageConfig = $scope.PageConfig = PageConfig;
			window._Portal = $scope.Portal = Portal;
			window._Entity = $scope.Entity = Entity;
			window._Site = $scope.Site = Site;
			window._Application = $scope.Application = Application;
			window._UI = $scope.UI = UI;
			window._Time = $scope.Time = Time;
			$scope.common = common;

			var state_next;
			var state_current;
			var param_next;
			var param_current;

			Object.defineProperty(window, "scope", {
				get: function () {
					return angular.element("#content .ng-scope").scope();
				}
			});

			// ============================== Route Update ==============================
			$scope.$on('$stateChangeStart', function (event, next, nextParam, current, currentParam) {
				console.log("[Switch] current ->", current, currentParam);
				console.log("[Switch] next ->", next, nextParam);

				state_next = next || {};
				state_current = current || {};
				param_next = nextParam;
				param_current = currentParam;

				var currentName = (current || {}).name || "";
				var nextName = (next || {}).name || "";

				// Page initialization
				if(currentName.match(STATE_NAME_MATCH)[0] !== nextName.match(STATE_NAME_MATCH)[0]) {
					PageConfig.reset();
				}
			});

			$scope.$on('$locationChangeSuccess',
				/**
				 * @param {{}} e
				 * @param {function} e.preventDefault
				 * @param {string} newUrl
				 * @param {string} oldUrl
				 */
				function(e, newUrl, oldUrl) {
					e.preventDefault();

					// Check if page needn't update
					for(var i = 0 ; i < register.routeList.length ; i += 1) {
						var route = register.routeList[i];
						try {
							if (route.config.sync && !route.config.sync(newUrl, oldUrl, state_next, param_next, state_current, param_current)) {
								return;
							}
						} catch(err) {
							console.error("[Sync] Route sync error:", route, err);
						}
					}

					$urlRouter.sync();
			});
			$urlRouter.listen();

			// ================================ Function ================================
			// Get side bar navigation item class
			$scope.getNavClass = function (portal) {
				var path = (portal.path || "").replace(/^#/, '');

				if ($wrapState.path() === path) {
					return "active";
				} else {
					return "";
				}
			};

			// ================================== Init ==================================
			$.each(register.portalList, function (i, config) {
				Portal.register(config.portal, config.isSite);
			});
		});

		// ======================================================================================
		// =                                      Bootstrap                                     =
		// ======================================================================================
		//noinspection JSCheckFunctionSignatures
		angular.element(document).ready(function() {
			console.info("[Eagle] UI start...");
			//noinspection JSCheckFunctionSignatures
			angular.bootstrap(document, ['eagleApp']);
		});
	});
})();
