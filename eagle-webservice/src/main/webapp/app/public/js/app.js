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

		var STATE_NAME_MATCH = /^[^.]*/;
		var state_next;
		var state_current;
		var param_next;
		var param_current;

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

			resolve.Site = function (Site) {
				return Site.getPromise(config);
			};

			resolve.Application = function (Application) {
				return Application.getPromise();
			};

			resolve.Time = function (Time) {
				return Time.getPromise(config, state_next, param_next);
			};

			return resolve;
		}

		eagleApp.config(function ($stateProvider, $urlRouterProvider, $animateProvider) {
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
					url: "/site/:siteId",
					templateUrl: "partials/site/home.html?_=" + window._TRS(),
					controller: "siteCtrl",
					resolve: routeResolve()
				})
			;

			// =========================== Application States ===========================
			$.each(register.routeList, function (i, route) {
				var config = $.extend({}, route.config);

				var resolve = {};
				var resolveConfig = {};
				if(route.config.resolve) {
					$.each(route.config.resolve, function (key, value) {
						if (typeof value === "function") {
							resolve[key] = value;
						} else {
							resolveConfig[key] = value;
						}
					});
				}
				config.resolve = $.extend(routeResolve(resolveConfig), resolve);

				$stateProvider.state(route.state, config);
			});
		});

		// ======================================================================================
		// =                                   Main Controller                                  =
		// ======================================================================================
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

			// Customize time range
			$scope.customizeTimeRange = function () {
				$("#eagleStartTime").val(Time.format("startTime"));
				$("#eagleEndTime").val(Time.format("endTime"));
				$("#eagleTimeRangeMDL").modal();
			};

			$scope.setLastDuration = function (hours) {
				var endTime = Time();
				var startTime = endTime.clone().subtract(hours, "hours");
				Time.timeRange(startTime, endTime);
			};

			$scope.updateTimeRange = function () {
				var startTime = Time.verifyTime($("#eagleStartTime").val());
				var endTime = Time.verifyTime($("#eagleEndTime").val());
				if(startTime && endTime) {
					Time.timeRange(startTime, endTime);
					$("#eagleTimeRangeMDL").modal("hide");
				} else {
					alert("Time range not validate");
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
