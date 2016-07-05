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

	/* App Module */
	var eagleApp = angular.module('eagleApp', ['ngRoute', 'ngAnimate', 'ui.router', 'eagleControllers', 'eagle.service']);

	// GRUNT REPLACEMENT: eagleApp.buildTimestamp = TIMESTAMP
	eagleApp._TRS = function() {
		return eagleApp.buildTimestamp || Math.random();
	};

	// ======================================================================================
	// =                                   Router config                                    =
	// ======================================================================================
	function routeResolve() {
		return {};
	}

	eagleApp.config(function ($stateProvider, $urlRouterProvider, $animateProvider) {
		$urlRouterProvider.otherwise("/");
		$stateProvider
		// ================================== Home ==================================
			.state('home', {
				url: "/",
				templateUrl: "partials/home.html?_=" + eagleApp._TRS(),
				controller: "homeCtrl",
				resolve: routeResolve()
			})
			.state('setup', {
				url: "/setup",
				templateUrl: "partials/setup.html?_=" + eagleApp._TRS(),
				controller: "setupCtrl",
				resolve: routeResolve()
			})
		// ================================= Alerts =================================
			.state('alert', {
				abstract: true,
				url: "/alert/",
				templateUrl: "partials/alert/main.html?_=" + eagleApp._TRS(),
				controller: "alertCtrl",
				resolve: routeResolve()
			})
			.state('alert.list', {
				url: "",
				templateUrl: "partials/alert/list.html?_=" + eagleApp._TRS(),
				controller: "alertListCtrl",
				resolve: routeResolve()
			})
			.state('alert.policyList', {
				url: "policyList",
				templateUrl: "partials/alert/policyList.html?_=" + eagleApp._TRS(),
				controller: "policyListCtrl",
				resolve: routeResolve()
			})
			.state('alert.policyCreate', {
				url: "policyCreate",
				templateUrl: "partials/alert/policyCreate.html?_=" + eagleApp._TRS(),
				controller: "policyCreateCtrl",
				resolve: routeResolve()
			});
	});

	// ======================================================================================
	// =                                   Main Controller                                  =
	// ======================================================================================
	var STATE_NAME_MATCH = /^[^\.]*/;

	eagleApp.controller('MainCtrl', function ($scope, $wrapState, PageConfig, Portal, Entity) {
		window._WrapState = $scope.$wrapState = $wrapState;
		window._PageConfig = $scope.PageConfig = PageConfig;
		window._Portal = $scope.Portal = Portal;
		window._Entity = $scope.Entity = Entity;

		$scope.$on('$stateChangeStart', function (event, next, nextParam, current, currentParam) {
			console.log("[Switch] current ->", current, currentParam);
			console.log("[Switch] next ->", next, nextParam);

			// Page initialization
			if(current.name.match(STATE_NAME_MATCH)[0] !== next.name.match(STATE_NAME_MATCH)[0]) {
				PageConfig.reset();
			}
		});
	});
}());