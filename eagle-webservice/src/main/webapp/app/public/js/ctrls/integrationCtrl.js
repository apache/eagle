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

	var eagleControllers = angular.module('eagleControllers');

	// ======================================================================================
	// =                                        Main                                        =
	// ======================================================================================
	eagleControllers.controller('integrationCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Integration";
		$scope.getState = function() {
			return $wrapState.current.name;
		};
	});

	// ======================================================================================
	// =                                        Site                                        =
	// ======================================================================================
	eagleControllers.controller('integrationSiteListCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Integration";
		PageConfig.subTitle = "Site";
	});

	eagleControllers.controller('integrationSiteCtrl', function ($sce, $scope, $wrapState, PageConfig, Entity, UI, Site, Application) {
		PageConfig.title = "Site";
		PageConfig.subTitle = $wrapState.param.id;

		// Check site
		$scope.site = Site.find($wrapState.param.id);
		if(!$scope.site) {
			$.dialog({
				title: "OPS",
				content: "Site not found!"
			}, function () {
				$wrapState.go("integration.siteList");
			});
			return;
		}

		// Map applications
		var uninstalledApplicationList = common.array.minus(Application.providerList, $scope.site.applicationList, "type", "descriptor.type");
		$scope.applicationList = $.map($scope.site.applicationList, function (app) {
			app.installed = true;
			return app;
		}).concat($.map(uninstalledApplicationList, function (oriApp) {
			return { origin: oriApp };
		}));

		// Application status class
		$scope.getAppStatusClass = function (application) {
			switch((application.status || "").toUpperCase()) {
				case "INITIALIZED":
					return "success";
			}
			return "info";
		};

		// Application detail
		$scope.showAppDetail = function (application) {
			var docs = application.docs || {install: "", uninstall: ""};
			$scope.application = application;
			$scope.installHTML = $sce.trustAsHtml(docs.install);
			$scope.uninstallHTML = $sce.trustAsHtml(docs.uninstall);
			$("#appMDL").modal();
		};

		// Install application
		$scope.installApp = function (application) {
			var fields = common.getValueByPath(application, "configuration.properties", []);
			fields = $.map(fields, function (prop) {
				return {
					field: prop.name,
					name: prop.displayName,
					description: prop.description,
					defaultValue: prop.value
				};
			});

			UI.fieldConfirm({
				title: "Install '" + application.type + "'"
			}, null, fields)(function (entity, closeFunc) {
				Entity.create("apps/install", {
					siteId: $scope.site.siteId,
					appType: application.type,
					configuration: entity
				})._then(function () {
					$wrapState.reload();
					closeFunc();
				});
			});
		};

		// Uninstall application
		$scope.uninstallApp = function (application) {
			UI.deleteConfirm(application.descriptor.name + " - " + application.site.siteId)
			(function (entity, closeFunc) {
				
			});
		}
	});

	// ======================================================================================
	// =                                     Application                                    =
	// ======================================================================================
	eagleControllers.controller('integrationApplicationCtrl', function ($scope, $wrapState, PageConfig) {
		PageConfig.title = "Todo Application!!!";
	});
}());
