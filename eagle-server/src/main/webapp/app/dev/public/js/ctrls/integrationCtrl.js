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
	eagleControllers.controller('integrationSiteListCtrl', function ($scope, $wrapState, PageConfig, UI, Entity, Site) {
		PageConfig.title = "Integration";
		PageConfig.subTitle = "Site";

		$scope.deleteSite = function (site) {
			UI.deleteConfirm(site.siteId)
			(function (entity, closeFunc, unlock) {
				Entity.delete("sites", site.uuid)._then(function () {
					Site.reload();
					closeFunc();
				}, unlock);
			});
		};

		$scope.newSite = function () {
			UI.createConfirm("Site", {}, [
				{field: "siteId", name: "Site Id"},
				{field: "siteName", name: "Display Name", optional: true},
				{field: "description", name: "Description", optional: true, type: "blob", rows: 5}
			])(function (entity, closeFunc, unlock) {
				Entity.create("sites", entity)._then(function () {
					Site.reload();
					closeFunc();
				}, unlock);
			});
		};
	});

	eagleControllers.controller('integrationSiteCtrl', function ($sce, $scope, $wrapState, $interval, PageConfig, Entity, UI, Site, Application) {
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
		console.log("[Site]", $scope.site);

		$scope.siteName = $scope.site.siteId + ($scope.site.siteName ? "(" + $scope.site.siteName + ")" : "");

		// Map applications
		function mapApplications() {
			Site.getPromise().then(function () {
				$scope.site = Site.find($wrapState.param.id);
				var uninstalledApplicationList = common.array.minus(Application.providerList, $scope.site.applicationList, "type", "descriptor.type");
				$scope.applicationList = $.map($scope.site.applicationList, function (app) {
					app.installed = true;
					return app;
				}).concat($.map(uninstalledApplicationList, function (oriApp) {
					return { origin: oriApp };
				}));
			});
		}
		mapApplications();

		// Application refresh
		var refreshApplications = $scope.refreshApplications = function() {
			Application.reload().getPromise().then(mapApplications);
		};

		// Application status class
		$scope.getAppStatusClass = function (application) {
			switch((application.status || "").toUpperCase()) {
				case "INITIALIZED":
					return "primary";
				case "STARTING":
					return "warning";
				case "RUNNING":
					return "success";
				case "STOPPING":
					return "warning";
				case "STOPPED":
					return "danger";
			}
			return "default";
		};

		// Get started application count
		$scope.getStartedAppCount = function () {
			return $.grep($scope.site.applicationList, function (app) {
				return $.inArray((app.status || "").toUpperCase(), ["STARTING", "RUNNING"]) >= 0;
			}).length;
		};

		// Application detail
		$scope.showAppDetail = function (application) {
			application = application.origin;
			var docs = application.docs || {install: "", uninstall: ""};
			$scope.application = application;
			$scope.installHTML = $sce.trustAsHtml(docs.install);
			$scope.uninstallHTML = $sce.trustAsHtml(docs.uninstall);
			$("#appMDL").modal();
		};

		// ================================================================
		// =                         Installation                         =
		// ================================================================
		$scope.tmpApp = {};
		$scope.tmpAppConfigFields = [];
		$scope.installLock = false;

		$scope.newField = function () {
			UI.fieldConfirm({
				title: "New Field"
			}, null, [{
				field: "name",
				name: "Field Name"
			}])(function (entity, closeFunc, unlock) {
				if(common.array.find(entity.name, $scope.tmpAppConfigFields, "field")) {
					$.dialog({
						title: "OPS",
						content: "Field already exist!"
					});

					unlock();
				} else {
					$scope.tmpAppConfigFields.push({
						name: entity.name,
						_customize: true,
						required: true
					});

					closeFunc();
				}
			});
		};

		$scope.removeField = function (field) {
			$scope.tmpAppConfigFields = common.array.remove(field, $scope.tmpAppConfigFields);
			delete $scope.tmpApp.configuration[field.name];
		};

		$scope.checkFields = function () {
			var pass = true;
			var config = common.getValueByPath($scope, ["tmpApp", "configuration"]);
			$.each($scope.tmpAppConfigFields, function (i, field) {
				if(field.required && !config[field.name]) {
					pass = false;
					return false;
				}
			});
			return pass;
		};

		$scope.installAppConfirm = function () {
			$scope.installLock = true;

			Entity.create("apps/install", $scope.tmpApp)._then(function () {
				refreshApplications();
				$("#installMDL").modal("hide");
			}, function (res) {
				$.dialog({
					title: "OPS",
					content: res.data.message
				});
				$scope.installLock = false;
			});
		};

		// Install application
		$scope.installApp = function (application, entity) {
			entity = entity || {};
			application = application.origin;
			$scope.installLock = false;
			$scope.application = application;
			$scope.tmpApp = {
				siteId: $scope.site.siteId,
				appType: application.type,
				mode: "CLUSTER",
				jarPath: application.jarPath,
				configuration: {}
			};

			var fields = $scope.tmpAppConfigFields = common.getValueByPath(application, "configuration.properties", []).concat();

			$.each(fields, function (i, field) {
				$scope.tmpApp.configuration[field.name] = field.value;
			});

			// Fill miss field of entity
			common.merge($scope.tmpApp, entity);
			$.each(entity.configuration || {}, function (key) {
				if(!common.array.find(key, fields, ["name"])) {
					fields.push({
						name: key,
						_customize: true,
						required: true
					});
				}
			});

			$("#installMDL").modal();
			$("a[data-id='configTab']").click();
		};

		// Uninstall application
		$scope.uninstallApp = function (application) {
			UI.deleteConfirm(application.descriptor.name + " - " + application.site.siteId)
			(function (entity, closeFunc, unlock) {
				Entity.delete("apps/uninstall", application.uuid)._then(function () {
					refreshApplications();
					closeFunc();
				}, unlock);
			});
		};

		// Start application
		$scope.startApp = function (application) {
			Entity.post("apps/start", { uuid: application.uuid })._then(function () {
				refreshApplications();
			});
		};

		// Stop application
		$scope.stopApp = function (application) {
			Entity.post("apps/stop", { uuid: application.uuid })._then(function () {
				refreshApplications();
			});
		};

		$scope.editApp = function (application) {
			var type = application.descriptor.type;
			var provider = Application.findProvider(type);

			$scope.installApp({origin: provider}, {
				mode: application.mode,
				jarPath: application.jarPath,
				uuid: application.uuid,
				configuration: $.extend({}, application.configuration)
			});
		};

		// ================================================================
		// =                             Sync                             =
		// ================================================================
		var refreshInterval = $interval(refreshApplications, 1000 * 60);
		$scope.$on('$destroy', function() {
			$interval.cancel(refreshInterval);
		});
	});

	// ======================================================================================
	// =                                     Application                                    =
	// ======================================================================================
	eagleControllers.controller('integrationApplicationListCtrl', function ($sce, $scope, $wrapState, PageConfig, Application) {
		PageConfig.title = "Integration";
		PageConfig.subTitle = "Applications";

		$scope.showAppDetail = function(application) {
			var docs = application.docs || {install: "", uninstall: ""};
			$scope.application = application;
			$scope.installHTML = $sce.trustAsHtml(docs.install);
			$scope.uninstallHTML = $sce.trustAsHtml(docs.uninstall);
			$("#appMDL").modal();
		};
	});

	// ======================================================================================
	// =                                       Stream                                       =
	// ======================================================================================
	eagleControllers.controller('integrationStreamListCtrl', function ($scope, $wrapState, PageConfig, Application) {
		PageConfig.title = "Integration";
		PageConfig.subTitle = "Streams";

		$scope.streamList = $.map(Application.list, function (app) {
			return (app.streams || []).map(function (stream) {
				return {
					streamId: stream.streamId,
					appType: app.descriptor.type,
					siteId: app.site.siteId,
					schema: stream.schema
				};
			});
		});
	});
}());
