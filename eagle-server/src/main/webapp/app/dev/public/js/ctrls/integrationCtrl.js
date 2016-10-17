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
			$("a[data-id='introTab']").click();

			$scope.tmpApp = application;
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
		$scope.generalFields = [];
		$scope.advancedFields = [];
		$scope.customizeFields = [];
		$scope.installLock = false;

		// =================== Fields ===================
		$scope.newField = function () {
			UI.fieldConfirm({
				title: "New Field"
			}, null, [{
				field: "name",
				name: "Field Name"
			}, {
				field: "value",
				name: "Field Value",
				optional: true
			}])(function (entity, closeFunc, unlock) {
				var fullList = $scope.generalFields.concat($scope.advancedFields).concat($scope.customizeFields);
				if(common.array.find(entity.name, fullList, "field")) {
					$.dialog({
						title: "OPS",
						content: "Field already exist!"
					});

					unlock();
				} else {
					$scope.customizeFields.push({
						name: entity.name,
						_customize: true,
						required: true
					});
					$scope.tmpApp.configuration[entity.name] = entity.value;

					closeFunc();
				}
			});
		};

		$scope.removeField = function (field) {
			$scope.customizeFields = common.array.remove(field, $scope.customizeFields);
			delete $scope.tmpApp.configuration[field.name];
		};

		// =================== Check ===================
		$scope.checkJarPath = function () {
			var jarPath = ($scope.tmpApp || {}).jarPath;
			if(/\.jar$/.test(jarPath)) {
				$scope.tmpApp.mode = "CLUSTER";
			} else if(/\.class/.test(jarPath)) {
				$scope.tmpApp.mode = "LOCAL";
			}
		};

		$scope.collapseCheck = function () {
			setTimeout(function() {
				$scope.$apply();
			}, 400);
		};

		$scope.isCollapsed = function (dataId) {
			return !$("[data-id='" + dataId + "']").hasClass("in");
		};

		$scope.checkFields = function () {
			var pass = true;
			var config = common.getValueByPath($scope, ["tmpApp", "configuration"], {});
			$.each($scope.generalFields, function (i, field) {
				if (field.required && !config[field.name]) {
					pass = false;
					return false;
				}
			});
			return pass;
		};

		// =================== Config ===================
		$scope.getCustomFields = function (configuration) {
			var fields = common.getValueByPath($scope.application, "configuration.properties", []).concat();
			return $.map(configuration || {}, function (value, key) {
				if(!common.array.find(key, fields, ["name"])) {
					return {
						name: key,
						_customize: true,
						required: true
					};
				}
			});
		};

		$scope.configByJSON = function () {
			UI.fieldConfirm({
				title: "Configuration"
			}, {
				json: JSON.stringify($scope.tmpApp.configuration, null, "\t")
			}, [{
				field: "json",
				name: "JSON Configuration",
				type: "blob"
			}], function (entity) {
				var json = common.parseJSON(entity.json, false);
				if(!json) return 'Require JSON format';
			})(function (entity, closeFunc) {
				$scope.tmpApp.configuration = common.parseJSON(entity.json, {});
				$scope.customizeFields = $scope.getCustomFields($scope.tmpApp.configuration);
				closeFunc();
			});
		};

		// =================== Install ===================
		$scope.installAppConfirm = function () {
			$scope.installLock = true;

			var uuid = $scope.tmpApp.uuid;
			delete $scope.tmpApp.uuid;

			if(uuid) {
				Entity.create("apps/" + uuid, $scope.tmpApp)._then(function () {
					refreshApplications();
					$("#installMDL").modal("hide");
				}, function (res) {
					$.dialog({
						title: "OPS",
						content: res.data.message
					});
					$scope.installLock = false;
				});
			} else {
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
			}
		};

		// Install application
		$scope.installApp = function (application, entity) {
			entity = entity || {};
			application = application.origin;
			$scope.installLock = false;
			$scope.application = application;

			var docs = application.docs || {install: "", uninstall: ""};
			$scope.installHTML = $sce.trustAsHtml(docs.install);

			$scope.tmpApp = {
				mode: "CLUSTER",
				jarPath: application.jarPath,
				configuration: {}
			};

			if(!entity.uuid) {
				common.merge($scope.tmpApp, {
					siteId: $scope.site.siteId,
					appType: application.type
				});
				$scope.checkJarPath();
			}

			var fields = common.getValueByPath(application, "configuration.properties", []).concat();
			$scope.generalFields = [];
			$scope.advancedFields = [];
			$scope.customizeFields = [];

			$("[data-id='appEnvironment']").attr("class","collapse in").removeAttr("style");
			$("[data-id='appGeneral']").attr("class","collapse in").removeAttr("style");
			$("[data-id='appAdvanced']").attr("class","collapse").removeAttr("style");
			$("[data-id='appCustomize']").attr("class","collapse in").removeAttr("style");

			$.each(fields, function (i, field) {
				// Fill default value
				$scope.tmpApp.configuration[field.name] = field.value;

				// Reassign the fields
				if(field.required) {
					$scope.generalFields.push(field);
				} else {
					$scope.advancedFields.push(field);
				}
			});

			// Fill miss field of entity
			common.merge($scope.tmpApp, entity);
			$scope.customizeFields = $scope.getCustomFields(entity.configuration);

			// Dependencies check
			var missDep = false;
			$.each(application.dependencies, function (i, dep) {
				if(!Application.find(dep.type, $scope.site.siteId)[0]) {
					missDep = true;
					return false;
				}
			});

			$("#installMDL").modal();
			if(missDep) {
				$("a[data-id='guideTab']").click();
			} else {
				$("a[data-id='configTab']").click();
			}
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

		// ================================================================
		// =                          Management                          =
		// ================================================================
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
}());
