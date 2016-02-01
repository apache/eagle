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
	// =============================================================
	// =                       Configuration                       =
	// =============================================================
	// ========================== Feature ==========================
	eagleControllers.controller('configFeatureCtrl', function ($scope, PageConfig, Application, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		$scope._pageLock = false;

		// ================== Feature ==================
		// Current feature
		$scope.feature = Application.featureList[0];
		$scope.setFeature = function (feature) {
			$scope.feature = feature;
		};

		// Feature list
		$scope.features = {};
		$.each(Application.featureList, function(i, feature) {
			$scope.features[feature.tags.feature] = $.extend({}, feature, true);
		});

		// Create feature
		$scope.newFeature = function() {
			UI.createConfirm("Feature", {}, [
				{name: "Feature Name", field: "name"}
			], function(entity) {
				if(entity.name && $.map($scope.features, function(feature, name) {
						return name.toUpperCase() === entity.name.toUpperCase() ? true : null;
					}).length) {
					return "Feature name conflict!";
				}
			}).then(function(holder) {
				Entities.updateEntity(
					"FeatureDescService",
					{tags: {feature: holder.entity.name}},
					{timestamp: false}
				)._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// Delete feature
		$scope.deleteFeature = function(feature) {
			UI.deleteConfirm(feature.tags.feature).then(function(holder) {
				Entities.deleteEntity("FeatureDescService", feature)._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// Save feature
		$scope.saveAll = function() {
			$scope._pageLock = true;
			var _list = $.map($scope.features, function(feature) {
				return feature;
			});
			Entities.updateEntity("FeatureDescService", _list, {timestamp: false})._promise.success(function() {
				location.reload();
			}).finally(function() {
				$scope._pageLock = false;
			});
		};
	});

	// ======================== Application ========================
	eagleControllers.controller('configApplicationCtrl', function ($scope, $timeout, PageConfig, Application, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		$scope._pageLock = false;

		// ================ Application ================
		// Current application
		$scope.application = Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};

		// Application list
		$scope.applications = {};
		$.each(Application.list, function(i, application) {
			var _application = $scope.applications[application.tags.application] = $.extend({}, application, true);
			_application.optionalFeatures = $.map(Application.featureList, function(feature) {
				if(!common.array.find(feature.tags.feature, _application.features)) {
					return feature.tags.feature;
				}
			});
		});

		// Create application
		$scope.newApplication = function() {
			UI.createConfirm("Application", {}, [
				{name: "Application Name", field: "name"}
			], function(entity) {
				if(entity.name && $.map($scope.applications, function(application, name) {
						return name.toUpperCase() === entity.name.toUpperCase() ? true : null;
					}).length) {
					return "Application name conflict!";
				}
			}).then(function(holder) {
				Entities.updateEntity(
					"ApplicationDescService",
					{tags: {application: holder.entity.name}},
					{timestamp: false}
				)._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// Delete application
		$scope.deleteApplication = function(application) {
			UI.deleteConfirm(application.tags.application).then(function(holder) {
				Entities.deleteEntity("ApplicationDescService", application)._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// ================= Function ==================
		$scope._feature = "";
		function highlightFeature(feature) {
			$scope._feature = feature;

			$timeout(function() {
				$scope._feature = "";
			}, 100);
		}

		$scope.addFeature = function(feature, application) {
			application.features.push(feature);
			common.array.remove(feature, application.optionalFeatures);
			highlightFeature(feature);
		};

		$scope.removeFeature = function(feature, application) {
			application.optionalFeatures.push(feature);
			common.array.remove(feature, application.features);
		};

		$scope.moveFeature = function(feature, list, offset) {
			common.array.moveOffset(feature, list, offset);
			highlightFeature(feature);
		};

		// Save feature
		$scope.saveAll = function() {
			$scope._pageLock = true;

			var _list = $.map($scope.applications, function(application) {
				return application;
			});
			Entities.updateEntity("ApplicationDescService", _list, {timestamp: false})._promise.success(function() {
				location.reload();
			}).finally(function() {
				$scope._pageLock = false;
			});
		};
	});

	// ============================ Site ===========================
	eagleControllers.controller('configSiteCtrl', function ($scope, PageConfig, Site) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;

		$scope.site = Site.current() || Site.list[0];
		$scope.sites = {};
		$scope._newSiteName = null;
		$scope._newSiteLock = false;
		
		// =================== Site ===================
		$scope.setSite = function (site) {
			$scope.site = site;
		};

		$.each(Site.list, function(i, site) {
			$scope.sites[site.tags.site] = {
				app: $.extend({}, site.applicationList.set)
			};
		});

		// ================= New Site =================
		$scope.newSite = function() {
			$("#siteMDL").modal();
			setTimeout(function() {
				$("#siteName").focus();
			}, 500);
		};
		$scope.newSiteCheck = function() {
			if($scope._newSiteName === null) return "";

			// Empty name
			if($scope._newSiteName === "") {
				return "Site can't be empty!";
			}

			// Conflict name
			if($scope._newSiteName && $.map($scope.sites, function(site, name) {
					return name.toUpperCase() === $scope._newSiteName.toUpperCase() ? true : null;
				}).length) {
				return "Site name conflict!";
			}
			return "";
		};

		$scope.newSiteConfirm = function() {
			$scope._newSiteLock = true;

			// TODO: Ajax create
			$("#siteMDL").modal("hide")
				.on("hidden.bs.modal", function() {
					$("#siteMDL").off("hidden.bs.modal");
					Site.reload();
				});

			$scope._newSiteName = null;
		};

		// ================= Save Site ================
		$scope.saveAll = function() {
			$.each(Site.list, function(i, site) {
				site.app = $scope.sites[site.tags.site].app;

				// TODO: Ajax update entities
			});
		};
	});
})();