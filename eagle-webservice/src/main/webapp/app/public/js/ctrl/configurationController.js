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
	eagleControllers.controller('configFeatureCtrl', function ($scope, PageConfig, Application, Entities) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;

		$scope._newFeatureName = null;
		$scope._newFeatureLock = false;

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
			$("#featureMDL").modal();
		};

		$scope.newFeatureCheck = function() {
			if($scope._newFeatureName === null) return "";

			// Empty name
			if($scope._newFeatureName === "") {
				return "Feature can't be empty!";
			}

			// Conflict name
			if($scope._newFeatureName && $.map($scope.features, function(feature, name) {
					return name.toUpperCase() === $scope._newFeatureName.toUpperCase() ? true : null;
				}).length) {
				return "Feature name conflict!";
			}
			return "";
		};

		$scope.newFeatureConfirm = function() {
			var _feature;
			$scope._newFeatureLock = true;

			_feature = {
				tags: {
					feature: $scope._newFeatureName
				}
			};

			Entities.updateEntity(
				"FeatureDescService",
				_feature,
				{timestamp: false}
			)._promise.success(function() {
				// Reload Page
				$("#featureMDL").modal("hide")
					.on("hidden.bs.modal", function() {
						$(this).off("hidden.bs.modal");
						location.reload();
					});

				$scope._newSiteName = null;
			});
		};

		// Delete feature
		$scope.deleteFeature = function(feature) {
			$.dialog({
				title: "Delete Confirm",
				content: "Do you want to delete '" + feature.tags.feature + "'?",
				buttons: [
					{name: "Delete", class: "btn btn-danger", value: true},
					{name: "Cancel", class: "btn btn-default", value: false}
				]
			}, function(ret) {
				if(!ret) return;
				Entities.deleteEntity("FeatureDescService", feature)._promise.then(function() {
					location.reload();
				});
			});
		};
	});

	// ======================== Application ========================
	eagleControllers.controller('configApplicationCtrl', function ($scope, PageConfig, Application) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;

		// ================ Application ================
		$scope.application = Application.current() || Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};

		$scope.applications = {};
		$.each(Application.list, function(i, app) {
			$scope.applications[app.tags.application] = {
				feature: $.extend({}, app.feature)
			};
		});

		$scope.saveAll = function() {
			$.each(Application.list, function(i, app) {
				app.feature = $scope.applications[app.tags.application].feature;

				// TODO: Ajax update entities
			});
		};


		/*globalContent.setConfig(configPageConfig);

		 // ================ Application ================
		 $scope.application = Application.list[0];
		 $scope.setApplication = function (application) {
		 $scope.application = application;
		 };*/
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