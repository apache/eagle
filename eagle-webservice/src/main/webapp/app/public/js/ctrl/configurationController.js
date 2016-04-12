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
	// =                         Function                          =
	// =============================================================
	function watchEdit($scope, key) {
		$scope.changed = false;
		setTimeout(function() {
			var _func = $scope.$watch(key, function(newValue, oldValue) {
				if(angular.equals(newValue, oldValue)) return;
				$scope.changed = true;
				_func();
			}, true);
		}, 100);
	}

	// =============================================================
	// =                       Configuration                       =
	// =============================================================
	// ========================== Feature ==========================
	eagleControllers.controller('configFeatureCtrl', function ($scope, PageConfig, Application, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		$scope._pageLock = false;

		PageConfig
			.addNavPath("Home", "/")
			.addNavPath("Feature Config");

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
			}).then(null, null, function(holder) {
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
			UI.deleteConfirm(feature.tags.feature).then(null, null, function(holder) {
				Entities.delete("FeatureDescService", {feature: feature.tags.feature})._promise.then(function() {
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

		// Watch config update
		watchEdit($scope, "features");
	});

	// ======================== Application ========================
	eagleControllers.controller('configApplicationCtrl', function ($scope, $timeout, PageConfig, Application, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		$scope._pageLock = false;

		PageConfig
			.addNavPath("Home", "/")
			.addNavPath("Application Config");

		// ================ Application ================
		// Current application
		$scope.application = Application.list[0];
		$scope.setApplication = function (application) {
			$scope.application = application;
		};

		// Application list
		$scope.applications = {};
		$.each(Application.list, function(i, application) {
			var _application = $scope.applications[application.tags.application] = $.extend({}, application, {features: application.features.slice()}, true);
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
			}).then(null, null, function(holder) {
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
			UI.deleteConfirm(application.tags.application).then(null, null, function(holder) {
				Entities.delete("ApplicationDescService", {application: application.tags.application})._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// ================= Function ==================
		// Configuration check
		$scope.configCheck = function(config) {
			if(config && !common.parseJSON(config, false)) {
				return "Invalid JSON format";
			}
		};

		// Feature
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
			$scope.changed = true;
		};

		$scope.removeFeature = function(feature, application) {
			application.optionalFeatures.push(feature);
			common.array.remove(feature, application.features);
			$scope.changed = true;
		};

		$scope.moveFeature = function(feature, list, offset) {
			common.array.moveOffset(feature, list, offset);
			highlightFeature(feature);
			$scope.changed = true;
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

		// Watch config update
		watchEdit($scope, "applications");
	});

	// ============================ Site ===========================
	eagleControllers.controller('configSiteCtrl', function ($scope, $timeout, PageConfig, Site, Application, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		$scope._pageLock = false;

		PageConfig
			.addNavPath("Home", "/")
			.addNavPath("Site Config");

		// =================== Site ====================
		// Current site
		$scope.site = Site.list[0];
		$scope.setSite = function (site) {
			$scope.site = site;
		};


		// Site list
		$scope.sites = {};
		$.each(Site.list, function(i, site) {
			var _site = $scope.sites[site.tags.site] = $.extend({}, site, true);
			var _applications = [];
			var _optionalApplications = [];

			Object.defineProperties(_site, {
				applications: {
					get: function() {return _applications;}
				},
				optionalApplications: {
					get: function() {return _optionalApplications;}
				}
			});

			$.each(Application.list, function(i, application) {
				var _application = site.applicationList.set[application.tags.application];
				if(_application && _application.enabled) {
					_site.applications.push(_application);
				} else {
					if(_application) {
						_site.optionalApplications.push(_application);
					} else {
						_site.optionalApplications.push({
							prefix: "eagleSiteApplication",
							config: "",
							enabled: false,
							tags: {
								application: application.tags.application,
								site: site.tags.site
							}
						});
					}
				}
			});
		});

		// Create site
		$scope.newSite = function() {
			UI.createConfirm("Site", {}, [
				{name: "Site Name", field: "name"}
			], function(entity) {
				if(entity.name && $.map($scope.sites, function(site, name) {
						return name.toUpperCase() === entity.name.toUpperCase() ? true : null;
					}).length) {
					return "Site name conflict!";
				}
			}).then(null, null, function(holder) {
				Entities.updateEntity(
					"SiteDescService",
					{enabled: true, tags: {site: holder.entity.name}},
					{timestamp: false}
				)._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// Delete site
		$scope.deleteSite = function(site) {
			UI.deleteConfirm(site.tags.site).then(null, null, function(holder) {
				Entities.delete("SiteDescService", {site: site.tags.site})._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		// ================= Function ==================
		$scope._application = "";
		function highlightApplication(application) {
			$scope._application = application;

			$timeout(function() {
				$scope._application = "";
			}, 100);
		}

		$scope.addApplication = function(application, site) {
			site.applications.push(application);
			common.array.remove(application, site.optionalApplications);
			application.enabled = true;
			highlightApplication(application);
			$scope.changed = true;
		};

		$scope.removeApplication = function(application, site) {
			site.optionalApplications.push(application);
			common.array.remove(application, site.applications);
			application.enabled = false;
			$scope.changed = true;
		};

		$scope.setApplication = function(application) {
			var _oriConfig = application.config;
			UI.updateConfirm("Application", {config: _oriConfig}, [
				{name: "Configuration", field: "config", type: "blob"}
			]).then(null, null, function(holder) {
				application.config = holder.entity.config;
				holder.closeFunc();
				if(_oriConfig !== application.config) $scope.changed = true;
			});
		};

		// Save feature
		$scope.saveAll = function() {
			$scope._pageLock = true;

			var _list = $.map($scope.sites, function(site) {
				var _clone = $.extend({applications: site.applications.concat(site.optionalApplications)}, site);
				return _clone;
			});

			Entities.updateEntity("SiteDescService", _list, {timestamp: false, hook: true})._promise.success(function() {
				location.reload();
			}).finally(function() {
				$scope._pageLock = false;
			});
		};

		// Watch config update
		watchEdit($scope, "sites");
	});
})();