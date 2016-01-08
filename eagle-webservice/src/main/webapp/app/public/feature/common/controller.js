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

	var featureControllers = angular.module("featureControllers");
	var feature = featureControllers.register("common");

	// ==============================================================
	// =                          Function                          =
	// ==============================================================
	feature.service("PolicyOperation", function() {});
	feature.service("PolicyOperation", function() {});

	// ==============================================================
	// =                          Policies                          =
	// ==============================================================

	// ======================= Policy Summary =======================
	feature.navItem("summary", "Policies", "list");
	feature.controller('summary', function(PageConfig, Site, $scope, $q, Entities) {
		PageConfig.pageSubTitle = Site.current().name;

		$scope.dataSources = {};
		$scope.dataReady = false;

		var _policyList = Entities.queryGroup("AlertDefinitionService", {dataSource:null, site: Site.current().name}, "@dataSource", "count");

		_policyList._promise.then(function() {
			// List programs
			$.each(_policyList, function(i, unit) {
				var _dataSrc = Site.current().dataSrcList.find(unit.key[0]);
				if(_dataSrc) {
					_dataSrc.count = unit.value[0];
				} else {
					var _siteHref = $("<a>").attr('href', '#/dam/siteList').text("Setup");
					var _dlg = $.dialog({
						title: "Data Source Not Found",
						content: $("<div>")
							.append("Data Source [" + unit.key[0] + "] not found. Please check your configuration in ")
							.append(_siteHref)
							.append(" page.")
					});
					_siteHref.click(function() {
						_dlg.modal('hide');
					});
				}
			});

			$scope.dataReady = true;
		});
	});

	// ========================= Policy List ========================
	feature.controller('policyList', function(PageConfig, Site, $scope, $stateParams, Entities) {
		'use strict';

		PageConfig.pageTitle = "Policy List";
		PageConfig.pageSubTitle = Site.current().name;
		PageConfig
			.addNavPath("Policy View", "/common/summary")
			.addNavPath("Policy List");

		// Initial load
		$scope.policyList = [];
		if($stateParams.filter) {
			$scope.dataSource = Site.current().dataSrcList.find($stateParams.filter);
		}

		// List policies
		var _policyList = Entities.queryEntities("AlertDefinitionService", {site: Site.current().name, dataSource: $stateParams.filter});
		_policyList._promise.then(function() {
			$.each(_policyList, function(i, policy) {
				if($.inArray(policy.tags.dataSource, app.config.dataSource.uiInvisibleList) === -1) {
					policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
					policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[,;]/);
					policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

					$scope.policyList.push(policy);
				}
			});
		});
		$scope.policyList._promise = _policyList._promise;

		// Function
		$scope.searchFunc = function(item) {
			var key = $scope.search;
			if(!key) return true;

			var _key = key.toLowerCase();
			function _hasKey(item, path) {
				var _value = common.getValueByPath(item, path, "").toLowerCase();
				return _value.indexOf(_key) !== -1;
			}
			return _hasKey(item, "tags.policyId") || _hasKey(item, "__expression") || _hasKey(item, "desc") || _hasKey(item, "owner") || _hasKey(item, "__mailStr");
		};

		$scope.updatePolicyStatus = damContent.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			damContent.deletePolicy(policy, function(policy) {
				var _index = $scope.policyList.indexOf(policy);
				$scope.policyList.splice(_index, 1);
			});
		};
	});

	// ======================= Policy Detail ========================
	feature.controller('policyDetail', function(PageConfig, Site, $scope, $stateParams, Entities, nvd3) {
		'use strict';

		var MAX_PAGESIZE = 10000;

		PageConfig.pageTitle = "Policy Detail";
		PageConfig.lockSite = true;
		PageConfig
			.addNavPath("Policy View", "/common/summary")
			.addNavPath("Policy List", "/common/policyList")
			.addNavPath("Policy Detail");

		$scope.common = common;
		$scope.chartConfig = {
			chart: "line",
			xType: "time"
		};

		// Query policy
		if($routeParams.encodedRowkey) {
			$scope.policyList = Entities.queryEntity("AlertDefinitionService", $routeParams.encodedRowkey);
		} else {
			$scope.policyList = Entities.queryEntities("AlertDefinitionService", {
				policyId: $routeParams.policy,
				site: $routeParams.site,
				alertExecutorId: $routeParams.executor
			});
		}
		$scope.policyList._promise.then(function() {
			var policy = null;

			if($scope.policyList.length === 0) {
				$.dialog({
					title: "OPS!",
					content: "Policy not found!",
				}, function() {
					location.href = "#/dam/policyList";
				});
				return;
			} else {
				policy = $scope.policyList[0];

				policy.__mailStr = common.getValueByPath(common.parseJSON(policy.notificationDef, {}), "0.recipients", "");
				policy.__mailList = policy.__mailStr.trim() === "" ? [] : policy.__mailStr.split(/[\,\;]/);
				policy.__expression = common.parseJSON(policy.policyDef, {}).expression;

				$scope.policy = policy;
				Site.current(Site.find($scope.policy.tags.site));
				console.log($scope.policy);
			}

			// Visualization
			var _endTime = app.time.now().hour(23).minute(59).second(59).millisecond(0);
			var _startTime = _endTime.clone().subtract(1, "month").hour(0).minute(0).second(0).millisecond(0);
			var _cond = {
				dataSource: policy.tags.dataSource,
				policyId: policy.tags.policyId,
				_startTime: _startTime,
				_endTime: _endTime
			};

			// > eagle.policy.eval.count
			$scope.policyEvalSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.policy.eval.fail.count
			$scope.policyEvalFailSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.policy.eval.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.alert.count
			$scope.alertSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// > eagle.alert.fail.count
			$scope.alertFailSeries = nvd3.convert.eagle([Entities.querySeries("GenericMetricService", $.extend({_metricName: "eagle.alert.fail.count"}, _cond), "@cluster", "sum(value)", 60 * 24)]);

			// Alert list
			$scope.alertList = Entities.queryEntities("AlertService", {
				site: Site.current().name,
				dataSource: policy.tags.dataSource,
				policyId: policy.tags.policyId,
				_pageSize: MAX_PAGESIZE,
				_duration: 1000 * 60 * 60 * 24 * 30,
				__ETD: 1000 * 60 * 60 * 24
			});
		});

		// Function
		$scope.updatePolicyStatus = damContent.updatePolicyStatus;
		$scope.deletePolicy = function(policy) {
			damContent.deletePolicy(policy, function(policy) {
				location.href = "#/dam/policyList";
			});
		};
	});
})();