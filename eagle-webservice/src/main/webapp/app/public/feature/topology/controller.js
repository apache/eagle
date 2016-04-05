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

	var featureControllers = angular.module('featureControllers');
	var feature = featureControllers.register("topology");

	// ==============================================================
	// =                       Initialization                       =
	// ==============================================================

	// ==============================================================
	// =                          Function                          =
	// ==============================================================
	//feature.service("DashboardFormatter", function() {
	//});

	// ==============================================================
	// =                         Controller                         =
	// ==============================================================
	feature.configNavItem("monitoring", "Topology", "usb");

	// ========================= Monitoring =========================
	feature.configController('monitoring', function(PageConfig, $scope, $interval, Entities, UI, Site, Application) {
		var topologyRefreshInterval;

		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		PageConfig.pageTitle = "Topology Execution";

		$scope.topologyExecutionList = null;

		$scope.currentTopologyExecution = null;
		$scope.currentTopologyExecutionOptList = [];

		// ======================= Function =======================
		function refreshExecutionList() {
			var _list = Entities.queryEntities("TopologyExecutionService");
			_list._promise.then(function () {
				$scope.topologyExecutionList = _list;
			});
		}

		$scope.showTopologyDetail = function (topologyExecution) {
			$scope.currentTopologyExecution = topologyExecution;
			$("#topologyMDL").modal();

			$scope.currentTopologyExecutionOptList = Entities.queryEntities("TopologyOperationService", {_pageSize: 20});
		};

		// ==================== Initialization ====================
		refreshExecutionList();
		topologyRefreshInterval = $interval(refreshExecutionList, 10 * 1000);

		$scope.topologyList = Entities.queryEntities("TopologyDescriptionService");
		$scope.topologyList._promise.then(function () {
			$scope.topologyList = $.map($scope.topologyList, function (topology) {
				return topology.tags.topology;
			});
		});

		$scope.applicationList = $.map(Application.list, function (application) {
			return application.tags.application;
		});

		$scope.siteList = $.map(Site.list, function (site) {
			return site.tags.site;
		});

		// ================== Topology Execution ==================
		$scope.newTopologyExecution = function () {
			UI.createConfirm("Topology", {}, [
				{field: "site", type: "select", valueList: $scope.siteList},
				{field: "application", type: "select", valueList: $scope.applicationList},
				{field: "topology", type: "select", valueList: $scope.topologyList}
			], function (entity) {
				for(var i = 0 ; i < $scope.topologyExecutionList.length; i += 1) {
					var _entity = $scope.topologyExecutionList[i].tags;
					if(_entity.site === entity.site && _entity.application === entity.application && _entity.topology === entity.topology) {
						return "Topology already exist!";
					}
				}
			}).then(null, null, function(holder) {
				var _entity = {
					tags: {
						site: holder.entity.site,
						application: holder.entity.application,
						topology: holder.entity.topology
					},
					status: "NEW"
				};
				Entities.updateEntity("TopologyExecutionService", _entity)._promise.then(function() {
					holder.closeFunc();
					$scope.topologyExecutionList.push(_entity);
				});
			});
		};

		$scope.deleteTopologyExecution = function (topologyExecution) {
			UI.deleteConfirm(topologyExecution.tags.topology).then(null, null, function(holder) {
				Entities.deleteEntities("TopologyExecutionService", topologyExecution.tags)._promise.then(function() {
					holder.closeFunc();
					common.array.remove(topologyExecution, $scope.topologyExecutionList);
				});
			});
		};

		// ================== Topology Operation ==================
		$scope.doTopologyOperation = function (topologyExecution, operation) {
			$.dialog({
				title: operation + " Confirm",
				content: "Do you want to " + operation + " '" + topologyExecution.tags.topology + "'?",
				confirm: true
			}, function (ret) {
				if(!ret) return;

				var list = Entities.queryEntities("TopologyOperationService", {
					site: topologyExecution.tags.site,
					application: topologyExecution.tags.application,
					topology: topologyExecution.tags.topology,
					_pageSize: 20
				});

				list._promise.then(function () {
					var lastOperation = common.array.find(operation, list, "tags.operation");
					if(lastOperation && (lastOperation.status !== "INITIALIZED" || lastOperation.status !== "PENDING")) {
						refreshExecutionList();
						return;
					}

					Entities.updateEntity("rest/app/operation", {
						tags: {
							site: topologyExecution.tags.site,
							application: topologyExecution.tags.application,
							topology: topologyExecution.tags.topology,
							operation: operation
						},
						status: "INITIALIZED"
					}, {timestamp: false, hook: true});
				});
			});
		};

		$scope.startTopologyOperation = function (topologyExecution) {
			$scope.doTopologyOperation(topologyExecution, "START");
		};
		$scope.stopTopologyOperation = function (topologyExecution) {
			$scope.doTopologyOperation(topologyExecution, "STOP");
		};

		// ======================= Clean Up =======================
		$scope.$on('$destroy', function() {
			$interval.cancel(topologyRefreshInterval);
		});
	});

	// ========================= Management =========================
	feature.configController('management', function(PageConfig, $scope, Entities, UI) {
		PageConfig.hideApplication = true;
		PageConfig.hideSite = true;
		PageConfig.pageTitle = "Topology";

		$scope.topologyList = Entities.queryEntities("TopologyDescriptionService");

		$scope.newTopology = function () {
			UI.createConfirm("Topology", {}, [
				{field: "topology", name: "name"},
				{field: "exeClass", name: "execution class", type: "blob", rows: 5},
				{field: "type"},
				{field: "version", optional: true},
				{field: "description", optional: true, type: "blob"}
			], function (entity) {
				if(common.array.find(entity.topology, $scope.topologyList, "tags.topology", false, false)) {
					return "Topology name conflict!";
				}
			}).then(null, null, function(holder) {
				holder.entity.tags = {
					topology: holder.entity.topology
				};
				Entities.updateEntity("TopologyDescriptionService", holder.entity, {timestamp: false})._promise.then(function() {
					holder.closeFunc();
					$scope.topologyList.push(holder.entity);
				});
			});
		};

		$scope.updateTopology = function (topology) {
			UI.updateConfirm("Topology", $.extend({}, topology, {topology: topology.tags.topology}), [
				{field: "topology", name: "name", readonly: true},
				{field: "exeClass", name: "execution class", type: "blob", rows: 5},
				{field: "type"},
				{field: "version", optional: true},
				{field: "description", optional: true, type: "blob"}
			]).then(null, null, function(holder) {
				holder.entity.tags = {
					topology: holder.entity.topology
				};
				Entities.updateEntity("TopologyDescriptionService", holder.entity, {timestamp: false})._promise.then(function() {
					holder.closeFunc();
					$.extend(topology, holder.entity);
				});
			});
		};

		$scope.deleteTopology = function (topology) {
			UI.deleteConfirm(topology.tags.topology).then(null, null, function(holder) {
				Entities.deleteEntities("TopologyDescriptionService", {topology: topology.tags.topology})._promise.then(function() {
					holder.closeFunc();
					common.array.remove(topology, $scope.topologyList);
				});
			});
		};
	});
})();