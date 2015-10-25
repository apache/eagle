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

'use strict';

// =============================================================
// =                     User Profile List                     =
// =============================================================
damControllers.controller('userProfileListCtrl', function(globalContent, Site, damContent, $scope, $interval, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.pageSubTitle = Site.current().name;

	$scope.common = common;
	$scope.algorithms = [];

	// ======================================== Algorithms ========================================
	$scope.algorithmEntity = {};
	Entities.queryEntities("AlertDefinitionService", {site: Site.current().name, dataSource: "userProfile"})._promise.then(function(data) {
		$scope.algorithmEntity = common.getValueByPath(data, "data.obj[0]");
		$scope.algorithmEntity.policy = common.parseJSON($scope.algorithmEntity.policyDef);
	});

	// ======================================= User profile =======================================
	$scope.profileList = Entities.queryEntities("MLModelService", {site: Site.current().name}, ["user", "algorithm", "content", "version"]);
	$scope.profileList._promise.then(function() {
		var _algorithms = {};
		var _users = {};

		// Map user
		$.each($scope.profileList, function(i, unit) {
			_algorithms[unit.tags.algorithm] = unit.tags.algorithm;
			var _user = _users[unit.tags.user] = _users[unit.tags.user] || {user: unit.tags.user};
			_user[unit.tags.algorithm] = {
				version: unit.version
			};

			// DE
			if(unit.tags.algorithm === "DE") {
				var _statistics = common.parseJSON(unit.content);
				_statistics = common.getValueByPath(_statistics, "statistics", []);
				_user[unit.tags.algorithm].topCommands = $.map(common.array.top(_statistics, "mean"), function(command) {
					return command.commandName;
				});
			}
		});

		// Map algorithms
		$scope.algorithms = $.map(_algorithms, function(algorithm) {
			return algorithm;
		}).sort();

		$scope.profileList.splice(0);
		$scope.profileList.push.apply($scope.profileList, common.map.toArray(_users));
	});

	// =========================================== Task ===========================================
	$scope.tasks = [];
	function _loadTasks() {
		var _tasks = Entities.queryEntities("ScheduleTaskService", {
			site: Site.current().name,
			_pageSize: 100,
			_duration: 1000 * 60 * 60 * 24 * 14,
			__ETD: 1000 * 60 * 60 * 24
		});
		_tasks._promise.then(function() {
			$scope.tasks.splice(0);
			$scope.tasks.push.apply($scope.tasks, _tasks);

			// Duration
			$.each($scope.tasks, function(i, data) {
				if(data.timestamp && data.updateTime) {
					var _ms = (new moment(data.updateTime)).diff(new moment(data.timestamp));
					var _d = moment.duration(_ms);
					data._duration = Math.floor(_d.asHours()) + moment.utc(_ms).format(":mm:ss");
					data.duration = _ms;
				} else {
					data._duration = "--";
				}
			});
		});
	}

	$scope.runningTaskCount = function () {
		return common.array.count($scope.tasks, "INITIALIZED", "status") + 
				common.array.count($scope.tasks, "PENDING", "status") +
				common.array.count($scope.tasks, "EXECUTING", "status");
	};

	// Create task
	$scope.updateTask = function() {
		$.dialog({
			title: "Confirm",
			content: "Do you want to update now?",
			confirm: true
		}, function(ret) {
			if(!ret) return;

			var _entity = {
				status: "INITIALIZED",
				detail: "Newly created command",
				tags: {
					site: Site.current().name,
					type: "USER_PROFILE_TRAINING"
				},
				timestamp: +new Date()
			};
			Entities.updateEntity("ScheduleTaskService", _entity, {timestamp: false})._promise.success(function(data) {
				if(!Entities.dialog(data)) {
					_loadTasks();
				}
			});
		});
	};

	// Show detail
	$scope.showTaskDetail = function(task) {
		var _content = $("<pre>").text(task.detail);

		var $mdl = $.dialog({
			title: "Detail",
			content: _content
		});

		// TODO: Remove task
		_content.click(function(e) {
			if(!e.ctrlKey) return;

			$.dialog({
				title: "Confirm",
				content: "Remove this task?",
				confirm: true
			}, function(ret) {
				if(!ret) return;

				$mdl.modal('hide');
				Entities.deleteEntity("ScheduleTaskService", task)._promise.then(function(data) {
					_loadTasks();
				});
			});
		});
	};

	_loadTasks();
	var _loadInterval = $interval(_loadTasks, app.time.refreshInterval);
	$scope.$on('$destroy',function(){
		$interval.cancel(_loadInterval);
	});
});

// =============================================================
// =                    User Profile Detail                    =
// =============================================================
damControllers.controller('userProfileDetailCtrl', function(globalContent, Site, damContent, $scope, $routeParams, Entities) {
	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "User Profile";
	globalContent.pageSubTitle = Site.current().name;
	globalContent.navPath = ["User Profile", "Detail"];

	$scope.user = $routeParams.user;

	// User profile
	$scope.profiles = {};
	$scope.profileList = Entities.queryEntities("MLModelService", {site: Site.current().name, user: $scope.user});
	$scope.profileList._promise.then(function() {
		$.each($scope.profileList, function(i, unit) {
			unit._content = common.parseJSON(unit.content);
			$scope.profiles[unit.tags.algorithm] = unit;
		});

		// DE
		if($scope.profiles.DE) {
			$scope.profiles.DE._chart = {};

			$scope.profiles.DE.estimates = {};
			$.each($scope.profiles.DE._content, function(key, value) {
				if(key !== "statistics") {
					$scope.profiles.DE.estimates[key] = value;
				}
			});

			var _meanList = [];
			var _stddevList = [];
			var _categoryList = [];

			$.each($scope.profiles.DE._content.statistics, function(i, unit) {
				_meanList[i] = unit.mean;
				_stddevList[i] = unit.stddev;

				_categoryList[i] = unit.commandName;
			});
			$scope.profiles.DE._chart.series = [
				{
					name: "mean",
					data: _meanList
				},
				{
					name: "stddev",
					data: _stddevList
				},
				{
					type: "category",
					data: _categoryList
				},
			];

			// Percentage table list
			$scope.profiles.DE.meanList = [];
			var _total = common.array.sum($scope.profiles.DE._content.statistics, "mean");
			$.each($scope.profiles.DE._content.statistics, function(i, unit) {
				$scope.profiles.DE.meanList.push({
					command: unit.commandName,
					percentage: unit.mean / _total
				});
			});
		}

		// EigenDecomposition
		if($scope.profiles.EigenDecomposition && $scope.profiles.EigenDecomposition._content.principalComponents) {
			$scope.profiles.EigenDecomposition._chart = {
				series: [],
			};

			$.each($scope.profiles.EigenDecomposition._content.principalComponents, function(z, grp) {
				var _line = [];
				$.each(grp, function(x, y) {
					_line.push([x,y,z]);
				});

				$scope.profiles.EigenDecomposition._chart.series.push({
					data: _line
				});
			});
		}
	});

	// UI
	$scope.showRawData = function(content) {
		$.dialog({
			title: "Raw Data",
			content: $("<pre>").text(content)
		});
	};
});