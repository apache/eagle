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

damControllers.service('SensitivityConfig', function(Site) {
	'use strict';

		var config = {
		list: [
			{
				name: "HDFS",
				dataSrc: common.array.find("hdfsAuditLog", Site.current().dataSrcList, "tags.dataSource"),
				service: "FileSensitivityService",
				keys: ["filedir", "sensitivityType"],

				type: "folder",
				prefix: "fileSensitivity",
				api: "hdfsResource",
			},
			{
				name: "Hive",
				dataSrc: common.array.find("hiveQueryLog", Site.current().dataSrcList, "tags.dataSource"),
				service: "HiveResourceSensitivityService",
				keys: ["hiveResource", "sensitivityType"],

				type: "table",
				prefix: "hiveResourceSensitivity",
				api: {
					database: "hiveResource/databases",
					table: "hiveResource/tables",
					column: "hiveResource/columns",
				},
				mapping: {
					database: "database",
					table: "table",
					column: "column",
				},
			},
			{
				name: "HBase",
				dataSrc: common.array.find("hbaseSecurityLog", Site.current().dataSrcList, "tags.dataSource"),
				service: "HbaseResourceSensitivityService",
				keys: ["hbaseResource", "sensitivityType"],
				type: "table",
				prefix: "hbaseResourceSensitivity",
				api: {
					database: "hbaseResource/namespaces",
					table: "hbaseResource/tables",
					column: "hbaseResource/columns",
				},
				mapping: {
					database: "namespace",
					table: "table",
					column: "columnFamily",
				},
			},
		],
	};

	return config;
});

// =============================================================
// =                    Sensitivity Summary                    =
// =============================================================
damControllers.controller('sensitivitySummaryCtrl', function(globalContent, Site, damContent, $scope, Entities, SensitivityConfig) {
	'use strict';

	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "Data Classification";
	globalContent.pageSubTitle = Site.current().name;
	$scope.config = SensitivityConfig;

	$scope._sensitivitySource = null;
	$scope._sensitivityLock = false;
	$scope._sensitivityImportType = "By Text";
	$scope._sensitivityFile = "";
	$scope._sensitivityData = "";
	$scope._sensitivityError = "";

	// ==================== Statistic ====================
	var _refreshStatistic = function(entity) {
		if(entity) {
				entity.statisitc = Entities.queryGroup(entity.service, {site: Site.current().name}, "@site", "count");
		} else {
			$.each($scope.config.list, function(i, entity) {
				entity.statisitc = Entities.queryGroup(entity.service, {site: Site.current().name}, "@site", "count");
			});
		}
	};
	_refreshStatistic();

	// ================== Configuration ==================
	// Import sensitivity data
	$scope.showImportEditor = function(entity) {
		$scope._sensitivitySource = entity;

		$("#sensitivityMDL").modal('show');
		setTimeout(function() {
			$("#sensitivityData").focus();
		}, 500);
	};

	$scope.confirmImport = function() {
		if(!$scope._sensitivitySource) return;

		$scope._sensitivityLock = true;

		// Post data
		switch($scope._sensitivityImportType) {
		case "By Text":
		// > By Text
			Entities.updateEntity($scope._sensitivitySource.service, 
				common.parseJSON($scope._sensitivityData, null),
				 {timestamp: false})._promise.success(function(data) {
				if(!Entities.dialog(data)) {
					$("#sensitivityMDL").modal('hide');
					$scope._sensitivityData = "";

					_refreshStatistic($scope._sensitivitySource);
				}
			}).finally(function() {
				$scope._sensitivityLock = false;
			});
			break;

		case "By File":
		// > By File
			var formData = new FormData();
			formData.append("file", $("#sensitivityFile")[0].files[0]);
			formData.append("site", Site.current().name);

			$.ajax({
				url : app.getURL("updateEntity", {serviceName: $scope._sensitivitySource.service}),
				data : formData,
				cache : false,
				contentType : false,
				processData : false,
				type : 'POST',
			}).done(function(data) {
				if(!Entities.dialog(data)) {
					$("#sensitivityMDL").modal('hide');
					$scope._sensitivityFile = "";

					_refreshStatistic($scope._sensitivitySource);
				}
			}).always(function() {
				$scope._sensitivityLock = false;
				$scope.$apply();
			});
		}
	};

	$scope.importCheck = function() {
		if($scope._sensitivityLock) return false;
		$scope._sensitivityError = "";

		switch($scope._sensitivityImportType) {
		case "By Text":
			if(!$scope._sensitivityData) return false;

			var _list = common.parseJSON($scope._sensitivityData, null);
			if(!_list) {
				$scope._sensitivityError = "Can't parse json";
			} else if(!$.isArray(_list)) {
				$scope._sensitivityError = "Must be array";
			} else if(_list.length === 0) {
				$scope._sensitivityError = "Please provide at least one sensitivity item";
			}
			break;
		case "By File":
			if(!$scope._sensitivityFile) return false;
			break;
		}
		return !$scope._sensitivityError;
	};

	// Manage sensitivity data
	$scope.showManagementEditor = function(entity) {
		$scope._sensitivitySource = entity;
		$("#sensitivityListMDL").modal('show');

		entity.list = Entities.queryEntities(entity.service, {site: Site.current().name});
	};

	$scope.deleteItem = function(item) {
		$.dialog({
			title: "Delete Confirm",
			content: "Do you want to delete '" + item.tags[$scope._sensitivitySource.keys[0]] + "'?",
			buttons: [
				{name: "Delete", class: "btn btn-danger", value: true},
				{name: "Cancel", class: "btn btn-default", value: false},
			]
		}, function(ret) {
			if(!ret) return;

			common.array.remove(item, $scope._sensitivitySource.list);
			Entities.deleteEntity($scope._sensitivitySource.service, item)._promise.then(function() {
				_refreshStatistic($scope._sensitivitySource);
			});


			$scope.$apply();
		});
	};

	$scope.deleteAll = function(entity) {
		$.dialog({
			title: "Delete Confirm",
			content: "<span class='text-red fa fa-exclamation-triangle pull-left' style='font-size: 50px;'></span>" +
					"<p>You are <strong class='text-red'>DELETING</strong> all the sensitivity data from '" + entity.name + "'!</p>" +
					"<p>Proceed to delete?</p>",
			buttons: [
				{name: "Delete", class: "btn btn-danger", value: true},
				{name: "Cancel", class: "btn btn-default", value: false},
			]
		}, function(ret) {
			if(!ret) return;

			Entities.deleteEntities(entity.service, {
				site: Site.current().name
			})._promise.then(function() {
				_refreshStatistic(entity);
			});

			entity.list.splice(0);
			$scope.$apply();
		});
	};
});

// =============================================================
// =                        Sensitivity                        =
// =============================================================
damControllers.controller('sensitivityCtrl', function(globalContent, Site, damContent, $scope, $routeParams, SensitivityConfig) {
	'use strict';

	globalContent.setConfig(damContent.config);
	globalContent.pageTitle = "Data Classification";
	globalContent.pageSubTitle = Site.current().name;
	$scope.config = SensitivityConfig;
	$scope.ajaxId = Math.random();

	// Check data source
	$scope.dataSrc = common.array.find($routeParams.dataSrc, $scope.config.list, "name");
	if(!$scope.dataSrc) {
		$.dialog({
			title: "OPS",
			content: "Data Source '" + $routeParams.dataSrc + "' not found!"
		}, function() {
			Site.url('/dam/sensitivitySummary');
		});
	}
});

// ==============================================================
// =                    Sensitivity - Folder                    =
// ==============================================================
damControllers.controller('sensitivityViewFolderCtrl', function(globalContent, Site, damContent, $scope, $location, Entities, SensitivityConfig) {
	'use strict';

	$scope.path = $location.search().path || "/";
	$scope.pathUnitList = [];
	$scope.items = [];

	// Mark sensitivity
	$scope._oriItem = {};
	$scope._markItem = {};

	// ======================= View =======================
	// Path
	function _refreshPathUnitList(_path) {
		var _start,_current, _unitList = [];
		_path = _path + (_path.match(/\/$/) ? "" : "/");
		for(_current = _start = 0 ; _current < _path.length ; _current += 1) {
			if(_path[_current] === "/") {
				_unitList.push({
					name: _path.substring(_start, _current + (_current === 0 ? 1 : 0)),
					path: _path.substring(0, _current === 0 ? 1 : _current),
				});
				_start = _current + 1;
			}
		}
		$scope.pathUnitList = _unitList;
	}

	// Item
	$scope.updateItems = function(path) {
		if(path) $scope.path = path;

		$scope.items = Entities.query($scope.dataSrc.api, {site: Site.current().name, path: $scope.path});
		$scope.items._promise.success(function(data) {
			var $dlg = Entities.dialog(data, function() {
				if($scope.path !== "/") $scope.updateItems("/");
			});
		});
		_refreshPathUnitList($scope.path);
	};

	$scope.getFileName = function(item) {
		return (item.resource + "").replace(/^.*\//, "");
	};

	$scope.updateItems($scope.path);

	// =================== Sensitivity ===================
	$scope.markSensitivity = function(item) {
		$scope._oriItem = item;
		$scope._markItem = {
			prefix: $scope.dataSrc.prefix,
			tags: {
				site: Site.current().name,
			},
			sensitivityType: ""
		};
		$scope._markItem.tags[$scope.dataSrc.keys[0]] = item.resource;
		$("#sensitivityMDL").modal();
		setTimeout(function() {
			$("#sensitiveType").focus();
		}, 500);
	};
	$scope.confirmUpateSensitivity = function() {
		$scope._oriItem.sensitiveType = $scope._markItem.sensitivityType;
		var _promise = Entities.updateEntity($scope.dataSrc.service, $scope._markItem, {timestamp: false})._promise.success(function(data) {
			Entities.dialog(data);
		});
		$("#sensitivityMDL").modal('hide');
	};
	$scope.unmarkSensitivity = function(item) {
		$.dialog({
			title: "Unmark Confirm",
			content: "Do you want to remove the sensitivity mark on '" + item.resource + "'?",
			confirm: true
		}, function(ret) {
			if(!ret) return;

			var _cond = {site: Site.current().name};
			_cond[$scope.dataSrc.keys[0]] = item.resource;
			Entities.deleteEntities($scope.dataSrc.service, _cond);

			item.sensitiveType = null;
			$scope.$apply();
		});
	};
});

// =============================================================
// =                    Sensitivity - Table                    =
// =============================================================
damControllers.controller('sensitivityViewTableCtrl', function(globalContent, Site, damContent, $scope, $location, Entities, SensitivityConfig) {
	'use strict';

	$scope.table = null;

	// Mark sensitivity
	$scope._oriItem = {};
	$scope._markItem = {};

	// ======================= View =======================
	var _fillAttr = function(list, key, target) {
		list._promise.then(function() {
			$.each(list, function(i, unit) {
				unit[key] = unit[target];
			});
		});
	};

	$scope.databases = Entities.query($scope.dataSrc.api.database, {site: Site.current().name});
	_fillAttr($scope.databases, "database", $scope.dataSrc.mapping.database);

	$scope.loadTables = function(database) {
		if(database.tables) return;
		var _qry = {
			site: Site.current().name
		};
		_qry[$scope.dataSrc.mapping.database] = database[$scope.dataSrc.mapping.database];
		database.tables = Entities.query($scope.dataSrc.api.table, _qry);
		_fillAttr(database.tables, "table", $scope.dataSrc.mapping.table);
		_fillAttr(database.tables, "database", $scope.dataSrc.mapping.database);
	};

	$scope.loadColumns = function(database, table) {
		$scope.table = table;

		if(table.columns) return;
		var _qry = {
			site: Site.current().name
		};
		_qry[$scope.dataSrc.mapping.database] = database[$scope.dataSrc.mapping.database];
		_qry[$scope.dataSrc.mapping.table] = table[$scope.dataSrc.mapping.table];
		table.columns = Entities.query($scope.dataSrc.api.column, _qry);
		_fillAttr(table.columns, "column", $scope.dataSrc.mapping.column);
	};

	// =================== Sensitivity ===================
	$scope.markSensitivity = function(item, event) {
		if(event) event.stopPropagation();

		$scope._oriItem = item;
		$scope._markItem = {
			prefix: $scope.dataSrc.prefix,
			tags: {
				site: Site.current().name,
			},
			sensitivityType: ""
		};
		$scope._markItem.tags[$scope.dataSrc.keys[0]] = item.resource;
		$("#sensitivityMDL").modal();
		setTimeout(function() {
			$("#sensitiveType").focus();
		}, 500);
	};
	$scope.confirmUpateSensitivity = function() {
		$scope._oriItem.sensitiveType = $scope._markItem.sensitivityType;
		var _promise = Entities.updateEntity($scope.dataSrc.service, $scope._markItem, {timestamp: false})._promise.success(function(data) {
			Entities.dialog(data);
		});
		$("#sensitivityMDL").modal('hide');
	};
	$scope.unmarkSensitivity = function(item, event) {
		if(event) event.stopPropagation();

		$.dialog({
			title: "Unmark Confirm",
			content: "Do you want to remove the sensitivity mark on '" + item.resource + "'?",
			confirm: true
		}, function(ret) {
			if(!ret) return;

			var _qry = {
				site: Site.current().name,
			};
			_qry[$scope.dataSrc.keys[0]] = item.resource;
			Entities.deleteEntities($scope.dataSrc.service, _qry);

			item.sensitiveType = null;
			$scope.$apply();
		});
	};
});