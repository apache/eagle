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
	var feature = featureControllers.register("classification");

	// ==============================================================
	// =                          Function                          =
	// ==============================================================

	// =============================================================
	// =                        Sensitivity                        =
	// =============================================================
	feature.navItem("sensitivity", "Classification", "user-secret");
	feature.controller('sensitivity', function(PageConfig, Site, $scope, Application, Entities, UI) {
		PageConfig.pageTitle = "Data Classification";
		PageConfig.pageSubTitle = Site.current().tags.site;
		$scope.ajaxId = Math.random();
		$scope.viewConfig = Application.current().configObj.view;

		if(!$scope.viewConfig) {
			$.dialog({
				title: "OPS",
				content: "View configuration not defined in Application."
			});
			return;
		}

		// ===================== Function =====================
		$scope.export = function() {
			var _data = {};
			UI.fieldConfirm({title: "Export Classification", confirm: false, size: "large"}, _data, [
				{name: "Data", field: "data", type: "blob", rows: 20, optional: true, readonly: true}]
			);

			Entities.queryEntities($scope.viewConfig.service, {site: Site.current().tags.site})._promise.then(function(data) {
				_data.data = JSON.stringify(data, null, "\t");
			});
		};

		$scope.import = function() {
			UI.fieldConfirm({title: "Import Classification", size: "large"}, {}, [
				{name: "Data", field: "data", type: "blob", rows: 20, optional: true}
			], function(entity) {
				var _list = common.parseJSON(entity.data, false);
				if(!_list) {
					return "Invalid JSON format";
				}
				if(!$.isArray(_list)) {
					return "Not an array";
				}
			}).then(null, null, function(holder) {
				Entities.updateEntity($scope.viewConfig.service, common.parseJSON(holder.entity.data, []), {timestamp: false})._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};

		$scope.deleteAll = function() {
			UI.deleteConfirm("All the Classification Data").then(null, null, function(holder) {
				Entities.deleteEntities($scope.viewConfig.service, {site: Site.current().tags.site})._promise.then(function() {
					holder.closeFunc();
					location.reload();
				});
			});
		};
	});

	// =============================================================
	// =                    Sensitivity - Folder                   =
	// =============================================================
	feature.controller('sensitivityViewFolder', function(Site, $scope, $wrapState, Entities) {
		$scope.path = $wrapState.param.path || "/";
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
						path: _path.substring(0, _current === 0 ? 1 : _current)
					});
					_start = _current + 1;
				}
			}
			$scope.pathUnitList = _unitList;
		}

		// Item
		$scope.updateItems = function(path) {
			if(path) $scope.path = path;

			$scope.items = Entities.query($scope.viewConfig.api, {site: Site.current().tags.site, path: $scope.path});
			$scope.items._promise.success(function(data) {
				Entities.dialog(data, function() {
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
				prefix: $scope.viewConfig.prefix,
				tags: {
					site: Site.current().tags.site
				},
				sensitivityType: ""
			};
			$scope._markItem.tags[$scope.viewConfig.keys[0]] = item.resource;
			$("#sensitivityMDL").modal();
			setTimeout(function() {
				$("#sensitiveType").focus();
			}, 500);
		};
		$scope.confirmUpateSensitivity = function() {
			$scope._oriItem.sensitiveType = $scope._markItem.sensitivityType;
			Entities.updateEntity($scope.viewConfig.service, $scope._markItem, {timestamp: false})._promise.success(function(data) {
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

				var _cond = {site: Site.current().tags.site};
				_cond[$scope.viewConfig.keys[0]] = item.resource;
				Entities.deleteEntities($scope.viewConfig.service, _cond);

				item.sensitiveType = null;
				$scope.$apply();
			});
		};
	});

	// =============================================================
	// =                    Sensitivity - Table                    =
	// =============================================================
	feature.controller('sensitivityViewTable', function(Site, $scope, Entities) {
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

		$scope.databases = Entities.query($scope.viewConfig.api.database, {site: Site.current().tags.site});
		_fillAttr($scope.databases, "database", $scope.viewConfig.mapping.database);

		$scope.loadTables = function(database) {
			if(database.tables) return;
			var _qry = {
				site: Site.current().tags.site
			};
			_qry[$scope.viewConfig.mapping.database] = database[$scope.viewConfig.mapping.database];
			database.tables = Entities.query($scope.viewConfig.api.table, _qry);
			_fillAttr(database.tables, "table", $scope.viewConfig.mapping.table);
			_fillAttr(database.tables, "database", $scope.viewConfig.mapping.database);
		};

		$scope.loadColumns = function(database, table) {
			$scope.table = table;

			if(table.columns) return;
			var _qry = {
				site: Site.current().tags.site
			};
			_qry[$scope.viewConfig.mapping.database] = database[$scope.viewConfig.mapping.database];
			_qry[$scope.viewConfig.mapping.table] = table[$scope.viewConfig.mapping.table];
			table.columns = Entities.query($scope.viewConfig.api.column, _qry);
			_fillAttr(table.columns, "column", $scope.viewConfig.mapping.column);
		};

		// =================== Sensitivity ===================
		$scope.markSensitivity = function(item, event) {
			if(event) event.stopPropagation();

			$scope._oriItem = item;
			$scope._markItem = {
				prefix: $scope.viewConfig.prefix,
				tags: {
					site: Site.current().tags.site
				},
				sensitivityType: ""
			};
			$scope._markItem.tags[$scope.viewConfig.keys[0]] = item.resource;
			$("#sensitivityMDL").modal();
			setTimeout(function() {
				$("#sensitiveType").focus();
			}, 500);
		};
		$scope.confirmUpateSensitivity = function() {
			$scope._oriItem.sensitiveType = $scope._markItem.sensitivityType;
			Entities.updateEntity($scope.viewConfig.service, $scope._markItem, {timestamp: false})._promise.success(function(data) {
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
					site: Site.current().tags.site
				};
				_qry[$scope.viewConfig.keys[0]] = item.resource;
				Entities.deleteEntities($scope.viewConfig.service, _qry);

				item.sensitiveType = null;
				$scope.$apply();
			});
		};
	});
})();