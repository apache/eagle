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

	var serviceModule = angular.module('eagle.service');

	function wrapPromise(promise) {
		var retFunc = function (notifyFunc, resolveFunc, rejectFunc) {
			promise.then(resolveFunc, rejectFunc, function (holder) {
				notifyFunc(holder.entity, holder.closeFunc, holder.unlock);
			});
		};

		retFunc.then = promise.then;

		return retFunc;
	}

	/**
	 * Check function to check fields pass or not
	 * @callback checkFieldFunction
	 * @param {{}} entity
	 * @return {string}
	 */

	serviceModule.service('UI', function($rootScope, $q, $compile) {
		function UI() {}

		function _bindShortcut($dialog) {
			$dialog.on("keydown", function (event) {
				if(event.which === 13) {
					if(!$(":focus").is("textarea")) {
						$dialog.find(".confirmBtn:enabled").click();
					}
				}
			});
		}

		function _fieldDialog(create, name, entity, fieldList, checkFunc) {
			var _deferred, $mdl, $scope;

			var _entity = entity || {};

			_deferred = $q.defer();
			$scope = $rootScope.$new(true);
			$scope.name = name;
			$scope.entity = _entity;
			$scope.fieldList = fieldList.concat();
			$scope.checkFunc = checkFunc;
			$scope.lock = false;
			$scope.create = create;

			$scope.config = typeof name === "object" ? name : {};

			// Init
			if(!entity) {
				$.each(fieldList, function (i, field) {
					if(field.defaultValue) {
						_entity[field.field] = field.defaultValue;
					}
				});
			}

			// Modal
			$mdl = $(TMPL_FIELDS).appendTo('body');
			$compile($mdl)($scope);
			$mdl.modal();
			setTimeout(function () {
				$mdl.find("input, select").filter(':visible:first:enabled').focus();
			}, 500);

			$mdl.on("hide.bs.modal", function() {
				_deferred.reject();
			});
			$mdl.on("hidden.bs.modal", function() {
				_deferred.resolve({
					entity: _entity
				});
				$mdl.remove();
			});

			// Function
			$scope.getFieldDescription = function (field) {
				if(typeof field.description === "function") {
					return field.description($scope.entity);
				}
				return field.description || ((field.name || field.field) + '...');
			};

			$scope.emptyFieldList = function() {
				return $.map(fieldList, function(field) {
					if(!field.optional && !_entity[field.field]) {
						return field.field;
					}
				});
			};

			$scope.newField = function () {
				UI.fieldConfirm({
					title: "New Field"
				}, null, [{
					field: "field",
					name: "Field Name"
				}])(function (entity, closeFunc, unlock) {
					if(common.array.find(entity.field, $scope.fieldList, "field")) {
						$.dialog({
							title: "OPS",
							content: "Field already exist!"
						});

						unlock();
					} else {
						$scope.fieldList.push({
							field: entity.field,
							_customize: true
						});

						closeFunc();
					}
				});
			};

			$scope.removeField = function (field) {
				$scope.fieldList = common.array.remove(field, $scope.fieldList);
			};

			$scope.confirm = function() {
				$scope.lock = true;
				_deferred.notify({
					entity: _entity,
					closeFunc: function() {
						$mdl.modal('hide');
					},
					unlock: function() {
						$scope.lock = false;
					}
				});
			};

			_bindShortcut($mdl);

			return _deferred.promise;
		}

		/***
		 * Create a customize field confirm modal.
		 * @param {string} name							- Create entity name
		 * @param {object} entity						- Bind entity
		 * @param {Object[]} fieldList					- Display fields
		 * @param {string} fieldList[].field				- Mapping entity field
		 * @param {string=} fieldList[].name				- Field display name
		 * @param {*=} fieldList[].defaultValue				- Field default value. Only will be set if entity object is undefined
		 * @param {string=} fieldList[].type				- Field types: 'select', 'blob'
		 * @param {number=} fieldList[].rows				- Display as textarea if rows is set
		 * @param {string=} fieldList[].description			- Display as placeholder
		 * @param {boolean=} fieldList[].optional			- Optional field will not block the confirm
		 * @param {boolean=} fieldList[].readonly			- Read Only can not be updated
		 * @param {string[]=} fieldList[].valueList			- For select type usage
		 * @param {checkFieldFunction=} checkFunc	- Check logic function. Return string will prevent access
		 */
		UI.createConfirm = function(name, entity, fieldList, checkFunc) {
			return wrapPromise(_fieldDialog(true, name, entity, fieldList, checkFunc));
		};

		/***
		 * Create a customize field confirm modal.
		 * @param {object} config						- Configuration object
		 * @param {string} config.title						- Title of dialog box
		 * @param {string=} config.size						- "large". Set dialog size
		 * @param {boolean=} config.addable					- Set add customize field
		 * @param {boolean=} config.confirm					- Display or not confirm button
		 * @param {string=} config.confirmDesc				- Confirm button display description
		 * @param {object} entity						- bind entity
		 * @param {Object[]} fieldList					- Display fields
		 * @param {string} fieldList[].field				- Mapping entity field
		 * @param {string=} fieldList[].name				- Field display name
		 * @param {*=} fieldList[].defaultValue				- Field default value. Only will be set if entity object is undefined
		 * @param {string=} fieldList[].type				- Field types: 'select', 'blob'
		 * @param {number=} fieldList[].rows				- Display as textarea if rows is set
		 * @param {string=} fieldList[].description			- Display as placeholder
		 * @param {boolean=} fieldList[].optional			- Optional field will not block the confirm
		 * @param {boolean=} fieldList[].readonly			- Read Only can not be updated
		 * @param {string[]=} fieldList[].valueList			- For select type usage
		 * @param {checkFieldFunction=} checkFunc	- Check logic function. Return string will prevent access
		 */
		UI.fieldConfirm = function(config, entity, fieldList, checkFunc) {
			return wrapPromise(_fieldDialog("field", config, entity, fieldList, checkFunc));
		};

		UI.deleteConfirm = function (name) {
			var _deferred, $mdl, $scope;

			_deferred = $q.defer();
			$scope = $rootScope.$new(true);
			$scope.name = name;
			$scope.lock = false;

			// Modal
			$mdl = $(TMPL_DELETE).appendTo('body');
			$compile($mdl)($scope);
			$mdl.modal();

			$mdl.on("hide.bs.modal", function() {
				_deferred.reject();
			});
			$mdl.on("hidden.bs.modal", function() {
				_deferred.resolve({
					name: name
				});
				$mdl.remove();
			});

			// Function
			$scope.delete = function() {
				$scope.lock = true;
				_deferred.notify({
					name: name,
					closeFunc: function() {
						$mdl.modal('hide');
					},
					unlock: function() {
						$scope.lock = false;
					}
				});
			};

			return wrapPromise(_deferred.promise);
		};

		return UI;
	});

	// ===========================================================
	// =                         Template                        =
	// ===========================================================
	var TMPL_FIELDS =
		'<div class="modal fade" tabindex="-1" role="dialog">' +
		'<div class="modal-dialog" ng-class="{\'modal-lg\': config.size === \'large\'}" role="document">' +
		'<div class="modal-content">' +
		'<div class="modal-header">' +
			'<button type="button" class="close" data-dismiss="modal" aria-label="Close">' +
				'<span aria-hidden="true">&times;</span>' +
			'</button>' +
			'<h4 class="modal-title">{{config.title || (create ? "New" : "Update") + " " + name}}</h4>' +
		'</div>' +
		'<div class="modal-body">' +
			'<div class="form-group" ng-repeat="field in fieldList" ng-switch="field.type">' +
				'<label for="featureName">' +
					'<span ng-if="!field.optional && !field._customize">*</span> ' +
					'<a ng-if="field._customize" class="fa fa-times" ng-click="removeField(field)"></a> ' +
					'{{field.name || field.field}}' +
				'</label>' +
				'<textarea class="form-control" placeholder="{{getFieldDescription(field)}}" ng-model="entity[field.field]" rows="{{ field.rows || 10 }}" ng-readonly="field.readonly" ng-disabled="lock" ng-switch-when="blob"></textarea>' +
				'<select class="form-control" ng-model="entity[field.field]" ng-init="entity[field.field] = entity[field.field] || field.valueList[0]" ng-switch-when="select">' +
				'<option ng-repeat="value in field.valueList">{{value}}</option>' +
				'</select>' +
				'<input type="text" class="form-control" placeholder="{{getFieldDescription(field)}}" ng-model="entity[field.field]" ng-readonly="field.readonly" ng-disabled="lock" ng-switch-default>' +
			'</div>' +
			'<a ng-if="config.addable" ng-click="newField()">+ New field</a>' +
		'</div>' +
		'<div class="modal-footer">' +
			'<p class="pull-left text-danger">{{checkFunc(entity)}}</p>' +
			'<button type="button" class="btn btn-default" data-dismiss="modal" ng-disabled="lock">Close</button>' +
			'<button type="button" class="btn btn-primary confirmBtn" ng-click="confirm()" ng-disabled="checkFunc(entity) || emptyFieldList().length || lock" ng-if="config.confirm !== false">' +
				'{{config.confirmDesc || (create ? "Create" : "Update")}}' +
			'</button>' +
		'</div>' +
		'</div>' +
		'</div>' +
		'</div>';

	var TMPL_DELETE =
		'<div class="modal fade" tabindex="-1" role="dialog" aria-hidden="true">' +
		'<div class="modal-dialog">' +
		'<div class="modal-content">' +
		'<div class="modal-header">' +
		'<button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button>' +
		'<h4 class="modal-title">Delete Confirm</h4></div>' +
		'<div class="modal-body">' +
		'<span class="text-red fa fa-exclamation-triangle pull-left" style="font-size: 50px;"></span>' +
		'<p>You are <strong class="text-red">DELETING</strong> \'{{name}}\'!</p>' +
		'<p>Proceed to delete?</p>' +
		'</div>' +
		'<div class="modal-footer">' +
		'<button type="button" class="btn btn-danger" ng-click="delete()" ng-disabled="lock">Delete</button>' +
		'<button type="button" class="btn btn-default" data-dismiss="modal" ng-disabled="lock">Cancel</button>' +
		'</div>' +
		'</div>' +
		'</div>' +
		'</div>';
}());
