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

	/**
	 * Check function to check fields pass or not
	 * @callback checkFieldFunction
	 * @param {{}} entity
	 * @return {string}
     */

	var serviceModule = angular.module('eagle.service');

	// ===========================================================
	// =                         Service                         =
	// ===========================================================
	// Feature page
	serviceModule.service('UI', function($rootScope, $q, $compile) {
		var UI = {};

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

			_deferred = $q.defer();
			$scope = $rootScope.$new(true);
			$scope.name = name;
			$scope.entity = entity;
			$scope.fieldList = fieldList;
			$scope.checkFunc = checkFunc;
			$scope.lock = false;
			$scope.create = create;

			$scope.config = typeof name === "object" ? name : {};

			// Modal
			$mdl = $(TMPL_FIELDS).appendTo('body');
			$compile($mdl)($scope);
			$mdl.modal();

			$mdl.on("hide.bs.modal", function() {
				_deferred.reject();
			});
			$mdl.on("hidden.bs.modal", function() {
				_deferred.resolve({
					entity: entity
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
					if(!field.optional && !entity[field.field]) {
						return field.field;
					}
				});
			};

			$scope.confirm = function() {
				$scope.lock = true;
				_deferred.notify({
					entity: entity,
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
		 * Create a creation confirm modal.
		 * @param name			Name title
		 * @param entity		bind entity
		 * @param fieldList	Array. Format: {name, field, type(optional: select, blob), rows(optional: number), description(optional), optional(optional), readonly(optional), valueList(optional)}
		 * @param checkFunc	Check logic function. Return string will prevent access
		 */
		UI.createConfirm = function(name, entity, fieldList, checkFunc) {
			return _fieldDialog(true, name, entity, fieldList, checkFunc);
		};

		/***
		 * Create a update confirm modal.
		 * @param name			Name title
		 * @param entity		bind entity
		 * @param fieldList	Array. Format: {name, field, type(optional: select, blob), rows(optional: number), description(optional), optional(optional), readonly(optional), valueList(optional)}
		 * @param checkFunc	Check logic function. Return string will prevent access
		 */
		UI.updateConfirm = function(name, entity, fieldList, checkFunc) {
			return _fieldDialog(false, name, entity, fieldList, checkFunc);
		};

		/***
		 * Create a customize field confirm modal.
		 * @param {object} config					- Configuration object
		 * @param {string=} config.title				- Title of dialog box
		 * @param {string=} config.size				- "large". Set dialog size
		 * @param {boolean=} config.confirm			- Display or not confirm button
		 * @param {string=} config.confirmDesc		- Confirm button display description
		 * @param {object} entity					- bind entity
		 * @param {{name:string, field:string,type:('select'|'blob'),rows:number,description:string,optional:boolean,readonly:boolean,valueList:Array}[]} fieldList - Display fields
		 * @param {checkFieldFunction=} checkFunc	- Check logic function. Return string will prevent access
		 */
		UI.fieldConfirm = function(config, entity, fieldList, checkFunc) {
			return _fieldDialog("field", config, entity, fieldList, checkFunc);
		};

		UI.deleteConfirm = function(name) {
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

			return _deferred.promise;
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
								'<span ng-if="!field.optional">*</span> ' +
								'{{field.name || field.field}}' +
							'</label>' +
							'<textarea class="form-control" placeholder="{{getFieldDescription(field)}}" ng-model="entity[field.field]" rows="{{ field.rows || 10 }}" ng-readonly="field.readonly" ng-disabled="lock" ng-switch-when="blob"></textarea>' +
							'<select class="form-control" ng-model="entity[field.field]" ng-init="entity[field.field] = entity[field.field] || field.valueList[0]" ng-switch-when="select">' +
								'<option ng-repeat="value in field.valueList">{{value}}</option>' +
							'</select>' +
							'<input type="text" class="form-control" placeholder="{{getFieldDescription(field)}}" ng-model="entity[field.field]" ng-readonly="field.readonly" ng-disabled="lock" ng-switch-default>' +
						'</div>' +
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
})();
