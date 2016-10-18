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

eagleComponents.directive('uieSortable', function($rootScope) {
	'use strict';

	var COLLECTION_MATCH = /^\s*([\s\S]+?)\s+in\s+([\s\S]+?)(?:\s+as\s+([\s\S]+?))?(?:\s+track\s+by\s+([\s\S]+?))?\s*$/;

	var _move = false;
	var _selectElement;
	var _overElement;
	var _mockElement;
	var _offsetX, _offsetY;
	var _mouseDownPageX, _mouseDownPageY;

	function doMock(element, event) {
		var _offset = element.offset();
		if(_mockElement) _mockElement.remove();

		// Create mock element
		_mockElement = element.clone(false).appendTo("body");
		_mockElement.addClass("sortable-mock-element");
		_mockElement.css({
			display: "block",
			position: "absolute",
			"pointer-events": "none",
			"z-index": 10001,
			padding: element.css("padding"),
			margin: element.css("margin")
		});
		_mockElement.width(element.width());
		_mockElement.height(element.height());

		_mockElement.offset(_offset);
		_offsetX = event.pageX - _offset.left;
		_offsetY = event.pageY - _offset.top;
	}

	$(window).on("mousemove", function(event) {
		if(!_move) return;
		event.preventDefault();

		_mockElement.offset({
			left: event.pageX - _offsetX,
			top: event.pageY - _offsetY
		});
	});

	$(window).on("mouseup", function() {
		if(!_move) {
			_overElement = null;
			_selectElement = null;
			_mockElement = null;
			return;
		}
		_move = false;

		if(_overElement) {
			_overElement.removeClass("sortable-enter");

			if(_overElement[0] !== _selectElement[0]) {
				// Process switch
				var _oriHolder = _selectElement.holder;
				var _tgtHolder = _overElement.holder;
				var _oriSortableScope = _oriHolder.scope;
				var _tgtSortableScope = _tgtHolder.scope;
				var _oriScope = angular.element(_selectElement).scope();
				var _tgtScope = angular.element(_overElement).scope();

				var _oriRepeat = _selectElement.closest("[ng-repeat]").attr("ng-repeat");
				var _tgtRepeat = _overElement.closest("[ng-repeat]").attr("ng-repeat");
				var _oriMatch = _oriRepeat.match(COLLECTION_MATCH)[2];
				var _tgtMatch = _tgtRepeat.match(COLLECTION_MATCH)[2];
				var _oriCollection = _oriScope.$parent.$eval(_oriMatch);
				var _tgtCollection = _tgtScope.$parent.$eval(_tgtMatch);
				var _oriIndex = $.inArray(_oriCollection[_oriScope.$index], _oriSortableScope.ngModel);
				var _tgtIndex = $.inArray(_tgtCollection[_tgtScope.$index], _tgtSortableScope.ngModel);

				var _oriUnit = _oriSortableScope.ngModel[_oriIndex];
				var _tgtUnit = _tgtSortableScope.ngModel[_tgtIndex];
				_oriSortableScope.ngModel[_oriIndex] = _tgtUnit;
				_tgtSortableScope.ngModel[_tgtIndex] = _oriUnit;

				// Trigger event
				_oriHolder.change(_oriUnit, _tgtUnit);
				if (_oriHolder !== _tgtHolder) _tgtHolder.change(_oriUnit, _tgtUnit);

				$rootScope.$apply();
			}
		}

		if(_mockElement) _mockElement.remove();

		_overElement = null;
		_selectElement = null;
		_mockElement = null;
	});

	return {
		require: 'ngModel',
		restrict : 'AE',
		scope: {
			ngModel: "=",
			sortableEnabled: "=?sortableEnabled",
			sortableUpdateFunc: "=?sortableUpdateFunc"
		},
		link: function($scope, $element, $attrs, $ctrl) {
			var _holder = {
				scope: $scope,
				change: function(source, target) {
					if($scope.sortableUpdateFunc) $scope.sortableUpdateFunc(source, target);
				}
			};

			$element.on("mousedown", ">", function(event) {
				if($scope.sortableEnabled === false) return;

				_selectElement = $(this);
				_selectElement.holder = _holder;

				_mouseDownPageX = event.pageX;
				_mouseDownPageY = event.pageY;

				event.preventDefault();
			});

			$element.on("mousemove", ">", function(event) {
				if(_selectElement && !_move && common.math.distance(_mouseDownPageX, _mouseDownPageY, event.pageX, event.pageY) > 10) {
					_move = true;
					_overElement = _selectElement;
					_overElement.addClass("sortable-enter");

					doMock(_selectElement, event);
				}
			});

			$element.on("mouseenter", ">", function() {
				if(!_move) return;
				_overElement = $(this);
				_overElement.holder = _holder;
				_overElement.addClass("sortable-enter");
			});
			$element.on("mouseleave", ">", function() {
				if(!_move) return;
				$(this).removeClass("sortable-enter");
				_overElement = null;
			});
		},
		replace: false
	};
});