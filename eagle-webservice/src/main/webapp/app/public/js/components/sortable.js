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

eagleComponents.directive('sortable', function($rootScope) {
	'use strict';

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
				var _oriIndex = angular.element(_selectElement).scope().$index;
				var _tgtIndex = angular.element(_overElement).scope().$index;
				var _oriHolder = _selectElement.holder;
				var _tgtHolder = _overElement.holder;
				var _oriScope = _oriHolder.scope;
				var _tgtScope = _tgtHolder.scope;

				var _oriUnit = _oriScope.ngModel[_oriIndex];
				var _tgtUnit = _tgtScope.ngModel[_tgtIndex];
				_oriScope.ngModel[_oriIndex] = _tgtUnit;
				_tgtScope.ngModel[_tgtIndex] = _oriUnit;

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
			updateFunc: "=?updateFunc"
		},
		link: function($scope, $element, $attrs, $ctrl) {
			var _holder = {
				scope: $scope,
				change: function(source, target) {
					if($scope.updateFunc) $scope.updateFunc(source, target);
				}
			};

			$element.on("mousedown", ">", function(event) {
				_selectElement = $(this);
				_selectElement.holder = _holder;

				_mouseDownPageX = event.pageX;
				_mouseDownPageY = event.pageY;

				event.preventDefault();
			});

			$element.on("mousemove", ">", function(event) {
				if(_selectElement && !_move && common.math.distance(_mouseDownPageX, _mouseDownPageY, event.pageX, event.pageY) > 10) {
					console.log("RRRT",common.math.distance(_mouseDownPageX, _mouseDownPageY, event.pageX, event.pageY));
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