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
	// ================== AdminLTE Update ==================
	var _boxSelect = $.AdminLTE.options.boxWidgetOptions.boxWidgetSelectors;

	// Box collapse
	$(document).on("click", _boxSelect.collapse, function(e) {
		if(common.getValueByPath($._data(this), "events.click")) return;

		e.preventDefault();
		$.AdminLTE.boxWidget.collapse($(this));
	});

	// Box remove
	$(document).on("click", _boxSelect.remove, function(e) {
		if(common.getValueByPath($._data(this), "events.click")) return;

		e.preventDefault();
		$.AdminLTE.boxWidget.remove($(this));
	});

	// =================== jQuery Update ===================
	// Slide Toggle
	var _slideToggle = $.fn.slideToggle;
	$.fn.slideToggle = function(showOrHide) {
		if(arguments.length === 1 && typeof showOrHide === "boolean") {
			if(showOrHide) {
				this.slideDown();
			} else {
				this.slideUp();
			}
		} else {
			_slideToggle.apply(this, arguments);
		}
	};

	// Fade Toggle
	var _fadeToggle = $.fn.fadeToggle;
	$.fn.fadeToggle = function(showOrHide) {
		if(arguments.length === 1 && typeof showOrHide === "boolean") {
			if(showOrHide) {
				this.fadeIn();
			} else {
				this.fadeOut();
			}
		} else {
			_fadeToggle.apply(this, arguments);
		}
	};

	// Modal
	var _modal = $.fn.modal;
	$.fn.modal = function() {
		setTimeout(function() {
			$(this).find("input[type='text'], textarea").filter(':not([readonly]):enabled:visible:first').focus();
		}.bind(this), 500);
		_modal.apply(this, arguments);
		return this;
	};
})();