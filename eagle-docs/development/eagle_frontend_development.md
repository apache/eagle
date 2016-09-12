<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Eagle Frontend Development Guide
-----------------------------------------------------
Eagle provide easy way for customize UI development.
If you want to build customize page, you can follow [Customize Pages](#customize-pages)

### Frontend Design
Eagle frontend (using [AngularJS](https://angularjs.org/)) provide multi site support. Each site can use several applications which is composed of features.

#### Feature
Feature is basic UI element. It provide the pages and interactive logic but have no state by itself.
When frontend call the feature page, feature can get current `site`, `application` and also about the configuration from them.
With the configuration to process the logic. Current Eagle pages (excluded `configuration` and `login` page), all are built on features.

Feature should not share any code between each others. If you need code sharing, put them into one feature.

#### Application
Application is logic component which is composed of features. It provide the configuration for feature usage.
Application can be share between sites.
For example `hiveQueryLog`, `hdfsAuditLog` and `hbaseSecurityLog` are the default applications provided by Eagle.

Application has the additional attribute called `groupName`.
You can group the same duty application into one group and Eagle frontend will help to maintain them in navigation.

#### Site
Site can include multi applications.
Each application provide special configuration content so that you can defined the application logic which will read by feature.

### Customize Pages
Customize pages is all included in `eagle-webservice/src/main/webapp/app/public/feature` folder.
You can see the original pages provided by Eagle frontend are also designed as `feature`.
After maven building, all the features will compress in the tar file.
But you can also just copy feature into `lib/tomcat/webapps/eagle-service/ui/feature/` and restart tomcat. It will also works.

#### Build a Customize Feature Page
After reading follow steps, we recommend you can view the features already in Eagle.
Which can help your understanding more fast.

##### Step 1
Create a folder under `eagle-webservice/src/main/webapp/app/public/feature`. create follow files in it:
* controller.js
* page (folder)

##### Step 2
In `controller.js` code the feature entry function.
``` javascript
(function() {
    'use strict';

    var featureControllers = angular.module("featureControllers");
    var feature = featureControllers.register("yourFeatureName");
});
```
`featureControllers` provide the `register` function to register the feature in frontend.
When call the `feature`, Eagle will transform the request to the feature.

##### Step 3
Same as native angularJS, using `controller` to create a page controller.
``` javascript
feature.controller('yourCustomizePageName', function($scope, PageConfig, Site, Application, ...) {
    // do something
});
```

After register the `controller`, Eagle will auto create the route mapping for the page. Like `#/yourFeatureName/yourCustomizePageName`.
Now create the `yourCustomizePageName.html` in `page` folder and Eagle will automatic link the controller with it.

This document do not provide the angularJS guide.
If you are not familiar with angularJS, please click [HERE](https://angularjs.org/) for study.

##### Step 4
After build pages, register them in navigation sidebar:
``` javascript
feature.navItem("yourCustomizePageName", "navigationDisplayName", "navigationDisplayIcon");
```

`navigationDisplayIcon` use the icon from [Font Awesome](https://fortawesome.github.io/Font-Awesome/).
You can set the icon name without `fa fa-` prefix.

PS: You need not register all the pages in navigation bar.
Just register some of the pages and use link with others to keep your side navigation clean.

#### Build a Customize Management Feature Page
Most of them are the same as [Build a Customize Feature Page](#build-a-customize-feature-page),
just change to use `configController` to register the page controller and `configNavItem` to register management side navigation.