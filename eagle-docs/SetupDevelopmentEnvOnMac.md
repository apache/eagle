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

How to setup the Eagle development environment on Mac
===============================================

This tutorial is based Mac OS X. It can be used as a reference guide for other OS like Linux or Windows as well.  To save your time of jumping back and forth between different web pages, all necessary references will be point out. 

Prerequisite
-------

### - HomeBrew 
Make sure you have HomeBrew installed on your mac. If not, please run:

>\$ ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

you can find more information about HomeBrew at http://brew.sh/ .

### - Scala & SBT
Some core eagle modules are written with scala. To install Scala and SBT, just run:

> $ brew install scala

> $ brew install sbt

### - npm

Eagle-webservice module uses npm. To install it, run:

> $ brew install npm


### - Maven
Eagle is built with maven:
> 
> $ brew install maven


### - HomeBrew Cask 
Install HomeBrew Cask:
> $ brew install caskroom/cask/brew-cask

Next, install JDK via HomeBrew:

> $ brew cask search java

you will see all available JDK versions and you can install multiple JDK versions in this way. For eagle please choose java7 to install:

> $ brew cask install java7

-
> **Note:**
> During this writing SBT has issue with JDK 8. This issue has been tested confirmed by using: 
> -Java 1.8.0_66
> -Maven 3.3.9
> -Scala 2.11.7
> -Sbt 0.13.9

you can find more information about HomeBrew Cask at http://caskroom.io/ .

### - Jenv 

you can use Jenv to manage installed multiple Java versions. To install it:
> $ brew install https://raw.githubusercontent.com/entrypass/jenv/homebrew/homebrew/jenv.rb

and make sure activate it automatically:

> \$ echo 'eval "$(jenv init -)"' >> ~/.bash_profile

-
> **Note:**
> There is a known issue at this writing: https://github.com/gcuisinier/jenv/wiki/Trouble-Shooting
> Please make sure JENV_ROOT has been set before jenv init:
> $ export JENV_ROOT=/usr/local/opt/jenv

Now let Jenv manage JDK versions (remember In OSX all JVMs are located at /Library/Java/JavaVirtualMachines):

> \$ jenv add /Library/Java/JavaVirtualMachines/jdk1.8.0_66.jdk/Contents/Home/
$ jenv add /Library/Java/JavaVirtualMachines/jdk1.7.0_80.jdk/Contents/Home/

and

> $ jenv rehash

You can see all managed JDK versions:

> $ jenv versions

set global java version:

> $ jenv global oracle64-1.8.0.66

switch to your eagle home directory and set the local JDK version for eagle:

> $ jenv local oracle64-1.7.0.80

you can find more information about Jenv at https://github.com/rbenv/rbenv and http://hanxue-it.blogspot.com/2014/05/installing-java-8-managing-multiple.html.

Build Eagle
-----------

Go to Eagle home directory and run:

> mvn -DskipTests clean package

That's all. Now you have runnable eagle on your Mac. Have fun. :-)
