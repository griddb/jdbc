#!/bin/bash

mkdir -p src/main/java
mkdir -p src/main/resources/com/toshiba/mwcloud/gs/sql/internal

./make_source_for_mvn.pl src src/main/java
cp src/com/toshiba/mwcloud/gs/sql/internal/SQLDriver.properties src/main/resources/com/toshiba/mwcloud/gs/sql/internal
cp -r src_contrib/com src/main/java
cp -r src/META-INF src/main/resources
