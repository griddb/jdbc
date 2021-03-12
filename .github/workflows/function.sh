#!/bin/bash

check_file_exist() {
    local file_path=$1
    if [ ! -f "$file_path" ]; then
        echo "$file_path not found!"
    fi
}

# Check file
check_files() {
    check_file_exist "make_source_for_mvn.sh"
    check_file_exist "pom.xml"
    check_file_exist "sample/en/jdbc/JDBCSelect.java"
}

# Build maven jdbc package
build_jdbc() {
    check_files
    ./make_source_for_mvn.sh
    mvn package
}

# Create and run griddb server
run_griddb_server() {
    docker run -d --network="host" -e GRIDDB_CLUSTER_NAME=${GRIDDB_SERVER_NAME} griddb/griddb:4.5.2-bionic
}

# Turn off firewall
firewall_disable() {
   sudo ufw disable
}

# Get version jdbc driver
get_version() {
    local jdbc_version=$(cat pom.xml | grep "version" |  head -n 1 | cut -d ">" -f 2 | cut -d "<" -f 1)
    echo $jdbc_version
}

# Get name package jdbc driver
get_name() {
    local package_name=$(cat pom.xml | grep "artifactId" |  head -n 1 | cut -d ">" -f 2 | cut -d "<" -f 1)
    echo $package_name
}
# Export PATH and run JDBC sample
run_sample() {
    check_files
    local package_name=$(get_name)
    local jdbc_version=$(get_version)
    export CLASSPATH=${CLASSPATH}:target/$package_name-$jdbc_version.jar
    cp sample/en/jdbc/JDBCSelect.java .
    javac JDBCSelect.java
    java JDBCSelect
}
