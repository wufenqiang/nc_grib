#!/usr/bin/env bash
Cur_Dir=$(cd "$(dirname "$0")"; pwd)
cd $Cur_Dir
cd ../

mvn clean
mvn package


version=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' pom.xml`
artifactId=`awk '/<artifactId>[^<]+<\/artifactId>/{gsub(/<artifactId>|<\/artifactId>/,"",$1);print $1;exit;}' pom.xml`
groupId=`awk '/<groupId>[^<]+<\/groupId>/{gsub(/<groupId>|<\/groupId>/,"",$1);print $1;exit;}' pom.xml`

echo "mvn install:install-file -Dfile=.\target\\${artifactId}-${version}.jar -DgroupId=${groupId} -DartifactId=${artifactId} -Dversion=${version} -Dpackaging=jar"
mvn install:install-file -Dfile=\target\\${artifactId}-${version}.jar -DgroupId=${groupId} -DartifactId=${artifactId} -Dversion=${version} -Dpackaging=jar