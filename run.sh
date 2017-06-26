#!/bin/bash
BASE_HOME=$(cd "$(dirname "$0")"; pwd)
mainJar=$BASE_HOME/fulljoin/target/fulljoin-1.0-SNAPSHOT.jar
libjars="$mainJar"
libDir=$BASE_HOME/fulljoin/target/lib/
if [ -d "$binDir" ] ; then
    for jar in $libDir/*.jar
    do
        [ "$libjars" != "" ] && libjars="$libjars,"
        libjars="$libjars$jar"
    done
fi
/home/admin/hadoop/bin/hadoop --config /home/admin/hadoop/etc/hadoop jar $mainJar mr.FullJoin -libjars $libjars -f join.xml


if [ $? == 0 ] ; then
    echo "join SUCCEED"
    exit 0
else
    echo "join FAILED"
    exit 1
fi
