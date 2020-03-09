#!/bin/bash

APP_NAME=Merlion
APP_VERSION=1.0
JAR_NAME=${APP_NAME}-${APP_VERSION}.jar
#export JAVA_HOME=/opt/java8/

# 启动时间版本

DATE_VERSION=$(date +%Y%m%d%H%M%S)

# 生产配置
BIN_DIR=/usr/local/release/${APP_NAME}/
PID_FILE=/tmp/${APP_NAME}.pid
LOG_FILE=/tmp/${APP_NAME}.${DATE_VERSION}.log
MAIN_CLASS=com.dxy.data.merlion.service.DevApplication

export GLOG_v=1
export GLOG_log_dir=/tmp/a
# export LIBPROCESS_IP=10.25.26.135

# 自定义配置
export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

if [ -z "${DXY_PROJECT_HOME}" ]; then
  export DXY_PROJECT_HOME="$(cd "`dirname "$0"`"/.; pwd)"
fi

JAVA_OPTS="-Dserver.port=8100 -Dfile.encoding=UTF-8 -Dlog4j.configuration=file://${DXY_PROJECT_HOME}/conf/log4j.properties -Dlog4j.configurationFile=file://${DXY_PROJECT_HOME}/conf/log4j2.xml"

JAVA_OPTS=${JAVA_OPTS}" -Dsentry.dsn=https://87727747fd244d61a8dda6339cb1b657:48ed7ab92a774313bca9e326653c4831@sentry.k8s.uc.host.dxy/82"


JAVA_OPTS=${JAVA_OPTS}" -Xms8g -Xmx8g \
          -XX:ParallelGCThreads=8 \
          -XX:SurvivorRatio=1 \
          -XX:LargePageSizeInBytes=128M \
          -XX:MaxNewSize=1g  \
          -XX:CMSInitiatingOccupancyFraction=80 \
          -XX:+UseCMSCompactAtFullCollection \
          -XX:CMSFullGCsBeforeCompaction=0 \
          -XX:-UseGCOverheadLimit \
          -XX:MaxTenuringThreshold=5  \
          -XX:GCTimeRatio=19  \
          -XX:+UseConcMarkSweepGC \
          -XX:+UseParNewGC \
          -XX:+PrintGCDetails \
          -XX:+PrintGCTimeStamps \
          -XX:+HeapDumpOnOutOfMemoryError \
          -XX:HeapDumpPath=/tmp/${APP_NAME}-${MODULE}.dump \
          -Xloggc:/tmp/${APP_NAME}-${MODULE}-gc.$DATE_VERSION.log"
JARS=$(echo ${DXY_PROJECT_HOME}/libs/*.jar | tr ' ' ':')

function status() {
    echo "$APP_NAME Status"
    if [ -s ${PID_FILE} ]; then
        ps h -fp $(cat ${PID_FILE})
    fi
}

function common_run() {
   $JAVA_HOME/bin/java -cp $JARS "$@"
}

function package() {
    cd $DXY_PROJECT_HOME
    echo "Remove ${APP_NAME}-bin-${APP_VERSION}.tgz..."
    rm ${APP_NAME}-bin-${APP_VERSION}.tgz
    echo "Package App $APP_NAME-$APP_VERSION"
    mvn clean package -DskipTests "$@"
    mv $DXY_PROJECT_HOME/assembly/${APP_NAME}-bin-${APP_VERSION}.tgz $DXY_PROJECT_HOME
}

function usage() {
cat << EOF
  Usage: ./bootstrap.sh package
EOF
}

function echo_build_properties() {
  echo version=$APP_VERSION
  echo user=$USER
  echo revision=$(git rev-parse HEAD)
  echo branch=$(git rev-parse --abbrev-ref HEAD)
  echo date=$(date +"%Y/%m/%d %H:%M:%S")
  echo url=$(git config --get remote.origin.url)
}

function build_info() {
    echo_build_properties $2 > $DXY_PROJECT_HOME/INFO
}

function start_frontend() {
    $JAVA_HOME/bin/java $JAVA_OPTS -cp $JARS $MAIN_CLASS
}

function start() {
    echo "Start App $APP_NAME"
    if [ -s ${PID_FILE} ]; then
      r=`ps h -fp $(cat ${PID_FILE})`
    fi
    if [ "$r" == "" ]; then
        nohup $JAVA_HOME/bin/java $JAVA_OPTS -cp $JARS $MAIN_CLASS > ${LOG_FILE} 2>&1 & echo $! > ${PID_FILE}
        echo "${APP_NAME} log file ${LOG_FILE}"
    else
      echo "${APP_NAME} already running..."
    fi
}

function stop() {
    echo "Stop App $APP_NAME"
    if [ -s ${PID_FILE} ]; then
        echo "stopping ${APP_NAME}: $(cat ${PID_FILE})"
        kill -9 $(cat ${PID_FILE})
        rm -f ${PID_FILE}
    else
        echo "pid file not found"
        exit 1
    fi
}

function restart() {
    stop
    start
}

case "$1:$2:$3" in
    package:*)
        package "${@:2}"
        ;;
    run:*:*)
        common_run "${@:2}"
        ;;
    build_info:*:*|bi:*:*)
        build_info
        ;;
    start_frontend:*)
        start_frontend
        ;;
    start:*)
        start
        ;;
    stop:*)
        stop 
        ;;
    restart:*)
        restart
        ;;
    h|help)
        usage 
        ;;
    *)
        usage
        exit 0
esac

