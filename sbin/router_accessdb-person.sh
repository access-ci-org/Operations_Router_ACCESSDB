#!/bin/bash

### BEGIN INIT INFO
# Provides:          %APP_NAME%-person
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Route ACCESSDB-Users information from a source to a destination
# Description:       Route ACCESSDB-Users information from a {file, directory, amqp} to a {directory, api}
### END INIT INFO

####### Customizations START #######
APP_NAME=%APP_NAME%-person
APP_HOME=%APP_HOME%
WAREHOUSE_DJANGO=%WAREHOUSE_DJANGO%
# Override in shell environment
if [ -z "$PYTHON_BASE" ]; then
    PYTHON_BASE=%PYTHON_BASE%
fi
####### Customizations END #######

####### Everything else should be standard #######
APP_SOURCE=${APP_HOME}/PROD

APP_LOG=${APP_HOME}/var/${APP_NAME}.daemon.log
if [[ "$1" != --pdb && "$2" != --pdb && "$3" != --pdb && "$4" != --pdb ]]; then
    exec >${APP_LOG} 2>&1
fi

APP_BIN=${APP_SOURCE}/bin/${APP_NAME}.py
APP_OPTS="-l info -c ${APP_HOME}/conf/${APP_NAME}.conf"

PYTHON_BIN=python3
export LD_LIBRARY_PATH=${PYTHON_BASE}/lib
source ${APP_HOME}/python/bin/activate

export PYTHONPATH=${APP_SOURCE}/lib:${WAREHOUSE_DJANGO}
export APP_CONFIG=${APP_HOME}/conf/django_prod_router.conf
export DJANGO_SETTINGS_MODULE=Operations_Warehouse_Django.settings

do_start () {
    echo "Starting ${APP_NAME}:"
    echo "${PYTHON_BIN} ${APP_BIN} $@ ${APP_OPTS}"
    ${PYTHON_BIN} ${APP_BIN} $@ ${APP_OPTS}
    RETVAL=$?
}

case "$1" in
    start)
        do_${1} ${@:2}
        ;;

    *)
        echo "Usage: ${APP_NAME} {start} [<optional_parameters>]"
        exit 1
        ;;

esac
echo rc=$RETVAL
exit $RETVAL
