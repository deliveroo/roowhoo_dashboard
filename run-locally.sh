#!/bin/bash
invalid_args() {

    printf "***************************\n"
    printf "* Error: Invalid argument.*\n"
    printf "* Pleas send these args: *\n"
    printf "     --kafka_username \n"
    printf "     --kafka_password \n"
    printf "     --broker_bootstrap \n"
    printf "     --broker_protocol \n"
    printf "     --zookeeper_host \n"
    printf "     --zookeeper_port \n"
    printf "***************************\n"
    exit 1
}
while [ $# -gt 0 ]; do
  case "$1" in
    --kafka_username=*)    kafka_username="${1#*=}";;
    --kafka_password=*)    kafka_password="${1#*=}";;
    --broker_bootstrap=*)  broker_bootstrap="${1#*=}";;
    --broker_protocol=*)   broker_protocol="${1#*=}";;
    --zookeeper_host=*)    zookeeper_host="${1#*=}";;
    --zookeeper_port=*)    zookeeper_port="${1#*=}";;
    *)
      invalid_args
  esac
  shift
done


[ -z "$kafka_username" ] && invalid_args
[ -z "$kafka_password" ] && invalid_args
[ -z "$broker_bootstrap" ] && invalid_args
[ -z "$broker_protocol" ] && invalid_args
[ -z "$zookeeper_host" ] && invalid_args
[ -z "$zookeeper_port" ] && invalid_args

export BROKER_PROTOCOL=$broker_protocol
export ZOOKEEPER_PORT=$zookeeper_port
export ZOOKEEPER_HOST=$zookeeper_host
export BROKER_BOOTSTRAP=$broker_bootstrap
export KAFKA_USERNAME=$kafka_username
export KAFKA_PASSWORD=$kafka_password
export DEPLOYMENT_ENV=dev

sbt run
