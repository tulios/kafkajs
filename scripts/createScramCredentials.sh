#!/bin/bash -e

DEFAULT_KAFKA_USERNAME='testscram'
DEFAULT_PASSWORD_256='testtestscram=256'
DEFAULT_PASSWORD_512='testtestscram=512'

kafka_username=""
password_256=""
password_512="" 

find_container_id() {
  echo $(docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafkajs" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q)
}

usage()
{
    echo "
    Usage: createScramCredentials.sh [OPTIONS] none

    Create a user in test cluster with SCRAM 256 and 512 passwords, uses default credentials if none are passed

    Options:
      -u --user string               Set username to be created
      -p --password string           Set passwords (256 and 512)
      -p256  --p256 string           Sets SCRAM 256 only password 
      -p512  --p512 string           Sets SCRAM 512 only password
      -h --help                      Prints help information and quit 
    "
}

while [ "$1" != "" ]; do
    case $1 in
        -u | --user )           shift
                                kafka_username=$1
                                ;;
        -p | --password )       shift
                                password_256=$1
                                password_512=$1     
                                ;;
        -p256 | --p256 )        shift
                                password_256=$1
                                ;;
        -p512 | --p512 )        shift
                                password_512=$1
                                ;;
        -h | --help )           usage
                                exit 1
                                ;;
        * )                     usage
                                exit 1
                                ;;
    esac
    shift
done

if [ "$kafka_username" = "" ]; then
  kafka_username=$DEFAULT_KAFKA_USERNAME
fi

if [ "$password_256" = "" ]; then
  password_256=$DEFAULT_PASSWORD_256
fi

if [ "$password_512" = "" ]; then
  password_512=$DEFAULT_PASSWORD_512
fi
 
echo 'Registering a user with the following credentials '
echo "username: '${kafka_username}'"
echo "password_256: '${password_256}'"
echo "password_512: '${password_512}'"

docker exec \
 $(find_container_id) \
 bash -c "kafka-configs --zookeeper zookeeper:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=${password_256}],SCRAM-SHA-512=[password=${password_512}]' --entity-type users --entity-name ${kafka_username}"
