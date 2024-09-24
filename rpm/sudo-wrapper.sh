#!/bin/bash
if [[ $INTERPRETER_GROUP_ID =~ ^spark ]]; then
    bash -c "$@";
else
    sudo mkhomedir_helper "${ZEPPELIN_IMPERSONATE_USER}"
    sudo -H -i -u "${ZEPPELIN_IMPERSONATE_USER}" env "PYTHONPATH=/opt/teragrep/pyz_01" bash -c "$@"
fi;
