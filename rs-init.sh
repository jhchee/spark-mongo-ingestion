#!/bin/bash

mongo <<EOF
var config = {
    "_id": "dbrs",
    "version": 1,
    "members": [
        {
            "_id": 1,
            "host": "mongo1:27017",
            "priority": 1
        },
        {
            "_id": 2,
            "host": "mongo2:27017",
            "priority": 2
        }
    ]
};
rs.initiate(config, { force: true });
rs.status();
EOF