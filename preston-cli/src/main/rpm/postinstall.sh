#!/usr/bin/env bash

ln -s /usr/local/preston/bin/preston-cli.sh /usr/bin/preston
ln -s /usr/local/preston/bin/preston_completion /etc/bash_completion.d/preston-complete

chown preston-bioguoda.preston-bioguoda /usr/local/preston/bin/preston-cli.sh
chown preston-bioguoda.preston-bioguoda /usr/local/preston/bin/preston_completion
