#!/usr/bin/env bash

rm -rf /usr/local/preston/
unlink /usr/bin/preston
unlink /etc/bash_completion.d/preston-complete

userdel -f preston-bioguoda

if grep -q -E "^preston-bioguoda:" /etc/group;
then
  groupdel preston-bioguoda
fi