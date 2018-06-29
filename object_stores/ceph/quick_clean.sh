#!/bin/bash
ceph osd pool delete swiftpool swiftpool --yes-i-really-really-mean-it
ceph osd pool create swiftpool 1024 1024 replicated
ceph osd pool set swiftpool size 1
