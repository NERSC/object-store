#!/bin/bash
while read p; do
  rados --user jialin -p swiftpool stat $p | awk '{printf $6" "}' && ceph osd map swiftpool $p | awk '{print $10" "$14}'
done <$1
