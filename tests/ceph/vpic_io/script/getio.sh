#!/bin/bash
echo 'io time'
grep '\.' $1 | awk 'NR % 2'

echo 'total time'
grep '\.' $1 | awk 'NR % 2 ==1'
