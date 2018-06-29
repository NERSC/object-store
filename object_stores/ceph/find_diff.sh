#!/bin/bash
echo "smallfile largefile > diffile"
grep -vxFf $1 $2 > $3
