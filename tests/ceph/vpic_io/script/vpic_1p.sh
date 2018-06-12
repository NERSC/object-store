#!bin/bash 
#for j in  0.5 1 2 4 
for j in 8 16 32 64 128 256
do 
  for i in 1 2 3 
   do
        echo "particle:$j,test:$i" >> 1node_vpic.log
        ./VPIC vpic_${j}.h5.test.${i} swiftpool $CEPH_CONF ${j} 1024 >> 1node_vpic.log
        #echo "done written vpic_p${i}.t${j}.h5"
   done
done
