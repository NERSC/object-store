#!/bin/bash 
tproc=33
sizeperproc=0.03125 # unit: million (particles)
nparticle="$(echo "$tproc*$sizeperproc" | bc)"
echo $nparticle
intnparticle=${nparticle%.*}
echo $intnparticle
