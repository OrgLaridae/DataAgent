#!/bin/bash

#Automated runnig of WRF model

#execute like below
#$ ./automated.sh [options] <<-END
#32
#0
#2
#END

START=$(date +%s);

#set paths to following directories
WRF_PATH="/home/ruveni/WRF/AutomatedScripts/WRFV3";
WPS_PATH="/home/ruveni/WRF/AutomatedScripts/WPS";
WRF_EMREAL_PATH="/home/ruveni/WRF/AutomatedScripts/WRFV3/test/em_real";

#set path to data
WRF_DATA="/home/ruveni/WRF/AutomatedScripts/WPS/ungrib/Dakota/";


#WRF Model initial
echo "Navigating to WRF root";
#cd /home/chamil/Playground/WRFV3;
cd $WRF_PATH;
ls;
echo "configuring WRF model...";
./configure;
echo "compiling WRF model for real data case...";
./compile em_real;

END=$(date +%s);
DIFF=$(( $END - $START ));

echo "";
echo "WRF model compiled successfully";
echo "runtime = $DIFF seconds";
echo "";