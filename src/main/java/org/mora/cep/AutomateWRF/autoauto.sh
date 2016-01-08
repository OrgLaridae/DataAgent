#!/bin/bash

#set the path to automated.sh
AUTO_PATH="/home/chamil/Playground";

export NETCDF=/opt/netCDF-3.6.0;
cd $AUTO_PATH;
chmod +x automated.sh;
./automated.sh [options] <<-END
32
0
2
END