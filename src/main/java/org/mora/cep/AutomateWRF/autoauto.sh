#!/bin/bash

#set the path to automated.sh
AUTO_PATH="/home/ruveni/IdeaProjects/DataAgent/src/main/java/org/mora/cep/AutomateWRF";

export NETCDF=/usr/local;
cd $AUTO_PATH;
chmod +x automated.sh;
chmod +x automateWRF.sh;
./automateWRF.sh [options] <<-END
32
0
END
./automated.sh [options] <<-END
2
END