#!/bin/bash

dicex_edi_dir=`dirname $0`
. /etc/profile.d/modules.sh
module load math/epd/7.3-1
module load db/oracle/client/12.1.0
export PATH=${dicex_edi_dir}:${PATH}
dicex_edi.py.sh "$@"
