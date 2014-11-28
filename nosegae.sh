#!/bin/bash
GAEPATH=`which dev_appserver.py`

if [ -L $GAEPATH ]; then
    GAEPATH=`echo $GAEPATH | sed 's/.*-> //'`
fi

GAEPATH=`dirname $GAEPATH`

PYTHONPATH=modules/:modules/networkx-1.9.1/ nosetests -w . --with-gae  --gae-lib-root=$GAEPATH $1 $2