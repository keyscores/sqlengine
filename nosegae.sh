#!/bin/bash
GAEPATH=`which dev_appserver.py`
GAEPATH=`echo $GAEPATH | sed 's/.*-> //'`
GAEPATH=`dirname $GAEPATH`

PYTHONPATH=modules/ nosetests -w . --with-gae  --gae-lib-root=$GAEPATH $1 $2
