#!/bin/bash
PYTHONPATH=modules/ nosetests -w . --with-gae  --gae-lib-root=$1 $2
