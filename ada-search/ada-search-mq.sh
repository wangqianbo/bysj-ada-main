#!/bin/bash

libs=`ls target/lib/*|tr '\n' ':'`.
java -cp target/ada-search-1.0.0-SNAPSHOT.jar:$libs ict.ada.search.AdaSearchMQService $*
