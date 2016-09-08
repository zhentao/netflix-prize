#!/bin/bash

M=$1
R=$2
U=$3

movie_title=$4
rating_record=$5

hive -f movie.hql -hiveconf M=$M -hiveconf R=$R -hiveconf U=$U -hiveconf movie_title=$movie_title -hiveconf rating_record=$rating_record
