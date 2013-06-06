#!/bin/bash

javadoc -d doc -sourcepath src/main/java com.groupon.mapreduce.mongo -overview overview.html com.groupon.mapreduce.mongo.in com.groupon.mapreduce.mongo.out
