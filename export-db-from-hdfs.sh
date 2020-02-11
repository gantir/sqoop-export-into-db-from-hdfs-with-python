#!/bin/bash

name="$1"
if [ -z "$name" ]
then
	echo "Please define endpoint. Example : ./script_name endpoint"
	
else
	echo "Listing all directories into HDFS with endpoint $name"
	
	results=$(hdfs dfs -ls  -R "/user/$USER/${name}")
	
	echo $results
fi



