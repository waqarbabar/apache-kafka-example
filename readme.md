###1- Download and install java 1.8 set the path in environment variable

###2-  Download apache kafka 
1- From: https://kafka.apache.org/downloads from 2.6.0 select Scala 2.13 or later version

###3- Setup Environment

1- Extract and place it under root folder

2- Open the extracted folder and you should be able to see bin, config, libs etc. directory structure

3- Create a new folder  with the name 'data' into kafka root folder

4- Into the data folder create two new folders with the names 'kafka' and 'zookeeper'

5- Open config folder and open server.properties file. Replace the 'log.dirs' value with newly created 'kafka' folder path. (path should be with forward slashes)

6- Open config folder and open zookeeper.properties file. Replace the 'dataDir' value with newly created 'zookeeper' folder path. (path should be with forward slashes)

###4- Set Path

1- Put the complete path of `<basepath>\bin\windows` in the environment variables and append it with path

###5- Test setup

1- Run command from kafka root folder `zookeeper-server-start.bat config\zookeeper.properties`

2- Run command from kafka root folder `kafka-server-start.bat config\server.properties`

 
    




