# mesos_starvation
Mesos Starvation Bug patch files

This patch will work only with mesos-0.19.0.

## Instructions:

    - get a clean version of mesos-0.19.0
    
    - copy all the files (.cpp and .hpp) in mesos/src/master folder
    
    - build mesos using instructions from mesos official website
    
    - if you are using hadoop-mapreduce with mesos, in core-site.xml specify role=hadoop otherwise hadoop won't release resources when all the tasks are done.
