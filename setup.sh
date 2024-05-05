#!/bin/bash

# Step 1: Compile and Package JARs
javac -classpath `hadoop classpath` -d schedule_classes /path/to/Schedule.java
jar -cvf Schedule.jar -C schedule_classes/ .

javac -classpath `hadoop classpath` -d taxitime_classes /path/to/TaxiTime.java
jar -cvf TaxiTime.jar -C taxitime_classes/ .

javac -classpath `hadoop classpath` -d cancellation_classes /path/to/Cancellation.java
jar -cvf Cancellation.jar -C cancellation_classes/ .

# Step 2: Upload to HDFS
USER=$(whoami)
hdfs dfs -mkdir -p /user/${USER}/workflow
hdfs dfs -put -f Schedule.jar /user/${USER}/workflow/
hdfs dfs -put -f Schedule.jar /user/${USER}/workflow/
hdfs dfs -put -f TaxiTime.jar /user/${USER}/workflow/
hdfs dfs -put -f Cancellation.jar /user/${USER}/workflow/
hdfs dfs -put -f workflow.xml /user/${USER}/workflow/

# Step 3: Create job.properties
echo "
nameNode=hdfs://host-placeholder:8020
jobTracker=host-placeholder:8021
userName=${USER}
numYears=5
oozie.wf.application.path=\${nameNode}/user/\${userName}/workflow/workflow.xml
" > job.properties

# Step 4: Run Oozie workflow
# oozie job -config job.properties -run