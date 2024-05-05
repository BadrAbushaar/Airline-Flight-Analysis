#!/bin/bash

# Step 1: Compile and Package JARs
javac Schedule.java -cp $(hadoop classpath)
jar cf Schedule.jar Schedule*.class

javac TaxiTime.java -cp $(hadoop classpath)
jar cf TaxiTime.jar TaxiTime*.class

javac Cancellation.java -cp $(hadoop classpath)
jar cf Cancellation.jar Cancellation*.class

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