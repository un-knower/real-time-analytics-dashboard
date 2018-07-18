sbt clean assembly
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar hadoop@ec2-52-91-164-54.compute-1.amazonaws.com:
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar big-hcn01:RealTime_Load/
