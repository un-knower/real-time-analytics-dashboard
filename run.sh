#!/usr/bin/env bash
sbt clean assembly
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar hadoop@ec2-34-201-126-64.compute-1.amazonaws.com:
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar ec2-user@ec2-54-85-62-208.compute-1.amazonaws.com:
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar big-hcn01:RealTime_Load/
