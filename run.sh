#!/usr/bin/env bash
sbt clean assembly
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar hadoop@<ec2-instance>:
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar ec2-user@<ec2-instance>:
scp ../realtime_load/target/RealTime_Load-assembly-1.0-SNAPSHOT.jar <on-prem>:RealTime_Load/
