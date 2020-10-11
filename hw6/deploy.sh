#!/bin/bash
sbt package
mv target/scala-2.11/hw6_2.11-0.1.jar target/scala-2.11/hw6.jar

sbt assembly
mv target/scala-2.11/hw6-assembly-0.1.jar target/scala-2.11/hw6fat.jar

scp target/scala-2.11/hw6*.jar n.sviridenko@rf-cluster.dadadata.ru:hw6