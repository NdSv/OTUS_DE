#!/bin/bash
sbt package
mv target/scala-2.11/hw4_2.11-0.1.jar target/scala-2.11/hw4.jar
scp target/scala-2.11/hw4.jar n.sviridenko@rf-cluster.dadadata.ru:hw4