rm -rf output
javac -classpath ${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d dikjstra_classes Dikjstra.java
jar -cvf dikjstra.jar -C dikjstra_classes/ .
${HADOOP_HOME}/bin/hadoop jar dikjstra.jar Dikjstra input output