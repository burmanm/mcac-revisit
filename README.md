# mcac-revisit

To build:

```
gradlew shadowJar
```

To deploy to Cassandra, add to conf/cassandra-env.sh:

```
REVISIT_ROOT=/home/replaceme/git/mcac-revisit
JVM_OPTS="$JVM_OPTS -javaagent:${REVISIT_ROOT}/build/libs/mcac-revisit-1.0-SNAPSHOT-all.jar"
```
