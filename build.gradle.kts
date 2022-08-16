plugins {
    id("java")
    id("com.github.johnrengelman.shadow").version("7.1.2")
    id("me.champeau.jmh").version("0.6.6")
}

group = "io.k8ssandra"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.cassandra:cassandra-all:4.0.5")
    implementation("net.bytebuddy:byte-buddy:1.12.13")
    implementation("io.prometheus:simpleclient_httpserver:0.16.0")
    implementation("io.prometheus:simpleclient_dropwizard:0.16.0")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testImplementation("com.codahale.metrics:metrics-core:3.0.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.0")
    testRuntimeOnly("org.apache.cassandra:cassandra-all:4.0.5")
}

tasks.jar {
    manifest {
        attributes(
            "Premain-Class" to "io.k8ssandra.metrics.Agent",
            "Can-Redefine-Classes" to true,
            "Can-Retransform-Classes" to true
        )
    }
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}

jmh {
    warmupIterations.set(1)
    iterations.set(1)
    fork.set(1)
    jmhVersion.set("1.35")
    profilers.add("async:libPath=/Users/michael.burman/Downloads/async-profiler-2.8.3-macos/build/libasyncProfiler.so;output=flamegraph;event=cpu;verbose=true")
}
