plugins {
    id("java")
    id("com.github.johnrengelman.shadow").version("7.1.2")
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

//tasks.register<Jar>("uberJar") {
//    archiveClassifier.set("uber")
//
//    from(sourceSets.main.get().output)
//
//    dependsOn(configurations.runtimeClasspath)
//    from({
//        configurations.runtimeClasspath.get().filter { it.name.endsWith("jar") }.map { zipTree(it) }
//    })
//}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}