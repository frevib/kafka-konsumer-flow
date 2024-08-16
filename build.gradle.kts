plugins {
    kotlin("jvm") version "2.0.0"
}

group = "com.eventloopsoftware"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")

}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("io.confluent:kafka-avro-serializer:7.7.0")
    implementation("io.micrometer:micrometer-registry-prometheus:1.13.1")
    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}