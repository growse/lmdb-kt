plugins {
	kotlin("jvm") version "1.8.10"
	application
}

group = "com.growse"

repositories {
	mavenCentral()
}

dependencies {
	implementation(project(":"))
	implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
	implementation("org.slf4j:slf4j-simple:2.0.6")
	implementation("com.github.ajalt.clikt:clikt:3.5.2")
	testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
	testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.2")
}

tasks.getByName<Test>("test") {
	useJUnitPlatform()
}

application {
	mainClass.set("com.growse.lmdb_kt.cli.CliKt")
}
