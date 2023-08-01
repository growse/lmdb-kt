plugins {
	kotlin("jvm") version "1.8.22"
	application
}

group = "com.growse"

repositories {
	mavenCentral()
}

dependencies {
	implementation(project(":"))
	implementation("io.github.oshai:kotlin-logging-jvm:5.0.2")
	implementation("org.slf4j:slf4j-simple:2.0.7")
	implementation("com.github.ajalt.clikt:clikt:4.2.0")
	testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.0")
	testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.0")
}

tasks.getByName<Test>("test") {
	useJUnitPlatform()
}

application {
	mainClass.set("com.growse.lmdb_kt.cli.CliKt")
}
