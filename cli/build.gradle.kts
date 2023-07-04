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
	implementation("io.github.oshai:kotlin-logging-jvm:5.0.0-beta-02")
	implementation("org.slf4j:slf4j-simple:2.0.7")
	implementation("com.github.ajalt.clikt:clikt:4.0.0")
	testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.3")
	testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.3")
}

tasks.getByName<Test>("test") {
	useJUnitPlatform()
}

application {
	mainClass.set("com.growse.lmdb_kt.cli.CliKt")
}
