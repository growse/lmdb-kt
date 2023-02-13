import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	kotlin("jvm") version "1.8.10"
	id("maven-publish")
	id("com.adarshr.test-logger") version ("3.2.0")
}

group = "com.growse"
version = "0.1-SNAPSHOT"

repositories {
	mavenCentral()
}

dependencies {
	implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
	testImplementation(kotlin("test"))
	testImplementation("org.slf4j:slf4j-simple:2.0.5")
	testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.2")
}

tasks.test {
	useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
	kotlinOptions.jvmTarget = "17"
}

publishing {
	repositories {
		maven {
			name = "GitHubPackages"
			url = uri("https://maven.pkg.github.com/growse/lmdb-kt")
			credentials {
				username = System.getenv("GITHUB_USERNAME")
				password = System.getenv("GITHUB_TOKEN")
			}
		}
	}
	publications {
		create<MavenPublication>("maven") {
			groupId = group.toString()
			artifactId = "lmdb_kt"
			version = version
			from(components["java"])
		}
	}
}
