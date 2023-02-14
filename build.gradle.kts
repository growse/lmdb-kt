import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	kotlin("jvm") version "1.8.10"
	id("maven-publish")
	signing
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
	testImplementation("org.slf4j:slf4j-simple:2.0.6")
	testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.2")
}

tasks.test {
	useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
	kotlinOptions.jvmTarget = "17"
}

java {
	withSourcesJar()
	withJavadocJar()
}

publishing {
	repositories {
		maven {
			name = "SonatypeOSSRH"
			val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
			val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
			url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
			val sonaUserName: String? by project
			val sonaPassword: String? by project
			credentials {
				username = sonaUserName
				password = sonaPassword
			}
		}
		maven {
			// change URLs to point to your repos, e.g. http://my.org/repo
			val releasesRepoUrl = uri(layout.buildDirectory.dir("repos/releases"))
			val snapshotsRepoUrl = uri(layout.buildDirectory.dir("repos/snapshots"))
			url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
		}
	}
	publications {
		create<MavenPublication>("maven") {
			groupId = group.toString()
			artifactId = "lmdb-kt"
			version = version
			from(components["java"])
			pom {
				name.set("LMDB-kt")
				description.set("A pure-kotlin/JVM LMDB library")
				url.set("https://github.com/growse/lmdb-kt")
				licenses {
					license {
						name.set("The MIT License")
						url.set("https://opensource.org/licenses/MIT")
					}
				}
				scm {
					connection.set("scm:git:git://github.com/growse/lmdb-kt.git")
					developerConnection.set("scm:git:ssh://git@github.com:growse/lmdb-kt.git")
					url.set("https://github.com/growse/lmdb-kt")
				}
			}
		}
	}
}

signing {
	val signingKey: String? by project
	val signingPassword: String? by project
	useInMemoryPgpKeys(signingKey, signingPassword)
	sign(publishing.publications["maven"])
}
