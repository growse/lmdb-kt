import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	alias(libs.plugins.kotlin)
	id("maven-publish")
	signing
	alias(libs.plugins.test.logger)
	jacoco
	alias(libs.plugins.ktfmt)
}


group = "com.growse"

version = "0.1.2-SNAPSHOT"

repositories { mavenCentral() }

dependencies {
	implementation(libs.kotlin.logging.jvm)
	testImplementation(libs.kotlin.test)
	testImplementation(libs.junit.params)
	testImplementation(libs.slf4j)
	testImplementation("org.lmdbjava:lmdbjava:0.9.0")
}

tasks.test {
	useJUnitPlatform()
	jvmArgs =
		listOf(
			"--add-opens",
			"java.base/java.nio=ALL-UNNAMED",
			"--add-opens",
			"java.base/sun.nio.ch=ALL-UNNAMED",
		)
}

tasks.jacocoTestReport {
	dependsOn(tasks.test) // tests are required to run before generating the report
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

publishing {
	repositories {
		maven {
			name = "SonatypeOSSRH"
			val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
			val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
			url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
			val sonatypeUsername: String? by project
			val sonatypePassword: String? by project
			credentials {
				username = sonatypeUsername
				password = sonatypePassword
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
				developers {
					developer {
						id.set("growse")
						name.set("Andrew Rowson")
						email.set("github@growse.com")
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
