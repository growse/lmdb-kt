plugins {
	alias(libs.plugins.kotlin)
	application
}

group = "com.growse"

repositories {
	mavenCentral()
}

dependencies {
	implementation(project(":"))
	implementation(libs.kotlin.logging.jvm)
	implementation(libs.slf4j)
	implementation(libs.clikt)
}

tasks.getByName<Test>("test") {
	useJUnitPlatform()
}

application {
	mainClass.set("com.growse.lmdb_kt.cli.CliKt")
}
