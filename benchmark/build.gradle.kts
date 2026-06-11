import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
  alias(libs.plugins.kotlin)
  alias(libs.plugins.jmh)
}

group = "com.growse"

repositories {
  mavenCentral()
}

dependencies {
  implementation(project(":"))
  implementation("org.lmdbjava:lmdbjava:0.9.3")
  implementation(libs.slf4j)
}

jmh {
  jvmArgsAppend.addAll(
      "--add-opens",
      "java.base/java.nio=ALL-UNNAMED",
      "--add-opens",
      "java.base/sun.nio.ch=ALL-UNNAMED",
  )
}

kotlin { compilerOptions { jvmTarget.set(JvmTarget.JVM_21) } }
