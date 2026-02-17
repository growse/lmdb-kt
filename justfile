set dotenv-load := true

default:
    @just --list

build:
    ./gradlew assemble
