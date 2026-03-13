set dotenv-load := true

default:
    @just --list

build:
    ./gradlew assemble

fmt:
    ./gradlew ktfmtFormat

test:
    ./gradlew test

test-unit:
    ./gradlew unitTest

test-integration:
    ./gradlew integrationTest
