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

fuzz:
    ./gradlew fuzzTest -Pfuzz

# Time-box fuzzing (e.g. just fuzz-for 30m, just fuzz-for 1h)
fuzz-for duration:
    timeout {{ duration }} just fuzz || true
