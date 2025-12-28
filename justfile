default:
    @just --list
# Format Kotlin sources using ktfmt
format:
	./gradlew ktfmtFormat --quiet

# Build the project
build:
	./gradlew assemble --quiet

# Run tests
test:
	./gradlew test
