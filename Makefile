# This command builds the project, including all targets, and generates the documentation.
build: check
	cargo build --all-targets
	cargo doc

# This command checks the licenses of all dependencies, formats the code, and runs the Clippy linter.
check:
	cargo deny --all-features check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

# This command runs the tests with backtrace enabled.
test:
	RUST_BACKTRACE=1 cargo test

.PHONY: build-cli
build-cli:
	cargo build -p skv-cli

.PHONY: install-cli
install-cli:
	cargo install --path cli