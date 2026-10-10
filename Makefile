# This command builds the project, including all targets, and generates the documentation.
build: check
	cargo build --all-targets
	cargo doc

# This command checks the licenses of all dependencies, formats the code, and runs the Clippy linter.
check:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

# This command runs the tests with backtrace enabled.
test:
	RUST_BACKTRACE=1 cargo test --workspace

.PHONY: build-cli
build-cli:
	cargo build -p surrealkv-cli

.PHONY: install-cli
install-cli:
	cargo install --path crates/surrealkv-cli

# This command regenerates CARGO.md, the crates.io readme, from README.md.
.PHONY: readme
readme:
	scripts/sync-crate-readme.sh
