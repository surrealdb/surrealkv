# This command builds the project, including all targets, and generates the documentation.
.PHONY: build check linux arm-linux windows macos arm-macos doc

build: check linux arm-linux windows macos arm-macos doc

linux:
	cargo build --target=x86_64-unknown-linux-gnu

arm-linux:
	cargo build --target=aarch64-unknown-linux-gnu

windows:
	cargo build --target=x86_64-pc-windows-msvc

macos:
	cargo build --target=x86_64-apple-darwin

arm-macos:
	cargo build --target=aarch64-apple-darwin

doc:
	cargo doc

# This command checks the licenses of all dependencies, formats the code, and runs the Clippy linter.
check:
	cargo deny --all-features check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

# This command runs the tests with backtrace enabled.
test: check
	RUST_BACKTRACE=1 cargo test
