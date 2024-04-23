test:
    cargo +nightly nextest run --all-features

fmt:
    cargo +nightly fmt; cargo +nightly clippy --lib --examples --tests --benches --all-features --fix --allow-dirty --allow-staged