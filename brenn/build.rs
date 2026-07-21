// Tells Cargo to re-invoke this build script (and therefore rebuild the
// crate) whenever BRENN_BUILD_ID changes, so bumping the build string
// triggers a rebuild without a full `cargo clean`.
fn main() {
    println!("cargo:rerun-if-env-changed=BRENN_BUILD_ID");
}
