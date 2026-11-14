//! Print the page rig's kit as one JSON document on stdout.

fn main() {
    let kit = brenn_surface_test_peer::page_rig::kit();
    println!(
        "{}",
        serde_json::to_string_pretty(&kit).expect("the kit serializes")
    );
}
