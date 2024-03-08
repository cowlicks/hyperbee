extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/schema.proto"], &["src/"]).expect("schema.proto must exist");
    uniffi::generate_scaffolding("src/hyperbee.udl").expect("hyperbee.udl must exist");
}
