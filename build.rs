use std::{env, path::PathBuf};

fn main() {
    let proto = "./proto/rhino.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let plugins = "#[derive(serde::Serialize, serde::Deserialize)]";

    tonic_build::configure()
        .type_attribute("PublishRequest", plugins)
        .type_attribute("PublishResponse", plugins)
        .type_attribute("SubscriptionRequest", plugins)
        .type_attribute("SubscriptionResponse", plugins)
        .type_attribute("Error", plugins)
        .build_server(true)
        .build_client(false)
        .out_dir(out_dir)
        .compile_protos(&[proto], &["protos"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));

    println!("cargo:rerun-if-changed=protos/genplat.proto");
}
