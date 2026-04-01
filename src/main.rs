use axiom_core::cas::{CasStore, InMemoryCas};
use axiom_core::version::VersionStore;

fn main() {
    let cas_ins = InMemoryCas::new();
    let versions = VersionStore::new();

    let data = b"Hello, World!".to_vec();
    let chunk_hash = cas_ins.put_chunk(data);
    let object_hash = cas_ins.put_object(vec![chunk_hash]);
    println!("Stored chunk with hash: {}", chunk_hash);
    println!("Stored object with hash: {}", object_hash);

    let v1 = versions.commit(vec![], object_hash, "initial commit".into());
    println!("Created version: {}", v1);

    let version = versions.get(&v1).unwrap();
    println!(
        "Version {} has root object hash: {}",
        version.id, version.root
    );
    let retrieved_object = cas_ins.get_object(&version.root).unwrap();
    println!("Retrieved object with hash: {}", retrieved_object.hash);
}
