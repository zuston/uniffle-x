use std::env;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // todo: use [protobuf-codegen](https://crates.io/crates/protobuf-codegen) to generate rust code

    // only setup ld library path in debug mode
    let profile = std::env::var("PROFILE").unwrap();
    if profile == "debug" {
        setup_ld_library_path();
    }
    Ok(())
}

fn setup_ld_library_path() {
    // java_home is required now to build and test
    let java_home = env::var("JAVA_HOME").expect("JAVA_HOME must be set");
    let possible_lib_paths = vec![
        format!("{java_home}/jre/lib/amd64/server/"),
        format!("{java_home}/lib/server"),
        format!("{java_home}/jre/lib/server"),
        format!("{java_home}/jre/lib/amd64/server")
    ];
    let lib_jvm_path = possible_lib_paths.iter().find(|&path| {
        let path = Path::new(&path);
        path.exists()
    }).expect("java_home is not valid");
    match env::consts::OS {
        "linux" => {
            let ld_path = env::var_os("LD_LIBRARY_PATH").unwrap_or("".parse().unwrap());
            let ld_path = format!("{}:{}",
                                  ld_path.to_str().unwrap(), lib_jvm_path);
            // this might be anti-pattern, but it works for our current setup
            println!("cargo:rustc-env=LD_LIBRARY_PATH={}", ld_path);
        }
        "macos" => {
            let ld_path = env::var_os("DYLD_LIBRARY_PATH").unwrap_or("".parse().unwrap());
            let ld_path = format!("{}:{}",
                                  ld_path.to_str().unwrap(), lib_jvm_path);
            // this might be anti-pattern, but it works for our current setup
            println!("cargo:rustc-env=DYLD_LIBRARY_PATH={}", ld_path);
        }
        _ => {
            // do nothing
        }
    }
}