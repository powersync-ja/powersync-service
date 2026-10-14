fn main() {
    println!("cargo:rustc-check-cfg=cfg(native_coverage)");
    napi_build::setup();
}
