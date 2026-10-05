pub fn print_case(
    name: impl std::fmt::Display,
    k: usize,
    n: usize,
    row_size: usize,
    workers: usize,
) {
    println!("{name}: K={k} N={n} row_size={row_size} workers={workers}");
}

pub fn print_environment(workers: impl std::fmt::Display) {
    println!("os: {}", std::env::consts::OS);
    println!("arch: {}", std::env::consts::ARCH);
    println!(
        "pkg: {} {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    println!("cpu: {}", cpu_model().as_deref().unwrap_or("unknown"));
    println!("rayon workers: {workers}");
}

fn cpu_model() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        info.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            match key.trim() {
                "model name" | "Hardware" => Some(value.trim().to_owned()),
                _ => None,
            }
        })
    }

    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}
