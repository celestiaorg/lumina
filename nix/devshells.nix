{ pkgs, lib, ... }:
let
  extraTargets = [
    # wasm
    "wasm32-unknown-unknown"
    # android
    "aarch64-linux-android"
    "armv7-linux-androideabi"
    "x86_64-linux-android"
    "i686-linux-android"
  ]
  # ios
  ++ lib.optionals pkgs.stdenv.isDarwin [
    "aarch64-apple-ios"
    "aarch64-apple-ios-sim"
  ];

  # keep the devshell on the same rustc as CI
  channel = (lib.importTOML ../rust-toolchain.toml).toolchain.channel;
  # hash of the rust dist manifest for `channel`; nix prints the correct value on mismatch
  channelSha256 = "sha256-P30Tm3O7vQAE725YtDCDHGjNrSsfZO4us11UwJGZSJo=";

  rustToolchain =
    with pkgs;
    fenix.combine [
      # components
      ((fenix.toolchainOf { inherit channel; sha256 = channelSha256; }).withComponents [
        "cargo"
        "clippy"
        "rustc"
        "rustfmt"
        "rust-analyzer"
      ])
      # extra targets
      (lib.lists.map (
        target: (fenix.targets."${target}".toolchainOf { inherit channel; sha256 = channelSha256; }).rust-std
      ) extraTargets)
    ];

  # rustup-like-ish wrapper for cargo to allow `+nightly` syntax
  cargoWrapper =
    with pkgs;
    writeShellScriptBin "cargo" ''
      if [[ "$1" == +nightly* ]]; then
          shift
          exec env PATH="${fenix.minimal.toolchain}/bin:$PATH" cargo "$@"
      fi
      exec ${rustToolchain}/bin/cargo "$@"
    '';
in
{
  devShells.default = pkgs.mkShell {
    packages = with pkgs; [
      # c/gnu base
      gnumake
      pkg-config
      stdenv

      # rust - toolchain
      (lib.hiPrio cargoWrapper) # should be first in PATH
      rustToolchain
      # rust - wasm
      binaryen # wasm-opt
      chromedriver
      geckodriver
      wasm-bindgen-cli_0_2_108
      wasm-pack
      # rust - other
      cargo-edit
      cargo-ndk
      cargo-udeps

      # javascript
      deno
      nodejs
      nodePackages.typescript-language-server

      # go
      gopls
    ];

    PLAYWRIGHT_BROWSERS_PATH = "${pkgs.playwright-driver.browsers}";
    PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = true;
    WASM_BINDGEN_TEST_TIMEOUT = 120;

    shellHook = ''
      if [ -f /etc/NIXOS ]; then
        # work around playwright issue with finding webkit
        # https://github.com/NixOS/nixpkgs/issues/398324
        export PLAYWRIGHT_HOST_PLATFORM_OVERRIDE="ubuntu-24.04";
      fi
    '';
  };
}
