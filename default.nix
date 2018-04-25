with import <nixpkgs> {
  overlays = [ (import /home/pe/nixpkgs-mozilla/rust-overlay.nix) ];
};

stdenv.mkDerivation rec {
    name = "nest-env";
    buildInputs = [ rustChannels.stable.rust libgit2 zlib libssh2 cmake git openssl pkgconfig libsodium ];
}
