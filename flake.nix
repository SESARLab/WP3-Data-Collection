{
  description = "Minimal flake environment";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
  };

  outputs = { self, nixpkgs, flake-utils, pre-commit-hooks }:
    with flake-utils.lib;
    eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        checks = {
          pre-commit-check = pre-commit-hooks.lib.${system}.run {
            src = builtins.path {
              path = ./.;
              name = "Counter";
            };
            hooks = {
              deadnix.enable = true;
              nixpkgs-fmt.enable = true;
              statix.enable = true;
              pylint.enable = true;
              flake8.enable = true;
            };
          };
        };

        devShells.default = pkgs.mkShell {
          venvDir = "./.venv";
          buildInputs = builtins.attrValues {
            inherit (pkgs) python3;
            inherit (pkgs.python3Packages) venvShellHook;
          };

          postVenvCreation = ''
            unset SOURCE_DATE_EPOCH
            # pip install -r Async/requirements.txt
            pip install -r CounteR/requirements.txt 
          '';

          preShellHook = ''
            ${self.checks.${system}.pre-commit-check.shellHook}
          '';
        };

        formatter = pkgs.nixpkgs-fmt;
      });
}
