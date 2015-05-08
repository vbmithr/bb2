#!/usr/bin/env ocaml
#directory "pkg"
#use "topkg.ml"

let () =
  Pkg.describe "breakbot2" ~builder:`OCamlbuild [
    Pkg.bin ~auto:true "src/bb2";
  ]
