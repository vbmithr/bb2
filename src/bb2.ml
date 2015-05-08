open Core.Std
open Async.Std
open Graph

module G = Imperative.Digraph.ConcreteBidirectionalLabeled(String)
    (struct type t = float let compare = Pervasives.compare let default = 1. end)
module D = Graphviz.Dot
    (struct
      include G
      let edge_attributes e = [`Label (Float.to_string @@ G.E.label e); `Color 4711]
      let default_edge_attributes _ = []
      let get_subgraph _ = None
      let vertex_attributes _ = [`Shape `Box]
      let vertex_name v = v
      let default_vertex_attributes _ = []
      let graph_attributes _ = [`Concentrate true]
    end)

module BF = Path.BellmanFord(G)
    (struct
      type edge = G.E.t
      type t = float
      let weight e = Float.neg @@ log @@ G.E.label e
      let compare = Float.compare
      let add = Float.add
      let zero = Float.zero
    end)

module StringSet = Set.Make(String)

let bittrex () =
  let module B = Bittrex.Bittrex(Bittrex_async.Bittrex) in
  let g = G.create () in
  B.MarketSummary.summaries () >>= fun markets ->
  Format.printf "Found %d markets.@." @@ List.length markets;
  let currs = ref StringSet.empty in
  List.iter markets ~f:(fun m ->
      let open B.MarketSummary in
      let dash = String.index_exn m.market_name '-' in
      let c1 = String.sub m.market_name 0 dash in
      let c2 = String.sub m.market_name (dash+1) (String.length m.market_name - dash - 1) in
      currs := StringSet.add !currs c1;
      currs := StringSet.add !currs c2;
      let c1 = if Char.is_digit c1.[0] then "\"" ^ c1 ^ "\"" else c1 in
      let c2 = if Char.is_digit c2.[0] then "\"" ^ c2 ^ "\"" else c2 in
      Format.printf "Processing %s-%s %f %f.@." c1 c2 m.bid m.ask;
      G.add_vertex g c1;
      G.add_vertex g c2;
      G.add_edge_e g (G.E.create c1 (1. /. m.ask) c2);
      G.add_edge_e g (G.E.create c2 m.bid c1)
    );

  StringSet.iter !currs ~f:(fun c ->
      (* Format.printf "Searching for negative cycle starting from %s.@." c; *)
      let cycle = try
          BF.find_negative_cycle_from g c
        with Not_found -> [] in
      if cycle <> [] then
        Format.printf "Negative cycle found.@."
    );
  D.fprint_graph Format.str_formatter g;
  let contents = Format.flush_str_formatter () in
  Writer.save "bittrex.dot" ~contents >>| fun () ->
  Format.printf "Done processing.@."

let cryptsy () =
  let module C = Bittrex.Cryptsy(Bittrex_async.Cryptsy) in
  let g = G.create () in
  let ht = Int.Table.create () in
  let currs = ref StringSet.empty in
  C.Ticker.tickers () >>= fun tickers ->
  List.iter tickers ~f:(fun t ->
      let open C.Ticker in
      Hashtbl.add_exn ht ~key:t.id ~data:t
    );
  C.Market.markets () >>= fun markets ->
  List.iter markets ~f:(fun m ->
      let open C.Market in
      let slash = String.index_exn m.label '/' in
      let c1 = String.sub m.label 0 slash in
      let c2 = String.sub m.label (slash+1) (String.length m.label - slash - 1) in
      currs := StringSet.add !currs c1;
      currs := StringSet.add !currs c2;
      let c1 = if Char.is_digit c1.[0] then "\"" ^ c1 ^ "\"" else c1 in
      let c2 = if Char.is_digit c2.[0] then "\"" ^ c2 ^ "\"" else c2 in
      let bidask = Hashtbl.find_exn ht m.id in
      let open C.Ticker in
      Format.printf "Processing %s-%s %f %f.@." c1 c2 bidask.bid bidask.ask;
      G.add_vertex g c1;
      G.add_vertex g c2;
      G.add_edge_e g (G.E.create c1 (1. /. bidask.ask) c2);
      G.add_edge_e g (G.E.create c2 bidask.bid c1)
    );

  StringSet.iter !currs ~f:(fun c ->
      (* Format.printf "Searching for negative cycle starting from %s.@." c; *)
      let cycle = try
          BF.find_negative_cycle_from g c
        with Not_found -> [] in
      if cycle <> [] then
        Format.printf "Negative cycle found %s.@." @@
        String.concat ~sep:" -> "
          (List.map cycle ~f:(fun (start,v,stop) -> start ^ " " ^ Float.to_string v))
    );
  D.fprint_graph Format.str_formatter g;
  let contents = Format.flush_str_formatter () in
  Writer.save "cryptsy.dot" ~contents >>| fun () ->
  Format.printf "Done processing.@."

let _ =
  don't_wait_for @@ cryptsy ();
  never_returns @@ Scheduler.go ()
