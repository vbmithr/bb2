open Core.Std
open Async.Std
open Graph

module G = Imperative.Digraph.ConcreteBidirectionalLabeled(String)
    (struct
      type t = float * int
      let compare = Pervasives.compare
      let default = 1., 0
    end)

module D = Graphviz.Dot
    (struct
      include G
      let edge_attributes e = [`Label (Float.to_string @@ fst @@ G.E.label e); `Color 4711]
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
      let weight e = Float.neg @@ log @@ fst @@ G.E.label e
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
      G.add_edge_e g (G.E.create c1 (1. /. m.ask, 0) c2);
      G.add_edge_e g (G.E.create c2 (m.bid, 0) c1)
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
      G.add_edge_e g (G.E.create c1 (1. /. bidask.ask, 0) c2);
      G.add_edge_e g (G.E.create c2 (bidask.bid, 0) c1)
    );

  StringSet.iter !currs ~f:(fun c ->
      (* Format.printf "Searching for negative cycle starting from %s.@." c; *)
      let cycle = try
          BF.find_negative_cycle_from g c
        with Not_found -> [] in
      if cycle <> [] then
        Format.printf "Negative cycle found %s.@." @@
        String.concat ~sep:" -> "
          (List.map cycle ~f:(fun (start,v,stop) -> start ^ " " ^ Float.to_string @@ fst v))
    );
  D.fprint_graph Format.str_formatter g;
  let contents = Format.flush_str_formatter () in
  Writer.save "cryptsy.dot" ~contents >>| fun () ->
  Format.printf "Done processing.@."

type exchange = [`BFX | `BTCE | `BTRex | `Poloniex | `Kraken | `Hitbtc] [@@deriving show]

module Order = struct
  type t = {
  p: float;
  q: float;
  x: exchange;
  } [@@deriving show,create]
end

module AskSet = struct
  module AskSet = CCHeap.Make(
    struct
      open Order
      type t = Order.t
      let leq t t' = Float.compare t.p t'.p < 0
    end
    )
  include AskSet

  let price_and_nb_below t threshold =
    let open Order in
    fold (fun (nb, a) {p; q; _} ->
        if p < threshold
        then succ nb, a +. p *. q else nb, a)
      (0, 0.) t
end

module BidSet = struct
  module BidSet = CCHeap.Make(
    struct
      open Order
      type t = Order.t
      let leq t t' = Float.compare t.p t'.p > 0
    end
    )
  include BidSet

  let price_and_nb_above t threshold =
    let open Order in
    fold (fun (nb, a) {p; q; _} ->
        if p > threshold
        then succ nb, a +. p *. q else nb, a)
      (0, 0.) t
end

let fees = [
  `BFX, 0.001;
  `BTCE, 0.002;
  `BTRex, 0.0025;
  `Poloniex, 0.002;
  `Kraken, 0.001;
  `Hitbtc, 0.001;
]

let depth () =
  let module BFX = Bittrex.Bitfinex(Bittrex_async.Bitfinex) in
  let module BTCE = Bittrex.BTCE(Bittrex_async.BTCE) in
  let module BTrex = Bittrex.Bittrex(Bittrex_async.Bittrex) in
  let module Poloniex = Bittrex.Poloniex(Bittrex_async.Poloniex) in
  let module Kraken = Bittrex.Kraken(Bittrex_async.Kraken) in
  let module Hitbtc = Bittrex.Hitbtc(Bittrex_async.Hitbtc) in

  let log = Log.create ~level:`Info ~output:[(Log.Output.stderr ())] in

  let rec loop ival =
    let open Bittrex.OrderBook in
    let ltcbid = ref BidSet.empty in
    let ltcask = ref AskSet.empty in

    Monitor.try_with
      (fun () -> Deferred.all_unit
          [
            (BFX.OrderBook.book `LTC `BTC >>| fun { bids; asks } ->
             let fee = List.Assoc.find_exn fees `BFX in
             ltcbid := List.fold_left ~init:!ltcbid ~f:(fun a { price; qty } ->
                 BidSet.add a @@ Order.create ~p:(price *. (1. -. fee)) ~q:qty ~x:`BFX ()
               ) bids;
             ltcask := List.fold_left ~init:!ltcask ~f:(fun a { price; qty } ->
                 AskSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`BFX ()
               ) asks;);

            (BTCE.OrderBook.book `LTC `BTC >>| fun { bids; asks } ->
             let fee = List.Assoc.find_exn fees `BTCE in
             ltcbid := List.fold_left ~init:!ltcbid ~f:(fun a { price; qty } ->
                 BidSet.add a @@ Order.create ~p:(price *. (1. -. fee)) ~q:qty ~x:`BTCE ()
               ) bids;
             ltcask := List.fold_left ~init:!ltcask ~f:(fun a { price; qty } ->
                 AskSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`BTCE ()
               ) asks;);

            (BTrex.OrderBook.book `LTC `BTC >>| fun { bids; asks } ->
             let fee = List.Assoc.find_exn fees `BTRex in
             ltcbid := List.fold_left ~init:!ltcbid ~f:(fun a { price; qty } ->
                 BidSet.add a @@ Order.create ~p:(price *. (1. -. fee)) ~q:qty ~x:`BTRex ()
               ) bids;
             ltcask := List.fold_left ~init:!ltcask ~f:(fun a { price; qty } ->
                 AskSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`BTRex ()
               ) asks;);

            (Poloniex.OrderBook.book `LTC `BTC >>| fun { bids; asks } ->
             let fee = List.Assoc.find_exn fees `Poloniex in
             ltcbid := List.fold_left ~init:!ltcbid ~f:(fun a { price; qty } ->
                 BidSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`Poloniex ()
               ) bids;
             ltcask := List.fold_left ~init:!ltcask ~f:(fun a { price; qty } ->
                 AskSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`Poloniex ()
               ) asks;);

            (Hitbtc.OrderBook.book `LTC `BTC >>| fun { bids; asks } ->
             let fee = List.Assoc.find_exn fees `Hitbtc in
             ltcbid := List.fold_left ~init:!ltcbid ~f:(fun a { price; qty } ->
                 BidSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`Hitbtc ()
               ) bids;
             ltcask := List.fold_left ~init:!ltcask ~f:(fun a { price; qty } ->
                 AskSet.add a @@ Order.create ~p:(price *. (1. +. fee)) ~q:qty ~x:`Hitbtc ()
               ) asks;);
          ]) >>= function
    | Ok () ->
      Log.info log "Updated all books succesfully.";
      (* let maxbid = BidSet.find_min_exn !ltcbid in *)
      (* let minask = AskSet.find_min_exn !ltcask in *)
      (* if minask < maxbid then (\* profit! *\) *)
      (*   let pr, nb = AskSet.price_and_nb_below minask maxbid in *)

      after @@ Time.Span.of_string (string_of_int ival ^ "s") >>= fun () ->
      loop ival
    | Error exn ->
      Log.info log "Error updating books: %s" (Exn.to_string exn);
      after @@ Time.Span.of_string (string_of_int ival ^ "s") >>= fun () ->
      loop ival
  in
  don't_wait_for @@ loop 2;
  Deferred.unit

let btcltc () =
  let module BFX = Bittrex.Bitfinex(Bittrex_async.Bitfinex) in
  let module BTCE = Bittrex.BTCE(Bittrex_async.BTCE) in
  let module BTrex = Bittrex.Bittrex(Bittrex_async.Bittrex) in
  let module Poloniex = Bittrex.Poloniex(Bittrex_async.Poloniex) in
  let module Kraken = Bittrex.Kraken(Bittrex_async.Kraken) in
  let module Hitbtc = Bittrex.Hitbtc(Bittrex_async.Hitbtc) in

  let g = G.create () in
  G.add_vertex g "btc";
  G.add_vertex g "ltc";
  G.add_vertex g "doge";


  BFX.Ticker.(ticker `LTC `BTC >>| fun { bid; ask; } ->
              let fee = List.Assoc.find_exn fees `BFX in
              let bid = bid *. (1. -. fee) in
              let ask = ask *. (1. +. fee) in
              Format.printf "BFX LTCBTC %g %g@." bid ask;
              G.add_edge_e g @@ G.E.create "btc" (1. /. ask, 0) "ltc";
              G.add_edge_e g @@ G.E.create "ltc" (bid, 0) "btc"
             ) >>= fun () ->

  BTCE.Ticker.(ticker `LTC `BTC >>| fun { sell; buy; } ->
               let fee = List.Assoc.find_exn fees `BTCE in
               let sell = sell *. (1. -. fee) in
               let buy = buy *. (1. +. fee) in
               Format.printf "BTCE LTCBTC %g %g@." sell buy;
               G.add_edge_e g @@ G.E.create "btc" (1. /. buy, 1) "ltc";
               G.add_edge_e g @@ G.E.create "ltc" (sell, 1) "btc"
              ) >>= fun () ->

  BTrex.Ticker.(ticker `LTC `BTC >>| fun { bid; ask; } ->
                let fee = List.Assoc.find_exn fees `BTRex in
                let bid = bid *. (1. -. fee) in
                let ask = ask *. (1. +. fee) in
                Format.printf "BTrex LTCBTC %g %g@." bid ask;
                G.add_edge_e g @@ G.E.create "btc" (1. /. ask, 2) "ltc";
                G.add_edge_e g @@ G.E.create "ltc" (bid, 2) "btc"
               ) >>= fun () ->

  Poloniex.Ticker.(ticker `LTC `BTC >>| fun { bid; ask; } ->
                   let fee = List.Assoc.find_exn fees `Poloniex in
                   let bid = bid *. (1. -. fee) in
                   let ask = ask *. (1. +. fee) in
                   Format.printf "Poloniex LTCBTC %g %g@." bid ask;
                   G.add_edge_e g @@ G.E.create "btc" (1. /. ask, 3) "ltc";
                   G.add_edge_e g @@ G.E.create "ltc" (bid, 3) "btc"
                  ) >>= fun () ->

  Kraken.Ticker.(ticker `BTC `LTC >>| fun { bid; ask; } ->
                 let fee = List.Assoc.find_exn fees `Kraken in
                 let bid = bid *. (1. -. fee) in
                 let ask = ask *. (1. +. fee) in
                 Format.printf "Kraken LTCBTC %g %g@." (1. /. ask) (1. /. bid);
                 G.add_edge_e g @@ G.E.create "btc" (bid, 4) "ltc";
                 G.add_edge_e g @@ G.E.create "ltc" (1. /. ask, 4) "btc"
                ) >>= fun () ->

  BTrex.Ticker.(ticker `DOGE `BTC >>| fun { bid; ask; } ->
                let fee = List.Assoc.find_exn fees `BTRex in
                let bid = bid *. (1. -. fee) in
                let ask = ask *. (1. +. fee) in
                Format.printf "BTrex DOGEBTC %g %g@." bid ask;
                G.add_edge_e g @@ G.E.create "btc" (1. /. ask, 2) "doge";
                G.add_edge_e g @@ G.E.create "doge" (bid, 2) "btc"
               ) >>= fun () ->

  Poloniex.Ticker.(ticker `DOGE `BTC >>| fun { bid; ask; } ->
                   let fee = List.Assoc.find_exn fees `Poloniex in
                   let bid = bid *. (1. -. fee) in
                   let ask = ask *. (1. +. fee) in
                   Format.printf "Poloniex DOGEBTC %g %g@." bid ask;
                   G.add_edge_e g @@ G.E.create "btc" (1. /. ask, 3) "doge";
                   G.add_edge_e g @@ G.E.create "doge" (bid, 3) "btc"
                  ) >>= fun () ->

  Kraken.Ticker.(ticker `BTC `DOGE >>| fun { bid; ask; } ->
                 let fee = List.Assoc.find_exn fees `Kraken in
                 let bid = bid *. (1. -. fee) in
                 let ask = ask *. (1. +. fee) in
                 Format.printf "Kraken DOGEBTC %g %g@." (1. /. ask) (1. /. bid);
                 G.add_edge_e g @@ G.E.create "btc" (bid, 4) "doge";
                 G.add_edge_e g @@ G.E.create "doge" (1. /. ask, 4) "btc"
                ) >>| fun () ->

  List.iter ["btc"; "ltc"; "doge"] ~f:(fun curr ->
      let cycle = try
          BF.find_negative_cycle_from g curr
        with Not_found -> [] in
      if cycle <> [] then
        Format.printf "Arbitrage opportunity found: %s.@." @@
        String.concat ~sep:" -> "
          (List.map cycle ~f:(fun (start,(v,id),stop) ->
               Format.sprintf "%s %g %d %s" start v id stop))
    );
  Shutdown.shutdown 0

let _ =
  don't_wait_for @@ btcltc ();
  never_returns @@ Scheduler.go ()
