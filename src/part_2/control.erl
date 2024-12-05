-module(control).
-import(dht, [start/2, get_node_info/1, calculate_hash/1, query_key/3]).
-import(lists, [nth/2, map/2]).
-import(string, [to_lower/1]).
-import(file, [read_file/1, write_file/2, make_dir/1]).
-import(io_lib, [format/2]).
-export([start/0, init/0]).

% MODIFY THE NUMBER OF NODES HERE ---------------------------
-define(NumberOfNodes, 100).
-define(StartingNodeIndex, 1).
% ----------------------------------------------------------

start() ->
    spawn(?MODULE, init, []).

init() ->
    io:format(
        "Starting DHT--------------------------------------------------------------------------------~n"
    ),
    % Read keys from keys.csv
    {ok, KeysData} = file:read_file("keys.csv"),
    Keys = parse_keys(KeysData),

    % Read queries from key_queries.csv
    {ok, QueriesData} = file:read_file("key_queries.csv"),
    Queries = parse_keys(QueriesData),

    % Start with ?NumberOfNodes nodes
    {ok, NodesWithPid} = dht:start(?NumberOfNodes, Keys),
    io:format("Nodes started~n"),

    % Process queries
    process_queries(Queries, NodesWithPid, []).

parse_keys(Data) ->
    Lines = string:tokens(binary_to_list(Data), "\n\r"),
    [list_to_integer(Line) || Line <- Lines].

process_queries([], _, QueriedIdentifiers) ->
    % QueriedIdentifiers is a list of strings in the form of "key_identifier|queried_node1_identifier|queried_node2_identifier|..."
    % Write one line for each query
    io:format("Queried Identifiers: ~p~n", [QueriedIdentifiers]),
    file:write_file(
        io_lib:format("node_~p_queries.csv", [?StartingNodeIndex]),
        lists:foldl(
            fun(Query, Acc) ->
                case Acc of
                    "" -> Query;
                    _ -> Acc ++ "\n" ++ Query
                end
            end,
            "",
            QueriedIdentifiers
        )
    ),
    ok;
process_queries([Query | Rest], Nodes, QueriedIdentifiers) ->
    % key is hashed in query_key
    NewQueriedIdentifiers = dht:query_key(Nodes, Query, ?StartingNodeIndex),

    % append the new queried identifiers to the list
    process_queries(Rest, Nodes, lists:append(QueriedIdentifiers, [NewQueriedIdentifiers])).
