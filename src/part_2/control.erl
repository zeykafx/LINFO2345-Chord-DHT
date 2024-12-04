-module(control).
-import(dht, [start/2, get_node_info/1, calculate_hash/1, query_key/2]).
-import(lists, [nth/2, map/2]).
-import(string, [to_lower/1]).
-import(file, [read_file/1, write_file/2, make_dir/1]).
-import(io_lib, [format/2]).
-export([start/0, init/0]).

% MODIFY THE NUMBER OF NODES HERE ---------------------------
-define(NumberOfNodes, 10).
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

    % Start initial DHT node

    % Start with ?NumberOfNodes nodes
    {ok, NodesWithPid} = dht:start(?NumberOfNodes, Keys),
    io:format("Nodes started~n"),

    % lists:map(
    %     fun({_NodeId, _NodeIndex, Pid}) ->
    %         % node id is not hashed, Identifier is hashed
    %         {Identifier, Successor, Predecessor, KeysListRcvd, _FingerTable} = get_node_info(
    %             Pid
    %         ),

    %         io:format("Node ~p: Predecessor ~p, Successor ~p, Number of Keys stored: ~p~n", [
    %             Identifier, Predecessor, Successor, length(KeysListRcvd)
    %         ])
    %     end,
    %     NodesWithPid
    % ),

    % Process queries
    process_queries(Queries, NodesWithPid).

% add keys to DHT
% add_keys_to_dht(Keys, Nodes) ->
%     HashedKeys = lists:map(fun(Key) -> calculate_hash(Key) end, Keys),
%     SortedHashedKeys = lists:sort(HashedKeys),
%     lists:foreach(fun(Key) -> dht:add_key(Nodes, Key) end, SortedHashedKeys),
%     io:format("Keys added to DHT~n").

parse_keys(Data) ->
    Lines = string:tokens(binary_to_list(Data), "\n\r"),
    [list_to_integer(Line) || Line <- Lines].

process_queries([], _) ->
    ok;
process_queries([Query | Rest], Nodes) ->
    % key is hashed in query_key
    dht:query_key(Nodes, Query),
    process_queries(Rest, Nodes).

% create_directory(Dir) ->
%     case file:make_dir(Dir) of
%         {error, eexist} -> ok;
%         ok -> ok
%     end.
