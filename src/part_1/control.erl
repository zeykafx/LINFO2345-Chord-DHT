-module(control).
-import(dht, [start/1, get_node_info/1, add_key/2, calculate_hash/1, get_node_info/1]).
-import(lists, [nth/2, map/2]).
-export([start/0, init/0]).

start() ->
    spawn(?MODULE, init, []).

init() ->
    % Read keys from keys.csv
    {ok, KeysData} = file:read_file("keys.csv"),
    Keys = parse_keys(KeysData),

    % Read queries from key_queries.csv
    % {ok, QueriesData} = file:read_file("key_queries.csv"),
    % Queries = parse_keys(QueriesData),

    % test: hash keys and print it out
    % HeadKeys = [nth(X, Keys) || X <- lists:seq(1, 10)],
    % io:format("Keys: ~p~n", [HeadKeys]),
    % map(fun(X) -> Hash = calculate_hash(X), io:format("hash: ~p -> ~p ~n", [X, Hash])  end, HeadKeys),

    % Start initial DHT node

    % Start with 10 nodes
    {ok, Nodes} = dht:start(10),

    % Add keys to DHT
    add_keys_to_dht(Keys, Nodes),

    % figure out how many keys are stored in each node
    Data = map(
        fun(Node) ->
            {NodeId, _, Succ, Pred, KeyRecvd} = get_node_info(Node),
            io:format("Node ~p: Succ ~p, Pred ~p~n", [NodeId, Succ, Pred]),
            maps:size(KeyRecvd)
        end,
        Nodes
    ),
    io:format("Keys stored in each node: ~p~n", [Data]).

% Process queries
% process_queries(Queries, Nodes).

% add keys to DHT
add_keys_to_dht(Keys, Nodes) ->
    lists:foreach(fun(Key) -> dht:add_key(Nodes, Key) end, Keys).

parse_keys(Data) ->
    Lines = string:tokens(binary_to_list(Data), "\n\r"),
    [list_to_integer(Line) || Line <- Lines].

% process_queries([], _) ->
%     ok;
% process_queries([Query | Rest], [FirstNode | _] = Nodes) ->
%     FirstNode ! {get, Query, self()},
%     receive
%         {Query, Value} ->
%             io:format("Query: ~p -> value: ~p~n", [Query, Value])
%     end,
%     process_queries(Rest, Nodes).
