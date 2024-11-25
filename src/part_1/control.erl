-module(control).
-import(dht, [start/1, put/3, get/2, add_key/2]).
-export([start/0, init/0]).

start() ->
    spawn(?MODULE, init, []).

init() ->
    % Read keys from keys.csv
    {ok, KeysData} = file:read_file("keys.csv"),
    Keys = parse_keys(KeysData),
    
    % Read queries from key_queries.csv
    {ok, QueriesData} = file:read_file("key_queries.csv"),
    Queries = parse_keys(QueriesData),
    
    % Start initial DHT node
    {ok, Nodes} = dht:start(10), % Start with 10 nodes

    % Add keys to DHT
    lists:foreach(fun(Key) -> add_key(lists:nth(1, Nodes), Key) end, Keys),
     
    % Process queries
    process_queries(Queries, Nodes).

parse_keys(Data) ->
    Lines = string:tokens(binary_to_list(Data), "\n\r"),
    [list_to_integer(Line) || Line <- Lines].

process_queries([], _) -> ok;
process_queries([Query|Rest], [FirstNode|_]=Nodes) ->
    FirstNode ! {get, Query, self()},
    receive
        {Query, Value} ->
            io:format("Query: ~p -> value: ~p~n", [Query, Value])
    end,
    process_queries(Rest, Nodes).
