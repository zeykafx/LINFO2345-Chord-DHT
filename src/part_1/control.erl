-module(control).
-import(dht, [start/1, get_node_info/1, add_key/2, calculate_hash/1, hash_to_string/1]).
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
    % Read keys from keys.csv
    {ok, KeysData} = file:read_file("keys.csv"),
    Keys = parse_keys(KeysData),

    % Read queries from key_queries.csv
    % {ok, QueriesData} = file:read_file("key_queries.csv"),
    % Queries = parse_keys(QueriesData),

    % Start initial DHT node

    % Start with ?NumberOfNodes nodes
    {ok, Nodes} = dht:start(?NumberOfNodes),

    % Add keys to DHT
    add_keys_to_dht(Keys, Nodes),

    % Logging node information to folder
    case create_directory(io_lib:format("dht_~p", [?NumberOfNodes])) of
        ok -> ok;
        {error, Reason} -> io:format("Error: ~p~n", [Reason])
    end,

    % figure out how many keys are stored in each node
    lists:map(
        fun(Node) ->
            % node id is not hashed, Identifier is hashed
            {NodeId, Identifier, Succ, Pred, HashedSucc, HashedPred, KeysListRcvd} = get_node_info(
                Node
            ),
            HexIdentifier = hash_to_string(Identifier),
            HexSucc = hash_to_string(HashedSucc),
            HexPred = hash_to_string(HashedPred),

            io:format("Node ~p (hash: ~s): Succ ~p, Pred ~p, Number of Keys stored: ~p~n", [
                NodeId, HexIdentifier, Succ, Pred, length(KeysListRcvd)
            ]),

            % node_identifier,successor_identifier,predecessor_identifier|key1_identifier|key2_identifier|key3_identifier...
            % All the identifier need to be a string representing the hex identifier. For exemple: "a3e324f01ab359d2"
            KeysList = lists:foldl(
                % basically go through all the keys and append them to the list by separating them with "|", except the last one
                fun(Key, Acc) ->
                    % KeyStr = string:to_lower(integer_to_list(Key, 16)),
                    KeyStr = hash_to_string(Key),
                    case Acc of
                        "" -> KeyStr;
                        _ -> Acc ++ "|" ++ KeyStr
                    end
                end,
                "",
                KeysListRcvd
            ),

            % write to file
            file:write_file(
                io_lib:format("dht_~p/node_~p.csv", [?NumberOfNodes, NodeId]),
                io_lib:format("~s,~s,~s|~s~n", [
                    HexIdentifier,
                    HexPred,
                    HexSucc,
                    KeysList
                ])
            )
        end,
        Nodes
    ).

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

create_directory(Dir) ->
    case file:make_dir(Dir) of
        {error, eexist} -> ok;
        ok -> ok
    end.
