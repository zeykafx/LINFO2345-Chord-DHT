-module(control).
-import(dht, [start/2, get_node_info/1, calculate_hash/1, hash_to_string/1]).
-import(lists, [nth/2, map/2]).
-import(string, [to_lower/1]).
-import(file, [read_file/1, write_file/2, make_dir/1]).
-import(io_lib, [format/2]).
-export([start/0, init/0]).

% MODIFY THE NUMBER OF NODES HERE ---------------------------
-define(NumberOfNodes, 100).
% ----------------------------------------------------------

start() ->
    spawn(?MODULE, init, []).

init() ->
    % Read keys from keys.csv
    {ok, KeysData} = file:read_file("keys.csv"),
    Keys = parse_keys(KeysData),

    % Start initial DHT node

    % Start with ?NumberOfNodes nodes
    {ok, NodesWithPid} = dht:start(?NumberOfNodes, Keys),

    % Logging node information to folder
    case create_directory(io_lib:format("dht_~p", [?NumberOfNodes])) of
        ok -> ok;
        {error, Reason} -> io:format("Error: ~p~n", [Reason])
    end,

    % figure out how many keys are stored in each node
    lists:map(
        fun({_NodeId, _NodeIndex, NodePid}) ->
            % node id is not hashed, Identifier is hashed
            {NodeIndex, Identifier, {SuccId, _SuccPid}, {PredId, _PredPid}, KeysListRcvd} = get_node_info(
                NodePid
            ),

            io:format("Node ~p (hash: ~p): Succ ~p, Pred ~p, Number of Keys stored: ~p~n", [
                NodeIndex, Identifier, SuccId, PredId, length(KeysListRcvd)
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
                io_lib:format("dht_~p/node_~p.csv", [?NumberOfNodes, NodeIndex]),
                io_lib:format("~s,~s,~s|~s~n", [
                    hash_to_string(Identifier),
                    hash_to_string(SuccId),
                    hash_to_string(PredId),
                    KeysList
                ])
            )
        end,
        NodesWithPid
    ).

parse_keys(Data) ->
    Lines = string:tokens(binary_to_list(Data), "\n\r"),
    [list_to_integer(Line) || Line <- Lines].


create_directory(Dir) ->
    case file:make_dir(Dir) of
        {error, eexist} -> ok;
        ok -> ok
    end.
