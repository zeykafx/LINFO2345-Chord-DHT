-module(dht).
-export([start/1, init_node/2, put/3, get/2, get_node_info/1, add_key/2]).
-import(crypto, [hash/2]).
-define(M, 16).
-define(Max_Key, math:pow(2, ?M) - 1).

% Starts the system with N nodes
start(N) ->
    NodeIds = lists:seq(0, N - 1),

    % initialize nodes, Nodes is the list of node Pids
    Nodes = lists:map(fun(Id) -> spawn(?MODULE, init_node, [Id, NodeIds]) end, NodeIds),
    % link nodes to main process, so they die when main process dies
    lists:foreach(fun(Node) -> link(Node) end, Nodes),
    {ok, Nodes}.

% Initializes a node with an identifier and the ring of nodes
init_node(Id, NodeIds) ->
    % Identifier = calculate_hash(Id),
    Identifier = Id,
    Successor = find_successor(Id, NodeIds),
    Predecessor = find_predecessor(Id, NodeIds),
    Keys = #{},
    loop(Id, Identifier, Successor, Predecessor, Keys).

% Main loop for node state
loop(Id, Identifier, Successor, Predecessor, Keys) ->
    receive
        {add_key, Key} ->
            % add the key to the map
            HashedKey = calculate_hash(Key),
            NewKeys = maps:put(HashedKey, undefined, Keys),
            loop(Id, Identifier, Successor, Predecessor, NewKeys);
        {put, Key, Value} ->
            HashedKey = calculate_hash(Key),
            % if the key is already in the map, update the value, otherwise add the key-value pair
            NewKeys = maps:put(HashedKey, Value, Keys),
            loop(Id, Identifier, Successor, Predecessor, NewKeys);
        {get, Key, Caller} ->
            HashedKey = calculate_hash(Key),
            % if the key is in the map, return the value, otherwise return undefined
            case maps:find(HashedKey, Keys) of
                {ok, Value} -> Caller ! {Key, Value};
                error -> Caller ! {Key, undefined}
            end,
            loop(Id, Identifier, Successor, Predecessor, Keys);
        {get_info, Caller} ->
            Caller ! {Id, Identifier, Successor, Predecessor, Keys},
            loop(Id, Identifier, Successor, Predecessor, Keys);
        stop ->
            ok
    end.


add_key(Node, Key) ->
    Node ! {add_key, Key}.

% Add a key-value pair to the DHT
put(Node, Key, Value) ->
    Node ! {put, Key, Value}.

% Get the value of a key from the DHT
get(Node, Key) ->
    Node ! {get, Key, self()},
    receive
        {Key, Value} -> Value
    after 5000 -> % timeout
        undefined
    end.

% Request node information
get_node_info(Node) ->
    Node ! {get_info, self()},
    receive
        Info -> Info
    end.

% Utility: Find successor node
find_successor(Id, NodeIds) ->
    case [X || X <- NodeIds, X > Id] of % basically, make a list of Xs that come from the NodeIds list, where X is greater than Id
        [] -> hd(NodeIds); % if the list is empty, return the head of the NodeIds list 
        [Successor | _] -> Successor % otherwise, return the first element of the list  
    end. % so what this does is that it finds the next node in the ring that is greater than the current node, if there is none (i.e., last node), it returns the first node in the ring

% Utility: Find predecessor node
find_predecessor(Id, NodeIds) ->
    % kind of the opposite of find_successor
    case lists:reverse([X || X <- NodeIds, X < Id]) of
        [] -> lists:last(NodeIds);
        [Predecessor | _] -> Predecessor
    end.

% Utility: Calculate SHA-1 hash of an identifier
calculate_hash(Key) ->
    String = io_lib:format("~p", [Key]),
	crypto:bytes_to_integer(crypto:hash(sha512, String)).
