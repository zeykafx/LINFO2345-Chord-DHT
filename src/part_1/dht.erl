-module(dht).
-export([start/1, init_node/2, get_node_info/1, add_key/2, calculate_hash/1]).
-import(crypto, [hash/2, sha/1]).
-import(lists, [seq/2, map/2, filter/2, append/2, reverse/1]).

-define(M, 16).
-define(Max_Key, round(math:pow(2, ?M)) - 1).

% Starts the system with N nodes
start(N) ->
    NodeIds = lists:seq(0, N - 1),

    % initialize nodes, Nodes is the list of node Pids
    Nodes = lists:map(
        fun(Id) ->
            spawn(?MODULE, init_node, [Id, NodeIds])
        end,
        NodeIds
    ),
    {ok, Nodes}.

% Initializes a node with an identifier and the ring of nodes
init_node(Id, NodeIds) ->
    Identifier = calculate_hash(Id),
    Successor = find_successor(Id, NodeIds),
    HashedSuccessor = calculate_hash(Successor),
    Predecessor = find_predecessor(Id, NodeIds),
    HashedPredecessor = calculate_hash(Predecessor),
    loop(Id, Identifier, Successor, Predecessor, HashedSuccessor, HashedPredecessor, []).

% Main loop for node state
loop(Id, Identifier, Successor, Predecessor, HashedSuccessor, HashedPredecessor, Keys) ->
    receive
        {add_key, Key} ->
            % add the key to the list
            % key is already hashed
            NewList = lists:append(Keys, [Key]),
            loop(
                Id, Identifier, Successor, Predecessor, HashedSuccessor, HashedPredecessor, NewList
            );
        {get_info, Caller} ->
            % return the node information
            Caller !
                {Id, Identifier, Successor, Predecessor, HashedSuccessor, HashedPredecessor, Keys},
            loop(Id, Identifier, Successor, Predecessor, HashedSuccessor, HashedPredecessor, Keys);
        stop ->
            ok
    end.

% Add a key to the DHT
% Nodes is the list of node Pids
% Key is the key to add (not hashed)
add_key(Nodes, Key) ->
    % Nodes is the list of node Pids
    HashedKey = calculate_hash(Key),
    {_, ResponsibleNode} = find_responsible_node(HashedKey, Nodes),
    ResponsibleNode ! {add_key, HashedKey}.

% Find the node responsible for a hashed key
% Nodes is the list of node Pids
% HashedKey is the hashed key
find_responsible_node(HashedKey, Nodes) ->
    % get the node information for each node
    NodeInfoList = lists:map(
        fun(Node) ->
            {_, NodeId, _, _, _, _, _} = get_node_info(Node),
            {NodeId, Node}
        end,
        Nodes
    ),
    % filter the list of nodes to find the first node that has an Id greater than the hashed key, otherwise wrap around
    ResponsibleNode =
        case lists:filter(fun({NodeId, _}) -> HashedKey =< NodeId end, NodeInfoList) of
            [] -> hd(NodeInfoList);
            [{NodeId, Node} | _] -> {NodeId, Node}
        end,
    ResponsibleNode.

% Request node information
get_node_info(Node) ->
    Node ! {get_info, self()},
    receive
        Info -> Info
    end.

% Utility: Find successor node
find_successor(Id, NodeIds) ->
    % basically, make a list of Xs that come from the NodeIds list, where X is greater than Id
    case [X || X <- NodeIds, X > Id] of
        % if the list is empty, return the head of the NodeIds list
        [] -> hd(NodeIds);
        % otherwise, return the first element of the list
        [Successor | _] -> Successor
        % so what this does is that it finds the next node in the ring that is greater than the current node, if there is none (i.e., last node), it returns the first node in the ring
    end.

% Utility: Find predecessor node
find_predecessor(Id, NodeIds) ->
    % kind of the opposite of find_successor
    case lists:reverse([X || X <- NodeIds, X < Id]) of
        [] -> lists:last(NodeIds);
        [Predecessor | _] -> Predecessor
    end.

% Utility: Calculate SHA-1 hash of an identifier
calculate_hash(Key) ->
    StringKey = integer_to_list(Key),
    Id = crypto:bytes_to_integer(crypto:hash(sha, StringKey)),
    % <<Id:160/integer>> = crypto:hash(sha, StringKey),
    HashResult = Id rem ?Max_Key,
    string:to_lower(integer_to_list(HashResult, 16)).

% This other way to hash gives the same results
    % HashResult = crypto:bytes_to_integer(crypto:hash(sha, IntKey)) rem ?Max_Key,
    % string:to_lower(integer_to_list(HashResult, 16)).

