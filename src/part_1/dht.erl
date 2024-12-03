-module(dht).
-export([start/2, init_node/4, get_node_info/1, calculate_hash/1, hash_to_string/1]).
-import(crypto, [hash/2, sha/1]).
-import(lists, [seq/2, map/2, filter/2, append/2, reverse/1]).

-define(M, 16).
-define(Max_Key, round(math:pow(2, ?M)) - 1).

% Starts the system with N nodes
start(N, Keys) ->
    NodeIds = lists:seq(0, N - 1),
    HashedNodeIds = lists:map(
        fun(Id) ->
            HashedId = calculate_hash(Id),
            {Id, HashedId}
        end,
        NodeIds
    ),
    SortedHashedNodeIds = lists:sort(
        fun({_, Hash1}, {_, Hash2}) -> Hash1 =< Hash2 end,
        HashedNodeIds
    ),

    HashedKeys = lists:map(fun(Key) -> calculate_hash(Key) end, Keys),
    SortedHashedKeys = lists:sort(HashedKeys),

    KeysForEachNode = set_keys_for_nodes(SortedHashedNodeIds, SortedHashedKeys),
    io:format("Keys for each node: ~p~n", [KeysForEachNode]),

    Nodes = lists:map(
        fun({NodeId, NodeIndex, NodeKeys}) ->
            Pid = spawn(?MODULE, init_node, [NodeIndex, NodeId, SortedHashedNodeIds, NodeKeys]),
            {NodeId, NodeIndex, Pid}
        end,
        KeysForEachNode
    ),

    % once all the nodes are started, we can set the successor and predecessor for each node
    finish_initialization(Nodes, SortedHashedNodeIds),

    {ok, Nodes}.

% Figure out for each node which keys it is responsible for
% per chord DHT, each node is responsible for the keys that are greater than the previous node's id and less than the current node's id
set_keys_for_nodes(SortedHashedNodeIds, SortedHashedKeys) ->
    % return a list of tuples where each tuple is (NodeId, [Keys])
    NumberNodes = length(SortedHashedNodeIds),
    lists:map(
        fun(Index) ->
            % get the node's hashed id
            {NodeIndex, NodeId} = lists:nth(Index, SortedHashedNodeIds),

            {_PredecessorIndex, PredecessorId} = lists:nth(
                ((Index - 2 + NumberNodes) rem NumberNodes + 1), SortedHashedNodeIds
            ),
            NodeKeys = lists:filter(
                fun(Key) ->
                    (Key >= PredecessorId andalso Key < NodeId) orelse
                        (PredecessorId > NodeId andalso (Key >= PredecessorId orelse Key < NodeId))
                end,
                SortedHashedKeys
            ),
            {NodeId, NodeIndex, NodeKeys}
        end,
        lists:seq(1, NumberNodes)
    ).

finish_initialization(NodesWithPid, SortedHashedNodeIds) ->
    lists:map(
        fun(I) ->
            {NodeId, _NodeIndex, Pid} = lists:nth(I, NodesWithPid),

            NumberNodes = length(SortedHashedNodeIds),

            % get the next node's hashed id
            NextNodeIndex = I rem NumberNodes + 1,
            {NextHashedId, _NextNodeId, NextPid} = lists:nth(NextNodeIndex, NodesWithPid),

            % get the previous node's hashed id
            % -2 because we are 1-indexed and we want the previous node
            PreviousNodeIndex = ((I - 2) + NumberNodes) rem NumberNodes + 1,
            {PreviousHashedId, _PreviousNodeId, PreviousPid} = lists:nth(
                PreviousNodeIndex, NodesWithPid
            ),

            io:format("Node ~p: Next ~p, Previous ~p~n", [NodeId, NextHashedId, PreviousHashedId]),

            % set the successor and predecessor for the node
            Pid ! {set_successor, {NextHashedId, NextPid}},
            Pid ! {set_predecessor, {PreviousHashedId, PreviousPid}}
        end,
        lists:seq(1, length(NodesWithPid))
    ).

% Initializes a node with an identifier and the ring of nodes
init_node(NodeIndex, Identifier, _SortedHashedNodeIds, Keys) ->
    Predecessor = {Identifier, self()},
    Successor = {Identifier, self()},

    loop(NodeIndex, Identifier, Successor, Predecessor, Keys).

% Main loop for node state
loop(NodeIndex, Identifier, Successor, Predecessor, Keys) ->
    receive
        {add_key, Key} ->
            % add the key to the list
            % key is already hashed
            NewList = lists:append(Keys, [Key]),
            loop(
                NodeIndex, Identifier, Successor, Predecessor, NewList
            );
        {get_info, Caller} ->
            % return the node information
            Caller ! {NodeIndex, Identifier, Successor, Predecessor, Keys},
            loop(NodeIndex, Identifier, Successor, Predecessor, Keys);
        {set_successor, NewSuccessor} ->
            loop(
                NodeIndex,
                Identifier,
                NewSuccessor,
                Predecessor,
                Keys
            );
        {set_predecessor, NewPredecessor} ->
            loop(
                NodeIndex,
                Identifier,
                Successor,
                NewPredecessor,
                Keys
            );
        stop ->
            ok
    end.

% Add a key to the DHT
% Nodes is the list of node Pids
% Key is the key to add (not hashed)
% add_key(Nodes, Key) ->
%     % Nodes is the list of node Pids
%     HashedKey = calculate_hash(Key),
%     {_, ResponsibleNode} = find_responsible_node(HashedKey, Nodes),
%     ResponsibleNode ! {add_key, HashedKey}.

% % Find the node responsible for a hashed key
% % Nodes is the list of node Pids
% % HashedKey is the hashed key
% find_responsible_node(HashedKey, Nodes) ->
%     % get the node information for each node
%     NodeInfoList = lists:map(
%         fun(Node) ->
%             {_, NodeId, _, _, _, _, _} = get_node_info(Node),
%             {NodeId, Node}
%         end,
%         Nodes
%     ),
%     % filter the list of nodes to find the first node that has an Id greater than the hashed key, otherwise wrap around
%     ResponsibleNode =
%         case lists:filter(fun({NodeId, _}) -> HashedKey =< NodeId end, NodeInfoList) of
%             [] -> hd(NodeInfoList);
%             [{NodeId, Node} | _] -> {NodeId, Node}
%         end,
%     ResponsibleNode.

% Request node information
get_node_info(Node) ->
    Node ! {get_info, self()},
    receive
        Info -> Info
    end.

% % Utility: Find successor node
% find_successor(Id, NodeIds) ->
%     % basically, make a list of Xs that come from the NodeIds list, where X is greater than Id
%     case [X || X <- NodeIds, X > Id] of
%         % if the list is empty, return the head of the NodeIds list
%         [] -> hd(NodeIds);
%         % otherwise, return the first element of the list
%         [Successor | _] -> Successor
%         % so what this does is that it finds the next node in the ring that is greater than the current node, if there is none (i.e., last node), it returns the first node in the ring
%     end.

% % Utility: Find predecessor node
% find_predecessor(Id, NodeIds) ->
%     % kind of the opposite of find_successor
%     case lists:reverse([X || X <- NodeIds, X < Id]) of
%         [] -> lists:last(NodeIds);
%         [Predecessor | _] -> Predecessor
%     end.

% Utility: Calculate SHA-1 hash of an identifier
calculate_hash(Key) ->
    StringKey = integer_to_list(Key),
    Id = crypto:bytes_to_integer(crypto:hash(sha, StringKey)),
    % <<Id:160/integer>> = crypto:hash(sha, StringKey),
    HashResult = Id rem ?Max_Key,
    % string:to_lower(integer_to_list(HashResult, 16)).
    HashResult.

hash_to_string(Hash) ->
    string:to_lower(integer_to_list(Hash, 16)).

% This other way to hash gives the same results
    % HashResult = crypto:bytes_to_integer(crypto:hash(sha, IntKey)) rem ?Max_Key,
    % string:to_lower(integer_to_list(HashResult, 16)).
