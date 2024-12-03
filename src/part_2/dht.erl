-module(dht).
-export([
    start/2,
    get_node_info/1,
    init_node/4,
    loop/6,
    calculate_hash/1,
    hash_to_string/1,
    query_key/2
]).
-import(crypto, [hash/2, sha/1]).
-import(lists, [seq/2, map/2, filter/2, append/2, reverse/1, nth/2]).

-define(M, 16).
-define(Max_Key, round(math:pow(2, ?M)) - 1).

% Starts the system with N nodes
start(N, Keys) ->
    NodeIds = lists:seq(0, N - 1),
    HashedNodeIds = lists:map(
        fun(Id) ->
            HashedId = calculate_hash(Id),
            io:format("Node ~p: Hashed Id ~p~n", [Id, HashedId]),
            {Id, HashedId}
        end,
        NodeIds
    ),
    SortedHashedNodeIds = lists:sort(
        fun({_, Hash1}, {_, Hash2}) -> Hash1 =< Hash2 end,
        HashedNodeIds
    ),
    % io:format("Node Ids: ~p, length: ~p~n", [SortedHashedNodeIds, length(SortedHashedNodeIds)]),

    HashedKeys = lists:map(fun(Key) -> calculate_hash(Key) end, Keys),
    SortedHashedKeys = lists:sort(HashedKeys),
    % io:format("Keys: ~p, length: ~p~n", [SortedHashedKeys, length(SortedHashedKeys)]),

    KeysForEachNode = set_keys_for_nodes(NodeIds, SortedHashedNodeIds, SortedHashedKeys),
    io:format("Keys for each node: ~p~n", [KeysForEachNode]),

    Nodes = lists:map(
        fun({NodeId, NodeIndex, NodeKeys}) ->
            Pid = spawn(?MODULE, init_node, [NodeIndex, NodeId, SortedHashedNodeIds, NodeKeys]),
            {NodeId, NodeIndex, Pid}
        end,
        KeysForEachNode
    ),

    % once all the nodes are started, we can set the successor and predecessor for each node as well as the finger table
    finish_initialization(Nodes, SortedHashedNodeIds),

    {ok, Nodes}.

% Figure out for each node which keys it is responsible for
% per chord DHT, each node is responsible for the keys that are greater than the previous node's id and less than the current node's id
set_keys_for_nodes(_NodeIds, SortedHashedNodeIds, SortedHashedKeys) ->
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
            Pid ! {set_predecessor, {PreviousHashedId, PreviousPid}},

            % Create the finger table for the node
            FingerTable = create_finger_table(I, NodesWithPid, NumberNodes),
            io:format("Node ~p: Finger Table ~p~n", [NodeId, FingerTable]),
            Pid ! {set_finger_table, FingerTable}
        end,
        lists:seq(1, length(NodesWithPid))
    ).

create_finger_table(NodeIdx, NodesWithPid, NumberNodes) ->
    % Create a finger table entry for each bit position
    lists:foldl(
        fun(I, FingerTable) ->
            % Calculate the target position for this finger
            % Each finger jumps 2^(i-1) positions ahead
            JumpDistance = trunc(math:pow(2, I - 1)),
            TargetPosition = (NodeIdx + JumpDistance) rem NumberNodes,
            % io:format("Node ~p: Finger ~p, Target ~p, Target Node~p~n", [NodeId, I, TargetPosition, lists:nth(TargetPosition + 1, NodesWithPid)]),

            {ResponsibleNodeHashedId, _, ResponsibleNodePid} = lists:nth(
                TargetPosition + 1, NodesWithPid
            ),

            % Add to finger table
            maps:put(I, {ResponsibleNodeHashedId, ResponsibleNodePid}, FingerTable)
        end,
        % Start with empty map
        #{},
        % Create entries for 1 to M
        lists:seq(0, ?M - 1)
    ).

% Initializes a node with an identifier and the ring of nodes
init_node(Identifier, NodeIndex, _NodeIds, Keys) ->
    % Start with a node that has itself as the predecessor and successor
    Predecessor = {Identifier, self()},
    Successor = {Identifier, self()},

    % Initialize the finger table
    FingerTable = #{},

    loop(
        NodeIndex,
        Identifier,
        Successor,
        Predecessor,
        Keys,
        FingerTable
    ).

% Main loop for node state
loop(NodeIndex, Identifier, Successor, Predecessor, Keys, FingerTable) ->
    receive
        {add_key, Key} ->
            % add the key to the list
            % key is already hashed
            NewList = lists:append(Keys, [Key]),
            loop(
                NodeIndex,
                Identifier,
                Successor,
                Predecessor,
                NewList,
                FingerTable
            );
        {get_info, Caller} ->
            % return the node information
            Caller !
                {NodeIndex, Identifier, Successor, Predecessor, Keys, FingerTable},
            loop(
                NodeIndex,
                Identifier,
                Successor,
                Predecessor,
                Keys,
                FingerTable
            );
        {set_successor, NewSuccessor} ->
            loop(
                NodeIndex,
                Identifier,
                NewSuccessor,
                Predecessor,
                Keys,
                FingerTable
            );
        {set_predecessor, NewPredecessor} ->
            loop(
                NodeIndex,
                Identifier,
                Successor,
                NewPredecessor,
                Keys,
                FingerTable
            );
        {set_finger_table, NewFingerTable} ->
            loop(
                NodeIndex,
                Identifier,
                Successor,
                Predecessor,
                Keys,
                NewFingerTable
            );
        stop ->
            ok
    end.

% Request node information
get_node_info(Node) ->
    Node ! {get_info, self()},
    receive
        Info -> Info
    end.

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

query_key(NodesWithPid, Key) ->
    io:format("Querying for key ~p~n", [Key]),
    HashedKey = calculate_hash(Key),
    [{_StartNodeId, _StartNodeIndex, StartNodePid} | _] = NodesWithPid,
    StartNodePid ! {query_key, HashedKey, self()},
    receive
        {key_found, Node} ->
            io:format("Key ~p found at node ~p~n", [Key, Node]),
            Node;
        _Other ->
            io:format("Key ~p not found~n", [Key]),
            undefined
    end.
