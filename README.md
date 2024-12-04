# LINFO2345-Chord-DHT

## Part 1:

### Parameters
#### Number of nodes
To change the number of nodes, change the value of the variable `NumberOfNodes` in the `control.erl` file (line 10)

### How to run
To run part 1:
Make sure you are in the src/part_1 directory and run
```bash
erl
```
Then run
```
c(dht), c(control), control:start().
```

Or run the following command in the src/part_1 directory:
```bash
make all
```
Which will compile and run the code for you (but it doesn't print errors).

### Output

This will generate a directory called `dht_NumberOfNodes.csv` in the src/part_1 directory (with NumberOfNodes being the value of that variable).
The directory will contain one csv file per node (with the name `node_NodeIndex.csv`), each containing one line following this syntax:
```
node_identifier,successor_identifier,predecessor_identifier|key1_identifier|key2_identifier|...
```


## Part 2:

### Parameters
#### Number of nodes and node to start queries from
To change the number of nodes, change the value of the variable `NumberOfNodes` & `StartingNodeIndex` in the `control.erl` file (line 10 & 11)

### How to run
To run part 2:
Make sure you are in the src/part_2 directory and run
```bash
erl
```

Then run

```
c(dht), c(control), control:start().
```

Or run the following command in the src/part_2 directory:
```bash
make all
```
This will generate a csv file called `node_StartingNodeIndex_queries.csv` in the src/part_2 directory (with StartingNodeIndex being the value of that variable).

### Output
The csv file will contain one line for each key queried, with the following format:
```
key_identifier,contacted_node_identifier1|contacted_node_identifier2|contacted_node_identifier3...
```


## Part 3:
