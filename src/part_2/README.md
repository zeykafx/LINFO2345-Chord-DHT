## Part 2:

### Number of nodes and node to start queries from
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

### Output
This will generate a csv file called `node_StartingNodeIndex_queries.csv` in the src/part_2 directory (with StartingNodeIndex being the value of that variable).

The csv file will contain one line for each key queried, with the following format:
```
key_identifier,contacted_node_identifier1|contacted_node_identifier2|contacted_node_identifier3...
```
