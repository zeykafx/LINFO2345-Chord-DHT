## Part 1:

### Number of nodes
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

### Output

This will generate a directory called `dht_NumberOfNodes.csv` in the src/part_1 directory (with NumberOfNodes being the value of that variable).
The directory will contain one csv file per node (with the name `node_NodeIndex.csv`), each containing one line following this syntax:
```
node_identifier,successor_identifier,predecessor_identifier|key1_identifier|key2_identifier|...
```
