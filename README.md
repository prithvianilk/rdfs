# RDFS

An attempt to make a reliable, distributed file system inspired by Hadoop File System.

## Project Structure

### 1. RDFS Main
- A cli tool to start and configure Data Node and Name Node servers.

### Commands

### `namenode`

Starts a Name Node server on a host.

Example: 

`bin/rdfs.sh namenode --name-node-port 3620 --name-node-heartbeat-port 3630`


### `datanode`

Starts a Data Node server on a host and join it to the RDFS cluster.

Example: 

`bin/rdfs.sh datanode --name-node-address 0.0.0.0 --name-node-heartbeat-port 3630 --data-node-port 3530`

### 2. RDFS Client
- A cli client tool to interact with RDFS. 

### Commands

### `write`

Writes the contents of a local file onto RDFS. 

Example: 

`bin/rdfs-client.sh write <local-filepath> <rdfs-file-name> --name-node-address 0.0.0.0 --name-node-port 3620 --block-size <preferred block size>`

### `read` 

Reads the contents of a file on RDFS and writes it to a file locally.

Example: 

`bin/rdfs-client.sh read <new-local-filename> <rdfs-file-name> --name-node-address 0.0.0.0 --name-node-port 3620`

### `delete`

Deletes a file from RDFS.

Example: 

`bin/rdfs-client.sh delete <rdfs-file-name> --name-node-address 0.0.0.0 --name-node-port 3620`

## TODO

- [ ] Perform file data migration from unavailable datanode to available datanode.
- [ ] Fix Dockerfiles.
- [ ] Write better README documentation (it's pretty sloppy right now).
