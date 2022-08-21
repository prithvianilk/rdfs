# RDFS

An attempt to make a reliable, distributed file system inspired by Hadoop File System.

## The RDFS Project consists of 2 parts

### 1. RDFS Main
- A cli tool to start and configure Data Node and Name Node servers.

### Commands

### namenode

Starts a Name Node server on a host.

Usage: 

```
bin/rdfs.sh namenode 

Flags:
--name-node-port, Default: 3620 
--name-node-heartbeat-port, Default: 3630
```


### datanode

Starts a Data Node server on a host and join it to the RDFS cluster.

Usage: 

```
bin/rdfs.sh datanode 

Flags:
--name-node-address, Default: 0.0.0.0 
--name-node-heartbeat-port, Default: 3630 
--data-node-port, Default: 3530
```

### 2. RDFS Client
- A cli client tool to interact with RDFS. 

### Commands

### write

Writes the contents of a local file onto RDFS. 

Usage: 

```
bin/rdfs-client.sh write <local-filepath> <rdfs-file-name> 

Flags:
--name-node-address, Default: 0.0.0.0
--name-node-port, Default: 3620
--block-size, Default: 128 x 10^6
```

### read 

Reads the contents of a file on RDFS and writes it to a file locally.

Usage: 

```
bin/rdfs-client.sh read <new-local-filename> <rdfs-file-name> 

Flags:
--name-node-address, Default: 0.0.0.0 
--name-node-port, Default: 3620
```

### delete

Deletes a file from RDFS.

Usage: 

```
bin/rdfs-client.sh delete <rdfs-file-name> 

Flags:
--name-node-address, Default: 0.0.0.0 
--name-node-port, Default: 3620
```

## Build using Docker

### Build the RDFS CLI

```
docker build -t rdfs -f docker/Dockerfile.rdfs .
```

### Build the RDFS Client CLI

```
docker build -t rdfs-client -f docker/Dockerfile.client .
```
