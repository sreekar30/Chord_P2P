# Distributed Operating System Project

This project implements a Chord Network Protocol of nodes — a decentralized peer-to-peer system, that is capable of handling dynamic node joins. It also performs a scalable key lookups within it, to measure efficiency and hop counts.

This project was developed in F# using AKKA framework to handle the asynchronous communication between nodes.

Execution Steps

Firstly checkout to chord folder and run the below command

command to execute - 
```
dotnet fsi Program.fsx <num_of_nodes> <num_of_requests>
```
num_of_nodes - This is an integer that represents the number of peers/nodes within the Chord network.
num_of_requests - This refers to the number of requests that each peer is responsible for handling (lookup requests).
