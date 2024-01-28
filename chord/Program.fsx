#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Threading
open System.Timers

let system = ActorSystem.Create("FSharp")

// number of bits
let m = 20

let mutable numberOfRequests = 0

let mutable numNodes = 0

// max number of unique keys possible
let maxKeys = int (2.0 ** float m)

// variable to maintain total number of hops for the requests/lookups made
let mutable totalHops = 0

// mapping between actor of chord node and the identifier of node
let actorNodesMap = new Dictionary<int, IActorRef>()

// Operations performed on the node actor existing in a chord network
type ChordNodeOperations = 
    | Create of int
    | Join of int * int
    | FindSuccessor of int*int*int*int
    | Stabilize
    | StabilizeReceiver of int
    | StabilizeSuccessor of int
    | Notify of int 
    | FixFingers 
    | ShowFingerTable
    | SendSuccessor of int
    | SendFingerSuccessor of int * int
    | LookupKey of int * int *int // Source node ID, target key
    | LookupResponse of int * int // Number of hops, Source node ID


// checks if the key lies within the range of numbers
let withinRange key left right = 
    let modulo = key%maxKeys
    
    if left=right 
        then true
    elif left<right then
        if left<modulo && modulo<=right then true
        else false
    elif right<left then
        if (right<modulo && modulo<=left) then false
        else true
    else false


// key is the target to be found and id is the source node 
// whose finger table is being checked and finger_table is its corressponding fingertable
// Finds and returns the node identifier of closest preceeeding node in the path to find the key
let closest_preceeeding_node (key:int) (id :int) (finger_table:int array) =
    let mutable result = id
    let mutable i = m-1
    let mutable runLoop = true
    while runLoop && i>=0 do
        if withinRange finger_table.[i] id key then
            result <- finger_table.[i]
            runLoop <- false
        i <- i-1
    result  


// Runs the node stablilization and fixes fingers periodically for a fixed time interval
let runPeriodically (actorRef:IActorRef) =
        actorRef <! Stabilize
        actorRef <! FixFingers


// Actor for the node present in the network 
// which acts like a state machine and stores successors, fingertable and the identifier value corresponding to a node
// Also performs various operation on the nodes and communicates with other nodes by sending messages
let ChordNode (actorRef: Actor<_>) = 

    let mutable node_id = -1
    let mutable finger_table = Array.create m -1
    let mutable successor = -1
    let mutable predecessor = -1
    let mutable next = 0

    let rec foo() = actor{
        let! message = actorRef.Receive()
        match message with 
        | Create id -> // creates the first node and responsible for chord network generation
            node_id <- id
            successor <- id
            // initialize finger table
            finger_table <- [|for i in 0 .. m-1 -> node_id|]
            
        | Join (oldNode:int, newNode:int) -> // takes the identifiers of old and new nodes and join the new node to old node 
            node_id <- newNode
            successor <- newNode
            // initialize finger table
            finger_table <- [|for i in 0 .. m-1 -> node_id|]
            actorNodesMap.[oldNode] <! FindSuccessor (oldNode, newNode, 1, 0)

        // find successor node of an identifier, here there are 2 settings, one for normal successor when join is initiated 
        // and the other one is for finding successor of the finger during fix fingers
        | FindSuccessor (source, target, setting, finger) -> 
            if (withinRange target node_id successor) then
                if setting = 1 then 
                // send successor to the respective target to store it and update
                    actorNodesMap.[target] <! SendSuccessor successor
                else 
                // send successor to the respective source node to update the corresponding finger
                    actorNodesMap.[source] <! SendFingerSuccessor (finger, successor) 
            else
                let mutable close_finger = closest_preceeeding_node target node_id finger_table

                actorNodesMap.[close_finger] <! FindSuccessor (source, target, setting, finger) 

        // receives the successor sent by the preceeding node and updates its successor
        | SendSuccessor correct_successor->
            successor <- correct_successor

        // called periodically to know if any new nodes are joined and update successors and predecessors accordingly
        | Stabilize ->
            if not(successor<0) then
            // ask successor to check its predessor
                actorNodesMap.[successor] <! StabilizeReceiver node_id

        // check if the identifier from where message is received and see if the current one is the right successor 
        | StabilizeReceiver source->
            if predecessor <> -1 then
                if withinRange predecessor source node_id 
                    then
                        actorNodesMap.[source] <! StabilizeSuccessor predecessor

            // check if the source is going to be the predecessor
            actorRef.Self <! Notify source
        
        // update successor of the preceeding node to right value
        | StabilizeSuccessor predecessor_of_successor->
            successor <- predecessor_of_successor%maxKeys

        // checks if the source is the predecessor and updates the predecessor
        | Notify source ->
            if predecessor = -1  || withinRange source predecessor node_id then 
                predecessor <- source%maxKeys

        // This is called periodically to fix all the fingers of the node
        | FixFingers ->
            if not(successor<0) then
                // find finger successors
                actorRef.Self <! FindSuccessor (node_id, (node_id + int ( 2.0 ** float next))%maxKeys, 2, next)
                next <- next + 1 ;
                if (next > m-1) then 
                    next <- 0 

        // Updates the finger successor correcly sent by the source
        | SendFingerSuccessor (finger , successor_of_finger) ->
            finger_table.[finger] <- successor_of_finger

        // Displays the finger table of a node
        | ShowFingerTable ->
            printfn "Finger table of %d" node_id
            for i = 0 to m-1 do
                printfn "%d" finger_table.[i]
            printfn "successor is %d" successor
            printfn "---------------------"
        
        // scalable key look up of an identifier in a node and calculates the hops to reach the right sucessor of the key's identifier
        | LookupKey (source, key, hops) -> 
            // when key's successor is found add the hop count to total hops
            if withinRange key  node_id successor then 
                actorNodesMap.[source] <! LookupResponse (hops, source) // Only one hop, the key resides in the node itself.
            else 
                actorNodesMap.[closest_preceeeding_node key node_id finger_table] <! LookupKey(source, key, hops+1)

        // updates the total hop count 
        | LookupResponse (hops, source) ->
            totalHops <- totalHops + hops    

        return! foo() 
    }
    
    // periodically call the stabilize and fix fingers 
    let timer = new Timer(100.0)
    timer.Elapsed.Add(fun args ->
        runPeriodically actorRef.Self 
    )
    timer.Start()
    
    foo()


// chord network creation and the simulator to shoot look up requests
let create_chord_network numNodes requests =

    printfn "maxKeys is %d" maxKeys
    let mutable nodeIds = [||]
    let mutable headActorRef = null
    let mutable generatedNumbers = Set.empty

    // create a chord actor system
    let system = ActorSystem.Create("ChordSystem")
    let random = Random()


    let nodeIds = [|for i in 0 .. numNodes-1 -> i*(5)|]

    for i in 0 .. numNodes-1 do
        let chordNode = sprintf "ChordNode_%d" nodeIds.[i]
        // create individual actors 
        let actorRef = spawn system  chordNode  ChordNode

        // Add the actor reference to the map
        actorNodesMap.Add(nodeIds.[i], actorRef)

        // first node to create network
        if i = 0  then
            actorRef <! Create nodeIds.[i]
        // join other nodes to the network     
        else 
            actorRef <! Join (nodeIds.[0], nodeIds.[i])

    // wait for nodes to stabilize
    Thread.Sleep(30000)

    // Start the lookup simulation
    let random = Random()

    for _ in 1 .. numberOfRequests do
        for j in nodeIds do
            let targetKey = random.Next(0, maxKeys)
            actorNodesMap.[j] <! LookupKey (j, targetKey, 1)
    
    Thread.Sleep(15000) // Wait for lookups to complete. Adjust this time as needed.

    printfn "total hops is %d" totalHops
    printfn "Average number of hops: %f" (float totalHops / (float requests * float numNodes))


let main () =
    let inputs = System.Environment.GetCommandLineArgs()
    let mutable nodes = 0
    let mutable requests = 0


    Int32.TryParse(inputs.[2], &nodes)|> ignore
    Int32.TryParse(inputs.[3], &requests)|> ignore

    
    numNodes <- nodes
    numberOfRequests <- requests

    printfn "Num Nodes: %d" numNodes
    printfn "Num Requests: %d" numberOfRequests
    create_chord_network numNodes numberOfRequests

main()
