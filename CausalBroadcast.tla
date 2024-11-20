---- MODULE CausalBroadcast ----
EXTENDS Naturals, Sequences

CONSTANTS 
    Nodes,        \* Set of all processes (nodes) in the system.
    Messages      \* Set of all possible messages.

VARIABLES 
    msgBag,       \* Mapping: node -> set of undelivered messages.
    vectorClock,  \* Mapping: node -> vector clock (a function Nodes -> Nat).
    delivered     \* Set of delivered messages.

(* --initial state-- *)
Init ==
    /\ msgBag = [n \in Nodes |-> {}]
    /\ vectorClock = [n \in Nodes |-> [m \in Nodes |-> 0]]
    /\ delivered = {}

(* --actions-- *)

(* Broadcasting a message *)
Broadcast(n, msg) ==
    /\ n \in Nodes
    /\ msg \in Messages
    /\ ~ (msg \in delivered)  \* Ensure the message isn't already delivered.
    /\ delivered' = delivered \cup {msg}  \* Local delivery upon broadcast.
    /\ vectorClock' = [vectorClock EXCEPT ![n][n] = @ + 1]
    /\ msgBag' = [msgBag EXCEPT ![ m \in Nodes \ {n}] = @ \cup {[<<msg, vectorClock[n]>>]}]
    /\ UNCHANGED <<msgBag[n]>>

(* Receiving a message *)
Receive(n, msg) ==
    /\ n \in Nodes
    /\ msg \in Messages
    /\ msg \notin delivered
    /\ \E sender \in Nodes : 
        LET msgVC == vectorClock[sender]
        IN
        /\ msgBag' = [msgBag EXCEPT ![n] = @ \cup {[<<msg, msgVC>>]}]
        /\ UNCHANGED <<vectorClock, delivered>>

(* Delivering a causally ready message *)
Deliver(n) ==
    \E msg \in msgBag[n] :
        LET 
            ms == msg[1]      \* The message itself
            msgVC == msg[2]   \* The vector clock associated with the message
        IN
        /\ \A k \in Nodes : vectorClock[n][k] >= msgVC[k]  \* Causality condition.
        /\ delivered' = delivered \cup {ms}
        /\ msgBag' = [msgBag EXCEPT ![n] = @ \ {msg}]
        /\ vectorClock' = [vectorClock EXCEPT ![n][Sender(ms)] = @ + 1]

(* --next-state relation-- *)
Next ==
    \/ \E n \in Nodes, msg \in Messages : Broadcast(n, msg)
    \/ \E n \in Nodes, msg \in Messages: Receive(n, msg)
    \/ \E n \in Nodes : Deliver(n)

(* --temporal specification-- *)
Spec ==
    Init /\ [][Next]_<<msgBag, vectorClock, delivered>>

(* --properties-- *)

(* Causal delivery: A message can only be delivered if all causally prior messages have been delivered. *)
CausalDelivery ==
    \A n \in Nodes : 
        \A msg \in msgBag[n] : 
            LET msgVC == msg[2] IN
            \A k \in Nodes : vectorClock[n][k] >= msgVC[k]

(* No duplication: Messages are delivered at most once. *)
NoDuplication ==
    \A msg \in delivered : 
        \neg (\E n \in Nodes : msg \in msgBag[n])

(* Liveness: Every message broadcast is eventually delivered to all nodes. *)
MessageDelivery ==
    \A m \in Nodes, msg \in Messages :
        Broadcast(m, msg) => 
        \A n \in Nodes : \E t : Deliver(n)

====

