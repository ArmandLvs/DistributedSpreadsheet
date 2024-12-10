---- MODULE CausalBroadcast ----
CONSTANT Nodes
CONSTANT Values
LOCAL INSTANCE Naturals
VARIABLE proc  
VARIABLE msgs 
VARIABLE clocks 
VARIABLE msgBag 

vars == <<proc, msgs, clocks, msgBag>>


Msgs == UNION {
    [t : {"PROPOSE"}, v : Values, cell : Values, src : Nodes],
    [t : {"UPDATE"}, v : Values, cell : Values, vc : [Nodes -> Nat ], src : Nodes, dst : Nodes]
}

TypeOK ==
    /\ proc \in [Nodes -> [Values -> Values]]
    /\ clocks \in [Nodes -> [Nodes -> Nat]]
    /\ msgBag \subseteq Msgs


IncrementClock(vc, n) ==
    [vc EXCEPT ![n] = @ + 1]

Max(vc1, vc2) ==
    [k \in Nodes |-> IF vc1[k] >= vc2[k] THEN vc1[k] ELSE vc2[k]]


UpdateProc(lproc, n, cell, value) ==
    [lproc EXCEPT ![n][cell] = value]


ReliableBroadcast(msg, src, dst) ==
    \E vc \in [Nodes -> Nat]:
        [t |-> "UPDATE", v |-> msg.v, cell |-> msg.cell, src |-> src, dst |-> dst, vc |-> vc]

\* handlers definitions
RecvPropose(p) ==
    \E m \in msgs :
        /\ m.t = "PROPOSE" /\ m.src = p
        /\ clocks' = [clocks EXCEPT ![p] = IncrementClock(clocks[p], p)]
        /\ msgs' = (msgs \ {m}) \cup { ReliableBroadcast(m, p, n) : n \in Nodes }
        /\ proc' = UpdateProc(proc, p, m.cell, m.v)

RecvUpdate(p) ==
    \E m \in msgs :
        /\ m.t = "UPDATE" /\ m.dst = p
        /\ clocks' = [clocks EXCEPT ![p] = Max(clocks[p], m.vc)]
        /\ msgBag' = msgBag \cup {m}

DeliverCausal(p) ==
    \E m \in msgBag :
        /\ m.dst = p
        /\ \A k \in Nodes : clocks[p][k] >= m.vc[k]
        /\ msgBag' = msgBag \ {m}
        /\ proc' = UpdateProc(proc, p, m.cell, m.v)
        /\ clocks' = [clocks EXCEPT ![p] = IncrementClock(clocks[p], m.src)]



Init ==
    /\ proc \in [Nodes -> [Values -> Values]]
    /\ msgs \subseteq [t : {"PROPOSE"}, v : Values, cell : Values, src : Nodes]
    /\ clocks = [n \in Nodes |-> [k \in Nodes |-> 0]]
    /\ msgBag = {}

Next ==
    \E p \in Nodes :
        \/ RecvPropose(p)
        \/ RecvUpdate(p)
        \/ DeliverCausal(p)

Fair == WF_vars(Next)
Spec == Init /\ [][Next]_vars /\ Fair


ReliableDelivery ==
    \A m \in msgs : \E p \in Nodes : m.dst = p => m \notin msgs

CausalDelivery ==
    \A m1, m2 \in msgs :
        /\ m1.src = m2.dst /\ m1.dst = m2.src
        /\ m1.vc[m1.src] < m2.vc[m1.src]
        => m1 \in msgs /\ m2 \notin msgs

InSync ==
    \A n1, n2 \in Nodes : proc[n1] = proc[n2]

EventuallyInSync ==
    <>[]InSync

THEOREM (Spec => ([]TypeOK /\ ReliableDelivery /\ CausalDelivery /\ EventuallyInSync))
PROOF OMITTED
======
