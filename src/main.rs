extern crate timely;
extern crate differential_dataflow;

use abomonation_derive::Abomonation;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::{Iterate, Reduce};
use uuid::Uuid;
use crate::Node::{Add, Conn, Suc, Zero};

//Node variants with their passive port ids
//Their active port id will be the first element of the tuple they are in
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Abomonation, Hash)]
enum Node {
    Suc(u128),
    Zero,
    Add(u128, u128),
    Conn(u128),
}

impl Node {
    fn construct_nat(nat: u64) -> (u128, Vec<(u128, Node)>) {
        let mut result = Vec::new();

        let mut prev_uuid = Uuid::new_v4().as_u128();
        result.push((prev_uuid, Zero));

        for _ in 1..nat {
            let uuid = Uuid::new_v4().as_u128();
            result.push((uuid, Suc(prev_uuid)));
            prev_uuid = uuid
        }

        (prev_uuid, result)
    }

    fn construct_add(nat1: u64, nat2: u64, output_id: u128) -> (Vec<(u128, Node)>, Vec<(u128, Node)>, (u128, Node)) {
        let (id1, vec1) = Self::construct_nat(nat1);
        let (id2, vec2) = Self::construct_nat(nat2);

        (vec1, vec2, (id1, Add(id2, output_id)))
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();

        let probe = worker.dataflow(|scope| {
            let nodes = input.to_collection(scope);

            //iterate does the nested transformation until the output stops changing (normal form)
            nodes.iterate(|transitive| {
                //reduce groups elements with same id, in this case the id is the active port
                transitive.reduce(|key, input, output| {
                    if input.len() == 1 {
                        //no active-active connection
                        output.push(((*key, *input[0].0), input[0].1));
                    } else {
                        //active-active connection!
                        let nodes = (input[0].0, input[1].0);
                        //the rewrite rules for addition
                        match nodes {
                            (Suc(s), Add(l, r)) | (Add(l, r), Suc(s)) => {
                                println!("--Doing a Succ/Add reduction--");
                                output.push(((*r, Suc(*key)), 1));
                                output.push(((*s, Add(*l, *key)), 1));
                            }
                            (Zero, Add(l, r)) | (Add(l, r), Zero) => {
                                println!("--Doing a Zero/Add reduction--");
                                //TODO: works only if l points to active port, fine for addition
                                //the reason this is needed is because we cannot modify
                                //the connection of the connected elements l, r to point to each other
                                output.push(((*l, Conn(*r)), 1));
                            }
                            (Conn(k), n) | (n, Conn(k)) => {
                                //Conn elements are reified here, if n is active-active connected to a Conn,
                                // just output n again pointing to the passive port of conn
                                output.push(((*k, *n), 1));
                            }
                            _ => {
                                //noop, unreachable unless invalid network
                                output.push(((*key, *nodes.0), 1));
                                output.push(((*key, *nodes.1), 1));
                            }
                        }
                    }
                })
                    //reduce wraps the result in the previous key again,
                    //but we provided our own, so we remove the wrapper
                    .map(|(_k, v)| v)
            })
                .consolidate()
                .inspect(|x| println!("Output: {:?}", x))
                .probe()
        });

        let l = 3;
        let r = 4;

        let (nat1, nat2, addition) = Node::construct_add(l, r, 0);
        println!("Adding {} to {}", l, r);
        println!("Each output will represent a diff,");
        println!("the change in multiplicity is the last element of the tuple");
        input.advance_to(0);
        for node in &nat1 {
            input.insert(*node);
        }
        for node in &nat2 {
            input.insert(*node);
        }
        input.insert(addition);


        println!("Fully reduced network:");
        input.advance_to(1);

        //wait for computation to finish,
        //only needed here so that the log output is in the correct order
        input.flush();
        while probe.less_than(&input.time()) {
            worker.step();
        }

        println!();
        println!("Decrementing nat1...");

        let z = nat1[0];
        let fs = nat1[1];
        let (x, _) = fs;

        //remove the Zero and first Succ,
        //replace with a Zero pointing to the element the Succ was pointing to
        input.remove(z);
        input.remove(fs);
        input.insert((x, Zero));

        println!("Change in fully reduced network:");
        input.advance_to(2);
        input.flush();
        while probe.less_than(&input.time()) {
            worker.step();
        }

        println!();
        println!("Incrementing nat2...");

        let new_id = Uuid::new_v4().as_u128();
        let (id, _) = nat2[0];

        //Remove the first Zero and insert a Suc + Zero
        input.remove(z);
        input.insert((id, Suc(new_id)));
        input.insert((new_id, Zero));

        println!("Change in fully reduced network:");
        input.advance_to(3);
        input.flush();
        while probe.less_than(&input.time()) {
            worker.step();
        }

        input.close()
    }).expect("Error in dataflow construction");
}