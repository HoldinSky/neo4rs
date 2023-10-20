use std::fmt::{Debug};

use futures::future::join;
use neo4rs::{Error, Graph, Node, query};
use rand::{Rng, thread_rng};

macro_rules! pair {
    ($first:expr, $second:expr) => {
        {
            Pair { first: $first, second: $second}
        }
    }
}

pub struct DbContext {
    graph: Graph,
}

#[derive(Clone, Debug)]
struct Path {
    from: NodeParams,
    to: NodeParams,
}

#[derive(Clone, Debug)]
struct NodeParams {
    label: String,
    prop: Prop,
}

#[derive(Clone, Debug)]
struct Prop {
    key: String,
    value: String,
}

#[derive(Clone, Debug)]
struct Pair<T, P> {
    first: T,
    second: P,
}

impl PartialEq for Pair<&String, &String> {
    fn eq(&self, other: &Self) -> bool {
        self.first == other.first && self.second == other.second
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl PartialEq for Pair<String, String> {
    fn eq(&self, other: &Self) -> bool {
        self.first == other.first && self.second == other.second
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

const REL: &str = "RELATES";

#[allow(dead_code)]
impl DbContext {
    pub fn create(graph: Graph) -> Self {
        Self { graph }
    }

    async fn create_nodes_of_label(&self, amount: i32, label: &str) {
        let transaction = self.graph.start_txn().await.unwrap();

        let mut queries: Vec<neo4rs::Query> = Vec::new();
        for i in 0..amount {
            queries.push(query(
                format!("CREATE (n:{label} {{name: \"{label}_{i}\"}})").as_str(),
            ));
        }
        transaction.run_queries(queries).await.unwrap();

        transaction.commit().await.unwrap();
    }

    async fn connect_nodes(&self, label_1: &str, label_2: &str) {
        let possible = self.find_possible_relations(label_1, label_2).await;
        let possible_size = possible.len();

        let needed_relation_count = (self.get_all_names_of_label(label_1).await.len() +
            self.get_all_names_of_label(label_1).await.len()) * 2;

        let mut relations = vec![];
        let txn = self.graph.start_txn().await.unwrap();

        let mut queries = vec![];

        let mut count = 0;
        let mut rnd = thread_rng();
        while count < needed_relation_count {
            let index = rnd.gen_range(0..possible_size);
            let pair = possible.get(index).clone().unwrap();
            if relations.contains(&pair) {
                continue;
            }

            let first = pair.first.clone();
            let second = pair.second.clone();
            relations.push(pair);

            queries.push(query(format!("MATCH (one:{label_1} {{name: \"{first}\"}}) MATCH (two:{label_2} {{name: \"{second}\"}}) CREATE (one) - [:{REL}] -> (two)").to_owned().as_str()));
            count += 1;
        }

        txn.run_queries(queries).await.unwrap();
        txn.commit().await.unwrap();
    }

    async fn find_possible_relations(&self, label_1: &str, label_2: &str) -> Vec<Pair<String, String>> {
        let query_str = format!(
            "MATCH (one:{label_1}) MATCH (two:{label_2}) RETURN one, two"
        );

        let mut result = self.graph.execute(query(query_str.to_owned().as_str())).await.unwrap();

        let mut relations = Vec::new();
        while let Ok(Some(row)) = result.next().await {
            let first: Node = row.get("one").unwrap();
            let second: Node = row.get("two").unwrap();

            let first: String = first.get("name").unwrap();
            let second: String = second.get("name").unwrap();

            relations.push(pair!(first, second));
        };

        relations
    }

    async fn get_all_names_of_label(&self, label: &str) -> Vec<String> {
        let query_str = format!(
            "MATCH (n:{label}) RETURN n"
        );

        let mut result = self.graph.execute(query(query_str.to_owned().as_str())).await.unwrap();

        let mut nodes = Vec::new();
        while let Ok(Some(row)) = result.next().await {
            let node: Node = row.get("n").unwrap();

            let name: String = node.get("name").unwrap();

            nodes.push(name);
        };

        nodes
    }

    async fn find_descending(
        &self,
        list: Vec<Path>,
        rel: String,
        depth: i32,
    ) -> Vec<Pair<Path, i64>> {
        let mut paths = Vec::new();

        for path in list {
            let cloned_path = path.clone();
            if let Ok(shortest) = self.find_shortest(path.from, path.to, rel.clone(), depth).await {
                paths.push(Pair { first: cloned_path, second: shortest });
            }
        };

        paths.sort_by(|val1, val2| val2.second.cmp(&val1.second));
        paths
    }

    async fn find_shortest(
        &self,
        param1: NodeParams,
        param2: NodeParams,
        rel: String,
        depth: i32,
    ) -> Result<i64, neo4rs::Error> {
        let query_str = format!(
            "MATCH (one:{l_1} {{{key_1}: \"{val_1}\"}}) MATCH (two:{l_2} {{{key_2}: \"{val_2}\"}}), p = shortestPath((one) - [:{rel}*..{depth}] - (two)) RETURN LENGTH(p)",
            l_1 = param1.label,
            key_1 = param1.prop.key,
            val_1 = param1.prop.value,
            l_2 = param2.label,
            key_2 = param2.prop.key,
            val_2 = param2.prop.value
        );

        let mut result = self
            .graph
            .execute(query(query_str.to_owned().as_str()))
            .await
            .unwrap();

        return if let Ok(Some(row)) = result.next().await {
            let length: i64 = row.get("LENGTH(p)").unwrap();
            Ok(length)
        } else {
            Err(Error::StringTooLong)
        };
    }

    async fn scramble_paths(&self, label1: String, label2: String, needed_count: i32) -> Vec<Path> {
        let fut1 = self.get_all_names_of_label(label1.as_str());
        let fut2 = self.get_all_names_of_label(label2.as_str());

        let (names1, names2) = join(fut1, fut2).await;

        let size1 = names1.len();
        let size2 = names2.len();

        let mut paths = vec![];
        let mut rnd = thread_rng();

        let mut relations = Vec::new();
        let mut count = 0;
        while count < needed_count {
            let from_prof = rnd.gen_bool(0.5);
            let to_prof = rnd.gen_bool(0.5);

            let from_ind = if from_prof { rnd.gen_range(0..size1) } else { rnd.gen_range(0..size2) };
            let to_ind = if to_prof { rnd.gen_range(0..size1) } else { rnd.gen_range(0..size2) };
            if from_ind == to_ind {
                continue;
            }

            let from_name = if from_prof {
                names1.get(from_ind).unwrap()
            } else {
                names2.get(from_ind).unwrap()
            };
            let to_name = if to_prof {
                names1.get(to_ind).unwrap()
            } else {
                names2.get(to_ind).unwrap()
            };


            if relations.contains(&Pair { first: from_name, second: to_name }) {
                continue;
            }

            paths.push(Path {
                from: NodeParams { label: if from_prof {label1.clone()} else {label2.clone()}, prop: { Prop { key: String::from("name"), value: from_name.clone() } } },
                to: NodeParams { label: if to_prof {label1.clone()} else {label2.clone()}, prop: { Prop { key: String::from("name"), value: to_name.clone() } } },
            });

            relations.push(pair!(from_name, to_name));
            count += 1;
        };

        paths
    }

    async fn delete(&self) {
        self.graph
            .run(query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n, r"))
            .await
            .unwrap();
    }
}

pub async fn execute(context: &DbContext) {
    let prof_label = String::from("Professor");
    let stud_label = String::from("Student");

    context.create_nodes_of_label(10, prof_label.as_str()).await;
    context.create_nodes_of_label(10, stud_label.as_str()).await;
    context.connect_nodes(prof_label.as_str(), stud_label.as_str()).await;

    let descending_order = context
        .find_descending(
            context.scramble_paths(prof_label, stud_label, 20).await,
            REL.to_string(),
            10,
        ).await;

    for (i, pair) in descending_order.iter().cloned().enumerate() {
        let path = pair.first;
        let length = pair.second;

        println!("{}. Path from '{}' to '{}' length = {}", i + 1, path.from.prop.value, path.to.prop.value, length);
    }
}

#[allow(dead_code)]
pub async fn delete_graph(context: &DbContext) {
    context.delete().await;
}
