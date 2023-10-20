use db::{
    establish_connection,
    logic::{delete_graph, execute, DbContext},
    read_config,
};

mod db;

#[tokio::main]
async fn main() {
    let db_context = prerequisites().await.unwrap();
    delete_graph(&db_context).await;

    execute(&db_context).await;
}

async fn prerequisites() -> Result<DbContext, neo4rs::Error> {
    let config_file = "sensitive/properties.yaml";
    let config = read_config(config_file);

    let graph = establish_connection(config).await;
    let graph = match graph {
        Ok(res) => res,
        Err(err) => {
            std::eprint!("{}", err);
            return Err(err);
        }
    };

    Ok(db::logic::DbContext::create(graph))
}
