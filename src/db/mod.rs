use neo4rs::{ConfigBuilder, Graph};
use serde_yaml::Value;

pub mod logic;

pub struct DbConfig {
    uri: String,
    user: String,
    password: String,
}

impl DbConfig {
    pub fn create(uri: &str, user: &str, password: &str) -> Self {
        Self {
            uri: String::from(uri),
            user: String::from(user),
            password: String::from(password),
        }
    }

    pub fn uri(&self) -> String {
        self.uri.clone()
    }
    pub fn user(&self) -> String {
        self.user.clone()
    }
    pub fn password(&self) -> String {
        self.password.clone()
    }
}

pub fn read_config(file_path: &str) -> DbConfig {
    let file = std::fs::File::open(file_path).unwrap();
    let data: Value = serde_yaml::from_reader(file).unwrap();
    let connection = data.get("database").unwrap().get("connection").unwrap();

    let uri = connection.get("uri").unwrap().as_str().unwrap();
    let user = connection.get("user").unwrap().as_str().unwrap();
    let password = connection.get("password").unwrap().as_str().unwrap();

    DbConfig::create(uri, user, password)
}

pub async fn establish_connection(dbconf: DbConfig) -> Result<Graph, neo4rs::Error> {
    let config = ConfigBuilder::default()
        .uri(dbconf.uri())
        .user(dbconf.user())
        .password(dbconf.password())
        .build()?;
    Graph::connect(config).await
}
