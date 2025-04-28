mod config;
mod data_catalog;
mod executions;
mod namespaces;
mod users;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::get,
    Router,
};
use config::Config;

struct App {
    config: Config,
    port: u16,
}

impl App {
    fn new(port: u16) -> Self {
        let config = Config::load();
        Self { config, port }
    }

    async fn run(&self) {
        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route("/web", get(get_page))
            .route("/", get(|| async { Redirect::permanent("/web") }));

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .unwrap();

        axum::serve(listener, app).await.unwrap();
    }
}

async fn get_page() -> impl IntoResponse {
    aqueducts_web::page("Aqueducts")
}

#[tokio::main]
async fn main() {
    let app = App::new(3030);

    app.run().await;
}
