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
use tower_http::services::ServeDir;

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
        let static_files = ServeDir::new("static").append_index_html_on_directories(false);

        let app = Router::new()
            .route("/health", get(|| async { StatusCode::OK }))
            .route("/web", get(get_page))
            .route("/", get(|| async { Redirect::permanent("/web") }))
            .nest_service("/static", static_files);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port))
            .await
            .unwrap();

        axum::serve(listener, app).await.unwrap();
    }
}

async fn get_page() -> impl IntoResponse {
    aqueducts_web::page(
        "Aqueducts Pipeline Editor",
        aqueducts_web::pipeline_editor(None),
    )
}

#[tokio::main]
async fn main() {
    let app = App::new(3030);

    app.run().await;
}
