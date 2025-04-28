use maud::{html, Markup, DOCTYPE};

fn header(page_title: &str) -> Markup {
    html! {
        (DOCTYPE)
        meta charset="utf-8";
        title { (page_title) }
        link rel="stylesheet" href = "https://cdn.simplecss.org/simple.min.css" {}
    }
}

fn footer() -> Markup {
    html! {
        footer {
            a href="rss.atom" { "RSS Feed" }
        }
    }
}

pub fn page(title: &str) -> Markup {
    html! {
        (header(title))
        h1 { (title) }
        (footer())
    }
}

#[cfg(test)]
mod tests {}
