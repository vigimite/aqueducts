use maud::{html, Markup, DOCTYPE};

// Common components
mod components {
    use maud::{html, Markup};

    pub fn section_header(title: &str, add_button_id: Option<&str>, add_button_text: Option<&str>) -> Markup {
        html! {
            div class="section-header" {
                h2 { (title) }
                @if let (Some(id), Some(text)) = (add_button_id, add_button_text) {
                    div class="section-controls" {
                        button type="button" id=(id) class="btn-add" { (text) }
                    }
                }
            }
        }
    }
    
    pub fn form_actions() -> Markup {
        html! {
            div class="form-actions" {
                button type="button" id="preview-pipeline" class="btn-primary" { "Preview" }
                button type="submit" id="save-pipeline" class="btn-primary" { "Save Pipeline" }
                button type="button" id="run-pipeline" class="btn-primary" { "Run Pipeline" }
            }
        }
    }
    
    pub fn preview_modal() -> Markup {
        html! {
            div id="preview-modal" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.7); z-index: 1000;" {
                div style="background-color: #222; margin: 10% auto; padding: 20px; width: 80%; max-width: 800px; border-radius: 5px; position: relative; border: 1px solid #444;" {
                    button id="close-modal" style="position: absolute; top: 10px; right: 10px; border: none; background: none; color: #ddd; font-size: 1.5rem; cursor: pointer;" { "×" }
                    h3 { "Pipeline Preview" }
                    pre id="preview-content" style="max-height: 60vh; overflow: auto; padding: 1rem; background-color: #111; color: #eee; border-radius: 4px; border: 1px solid #444;" {}
                }
            }
        }
    }
}

// Pipeline editor-specific components
mod pipeline_editor_components {
    use maud::{html, Markup};

    pub fn sources_section() -> Markup {
        html! {
            div class="section" id="sources-section" {
                (super::components::section_header("Sources", Some("add-source"), Some("Add Source")))
                div id="sources-container" {
                    // Initial source will be added by JavaScript
                }
            }
        }
    }
    
    pub fn stages_section() -> Markup {
        html! {
            div class="section" id="stages-section" {
                (super::components::section_header("Stages", Some("add-stage-row"), Some("Add Stage Row")))
                div id="stages-container" class="stages-container" {
                    // Initial stage will be added by JavaScript
                }
            }
        }
    }
    
    pub fn destination_section() -> Markup {
        html! {
            div class="section" id="destination-section" {
                (super::components::section_header("Destination", None, None))
                div id="destination-container" {
                    // Destination fields will be added by JavaScript
                }
            }
        }
    }
    
    pub fn pipeline_form_styles() -> Markup {
        html! {
            style {
                "
                /* Form layout and containers */
                .pipeline-form {
                    width: 100%;
                    margin-bottom: 2rem;
                }
                .section {
                    margin-bottom: 2rem;
                    padding: 1rem;
                    border: 1px solid #444;
                    border-radius: 4px;
                    background-color: #333;
                }
                .section-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 1rem;
                }
                .section-controls {
                    display: flex;
                    gap: 0.5rem;
                }
                
                /* Source/Destination fields */
                .source-item, .destination-item {
                    padding: 1rem;
                    margin-bottom: 1rem;
                    border: 1px solid #444;
                    border-radius: 4px;
                    background-color: #222;
                }
                .form-row {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 1rem;
                    margin-bottom: 1rem;
                }
                .form-group {
                    flex: 1;
                    min-width: 200px;
                }
                .form-group label {
                    display: block;
                    margin-bottom: 0.5rem;
                    font-weight: bold;
                    color: #ddd;
                }
                .form-group input, .form-group select, .form-group textarea {
                    width: 100%;
                    padding: 0.5rem;
                    border: 1px solid #555;
                    border-radius: 4px;
                    background-color: #333;
                    color: #eee;
                }
                
                /* Stages */
                .stages-container {
                    display: flex;
                    flex-direction: column;
                    gap: 1rem;
                }
                .stage-row {
                    display: flex;
                    gap: 1rem;
                    margin-bottom: 1rem;
                }
                .stage-item {
                    flex: 1;
                    padding: 1rem;
                    border: 1px solid #444;
                    border-radius: 4px;
                    background-color: #222;
                    position: relative;
                }
                .stage-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 0.5rem;
                }
                .stage-controls {
                    position: absolute;
                    top: 0.5rem;
                    right: 0.5rem;
                    display: flex;
                    gap: 0.25rem;
                }
                .stage-controls button {
                    padding: 0.25rem 0.5rem;
                    font-size: 0.8rem;
                }
                .editor-container {
                    width: 100%;
                    height: 200px;
                    border: 1px solid #555;
                    border-radius: 4px;
                    overflow: hidden;
                    margin-bottom: 1rem;
                }
                
                /* Buttons */
                .btn-add {
                    background-color: #2a9d8f;
                    color: white;
                }
                .btn-remove {
                    background-color: #e63946;
                    color: white;
                }
                .btn-primary {
                    background-color: #457b9d;
                    color: white;
                }
                .form-actions {
                    display: flex;
                    gap: 1rem;
                    margin-top: 2rem;
                    justify-content: flex-end;
                }
                
                /* Other elements */
                .hidden {
                    display: none;
                }
                "
            }
        }
    }
}

// Layout components
fn header(page_title: &str) -> Markup {
    html! {
        (DOCTYPE)
        meta charset="utf-8";
        meta name="viewport" content="width=device-width, initial-scale=1.0";
        title { (page_title) }
        link rel="stylesheet" href="https://cdn.simplecss.org/simple.min.css";
        link rel="icon" href="/static/logo.png" type="image/png";
    }
}

fn footer() -> Markup {
    html! {
        footer {
            p { "Aqueducts Pipeline Editor" }
        }
    }
}

// Main page layout
pub fn page(title: &str, inner: Markup) -> Markup {
    html! {
        (header(title))
        body {
            header {
                h1 { (title) }
            }
            main {
                (inner)
            }
            (footer())
        }
    }
}

// Pipeline editor component
pub fn pipeline_editor(initial_content: Option<String>) -> Markup {
    use pipeline_editor_components::*;
    use components::*;
    
    html! {
        // CSS Styles
        (pipeline_form_styles())
        
        // Main form
        form id="pipeline-form" class="pipeline-form" action="/api/pipeline" method="post" {
            // Sources Section
            (sources_section())
            
            // Stages Section
            (stages_section())
            
            // Destination Section
            (destination_section())
            
            // Form Actions
            (form_actions())
            
            // Hidden field to store the complete pipeline JSON
            input type="hidden" id="pipeline-json" name="pipeline" value="" {}
        }
        
        // Preview Modal
        (preview_modal())
        
        @if let Some(content) = initial_content {
            script { "window.initialContent = " (format!("{:?}", content)) ";" }
        }
        
        // Load CodeMirror and our custom editor script
        script type="module" src="/static/js/editor.js" {}
    }
}

#[cfg(test)]
mod tests {}