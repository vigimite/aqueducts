// Load required CodeMirror resources
function loadScript(src, callback) {
  const script = document.createElement("script");
  script.src = src;
  script.onload = callback;
  document.head.appendChild(script);
}

function loadStylesheet(href) {
  const link = document.createElement("link");
  link.rel = "stylesheet";
  link.href = href;
  document.head.appendChild(link);
}

// Load CodeMirror 5 resources
loadStylesheet("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.css");
loadStylesheet("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/theme/dracula.min.css");

// Load main CodeMirror library
loadScript("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.js", () => {
  // Load SQL mode and addons
  loadScript("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/mode/sql/sql.min.js", () => {
    loadScript("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/addon/edit/matchbrackets.min.js", () => {
      loadScript("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/addon/edit/closebrackets.min.js", () => {
        // Load YAML mode for pipeline preview
        loadScript("https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/mode/yaml/yaml.min.js", () => {
          // Initialize the pipeline editor once all dependencies are loaded
          initPipelineEditor();
        });
      });
    });
  });
});

// Default SQL query templates
const DEFAULT_SQL = `SELECT
  column1,
  column2
FROM source_table
WHERE condition = true
ORDER BY column1`;

// Default empty pipeline structure
const emptyPipeline = {
  sources: [],
  stages: [],
  destination: {}
};

// Store all CodeMirror instances
const editors = {
  stages: {},
  preview: null
};

// Main initialization function
function initPipelineEditor() {
  // Add initial UI elements
  addInitialSource();
  addInitialStageRow();
  addDestination();
  setupEventListeners();
  
  // Check for initial content
  if (window.initialContent) {
    try {
      loadPipeline(JSON.parse(window.initialContent));
    } catch (e) {
      console.error("Failed to parse initial content:", e);
    }
  }
}

// Add a new source to the form
function addSource(sourceData = null) {
  const sourceContainer = document.getElementById("sources-container");
  const sourceId = `source-${Date.now()}`;
  
  const sourceItem = document.createElement("div");
  sourceItem.className = "source-item";
  sourceItem.dataset.id = sourceId;
  
  sourceItem.innerHTML = `
    <div class="form-row">
      <div class="form-group">
        <label for="${sourceId}-type">Type</label>
        <select id="${sourceId}-type" name="${sourceId}-type" required>
          <option value="File" ${sourceData && sourceData.type === "File" ? "selected" : ""}>File</option>
          <option value="Odbc" ${sourceData && sourceData.type === "Odbc" ? "selected" : ""}>ODBC</option>
        </select>
      </div>
      <div class="form-group">
        <label for="${sourceId}-name">Name</label>
        <input type="text" id="${sourceId}-name" name="${sourceId}-name" required 
          value="${sourceData?.name || ''}">
      </div>
      <div class="form-group">
        <button type="button" class="btn-remove remove-source">Remove</button>
      </div>
    </div>
    
    <div class="file-options ${sourceData && sourceData.type !== "File" ? "hidden" : ""}">
      <div class="form-row">
        <div class="form-group">
          <label for="${sourceId}-file-type">File Type</label>
          <select id="${sourceId}-file-type" name="${sourceId}-file-type">
            <option value="Csv" ${sourceData?.file_type?.type === "Csv" ? "selected" : ""}>CSV</option>
            <option value="Parquet" ${sourceData?.file_type?.type === "Parquet" ? "selected" : ""}>Parquet</option>
            <option value="Json" ${sourceData?.file_type?.type === "Json" ? "selected" : ""}>JSON</option>
          </select>
        </div>
        <div class="form-group">
          <label for="${sourceId}-location">Location</label>
          <input type="text" id="${sourceId}-location" name="${sourceId}-location" 
            value="${sourceData?.location || ''}">
        </div>
      </div>
    </div>
    
    <div class="odbc-options ${sourceData && sourceData.type === "Odbc" ? "" : "hidden"}">
      <div class="form-row">
        <div class="form-group">
          <label for="${sourceId}-connection-string">Connection String</label>
          <input type="text" id="${sourceId}-connection-string" name="${sourceId}-connection-string" 
            value="${sourceData?.connection_string || ''}">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label for="${sourceId}-query">Query</label>
          <textarea id="${sourceId}-query" name="${sourceId}-query" rows="3">${sourceData?.query || ''}</textarea>
        </div>
      </div>
    </div>
  `;
  
  sourceContainer.appendChild(sourceItem);
  
  // Add event listeners for the source type toggles
  const typeSelect = document.getElementById(`${sourceId}-type`);
  typeSelect.addEventListener("change", function() {
    const fileOptions = sourceItem.querySelector(".file-options");
    const odbcOptions = sourceItem.querySelector(".odbc-options");
    
    if (this.value === "File") {
      fileOptions.classList.remove("hidden");
      odbcOptions.classList.add("hidden");
    } else {
      fileOptions.classList.add("hidden");
      odbcOptions.classList.remove("hidden");
    }
  });
  
  // Add event listener for the remove button
  sourceItem.querySelector(".remove-source").addEventListener("click", function() {
    sourceContainer.removeChild(sourceItem);
  });
  
  return sourceId;
}

// Add a destination to the form
function addDestination(destinationData = null) {
  const destinationContainer = document.getElementById("destination-container");
  const destinationId = "destination";
  
  const destinationItem = document.createElement("div");
  destinationItem.className = "destination-item";
  destinationItem.dataset.id = destinationId;
  
  destinationItem.innerHTML = `
    <div class="form-row">
      <div class="form-group">
        <label for="${destinationId}-type">Type</label>
        <select id="${destinationId}-type" name="${destinationId}-type" required>
          <option value="File" ${destinationData && destinationData.type === "File" ? "selected" : ""}>File</option>
          <option value="Odbc" ${destinationData && destinationData.type === "Odbc" ? "selected" : ""}>ODBC</option>
          <option value="Delta" ${destinationData && destinationData.type === "Delta" ? "selected" : ""}>Delta</option>
        </select>
      </div>
      <div class="form-group">
        <label for="${destinationId}-name">Name</label>
        <input type="text" id="${destinationId}-name" name="${destinationId}-name" required 
          value="${destinationData?.name || ''}">
      </div>
    </div>
    
    <div class="file-options ${destinationData && destinationData.type !== "File" ? "hidden" : ""}">
      <div class="form-row">
        <div class="form-group">
          <label for="${destinationId}-file-type">File Type</label>
          <select id="${destinationId}-file-type" name="${destinationId}-file-type">
            <option value="Csv" ${destinationData?.file_type?.type === "Csv" ? "selected" : ""}>CSV</option>
            <option value="Parquet" ${destinationData?.file_type?.type === "Parquet" ? "selected" : ""}>Parquet</option>
            <option value="Json" ${destinationData?.file_type?.type === "Json" ? "selected" : ""}>JSON</option>
          </select>
        </div>
        <div class="form-group">
          <label for="${destinationId}-location">Location</label>
          <input type="text" id="${destinationId}-location" name="${destinationId}-location" 
            value="${destinationData?.location || ''}">
        </div>
      </div>
    </div>
    
    <div class="odbc-options ${destinationData && destinationData.type === "Odbc" ? "" : "hidden"}">
      <div class="form-row">
        <div class="form-group">
          <label for="${destinationId}-connection-string">Connection String</label>
          <input type="text" id="${destinationId}-connection-string" name="${destinationId}-connection-string" 
            value="${destinationData?.connection_string || ''}">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label for="${destinationId}-write-mode">Write Mode</label>
          <select id="${destinationId}-write-mode" name="${destinationId}-write-mode">
            <option value="Append" ${destinationData?.write_mode?.operation === "Append" ? "selected" : ""}>Append</option>
            <option value="Replace" ${destinationData?.write_mode?.operation === "Replace" ? "selected" : ""}>Replace</option>
            <option value="Upsert" ${destinationData?.write_mode?.operation === "Upsert" ? "selected" : ""}>Upsert</option>
          </select>
        </div>
        <div class="form-group">
          <label for="${destinationId}-batch-size">Batch Size</label>
          <input type="number" id="${destinationId}-batch-size" name="${destinationId}-batch-size" 
            value="${destinationData?.batch_size || '100'}">
        </div>
      </div>
    </div>
    
    <div class="delta-options ${destinationData && destinationData.type === "Delta" ? "" : "hidden"}">
      <div class="form-row">
        <div class="form-group">
          <label for="${destinationId}-delta-location">Delta Location</label>
          <input type="text" id="${destinationId}-delta-location" name="${destinationId}-delta-location" 
            value="${destinationData?.location || ''}">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label for="${destinationId}-delta-write-mode">Write Mode</label>
          <select id="${destinationId}-delta-write-mode" name="${destinationId}-delta-write-mode">
            <option value="Append" ${destinationData?.write_mode?.operation === "Append" ? "selected" : ""}>Append</option>
            <option value="Replace" ${destinationData?.write_mode?.operation === "Replace" ? "selected" : ""}>Replace</option>
            <option value="Upsert" ${destinationData?.write_mode?.operation === "Upsert" ? "selected" : ""}>Upsert</option>
          </select>
        </div>
      </div>
    </div>
  `;
  
  destinationContainer.appendChild(destinationItem);
  
  // Add event listeners for the destination type toggles
  const typeSelect = document.getElementById(`${destinationId}-type`);
  typeSelect.addEventListener("change", function() {
    const fileOptions = destinationItem.querySelector(".file-options");
    const odbcOptions = destinationItem.querySelector(".odbc-options");
    const deltaOptions = destinationItem.querySelector(".delta-options");
    
    if (this.value === "File") {
      fileOptions.classList.remove("hidden");
      odbcOptions.classList.add("hidden");
      deltaOptions.classList.add("hidden");
    } else if (this.value === "Odbc") {
      fileOptions.classList.add("hidden");
      odbcOptions.classList.remove("hidden");
      deltaOptions.classList.add("hidden");
    } else if (this.value === "Delta") {
      fileOptions.classList.add("hidden");
      odbcOptions.classList.add("hidden");
      deltaOptions.classList.remove("hidden");
    }
  });
  
  return destinationId;
}

// Add a new stage row (parallel stages)
function addStageRow(stageRowData = null) {
  const stagesContainer = document.getElementById("stages-container");
  const rowId = `row-${Date.now()}`;
  
  const rowElement = document.createElement("div");
  rowElement.className = "stage-row";
  rowElement.dataset.id = rowId;
  
  // Add a button to remove the entire row
  const rowControls = document.createElement("div");
  rowControls.className = "row-controls";
  rowControls.innerHTML = `
    <button type="button" class="btn-remove remove-row">Remove Row</button>
  `;
  
  rowElement.appendChild(rowControls);
  stagesContainer.appendChild(rowElement);
  
  // Add the first stage in this row
  addStageToRow(rowId);
  
  // If we have data, add all the stages
  if (stageRowData && Array.isArray(stageRowData)) {
    // Remove the default stage we just added
    const defaultStage = rowElement.querySelector(".stage-item");
    if (defaultStage) {
      rowElement.removeChild(defaultStage);
    }
    
    // Add each stage from the data
    stageRowData.forEach(stageData => {
      addStageToRow(rowId, stageData);
    });
  }
  
  // Add event listener for removing the row
  rowElement.querySelector(".remove-row").addEventListener("click", function() {
    // Clean up any editors in this row
    const stageElements = rowElement.querySelectorAll(".stage-item");
    stageElements.forEach(stageEl => {
      const stageId = stageEl.dataset.id;
      if (editors.stages[stageId]) {
        editors.stages[stageId].toTextArea(); // Cleanup CodeMirror
        delete editors.stages[stageId];
      }
    });
    
    stagesContainer.removeChild(rowElement);
  });
  
  return rowId;
}

// Add a stage to a specific row
function addStageToRow(rowId, stageData = null) {
  const rowElement = document.querySelector(`.stage-row[data-id="${rowId}"]`);
  const stageId = `stage-${Date.now()}`;
  
  const stageElement = document.createElement("div");
  stageElement.className = "stage-item";
  stageElement.dataset.id = stageId;
  
  stageElement.innerHTML = `
    <div class="stage-header">
      <div class="form-group">
        <label for="${stageId}-name">Stage Name</label>
        <input type="text" id="${stageId}-name" name="${stageId}-name" required 
          value="${stageData?.name || ''}">
      </div>
      <div class="stage-controls">
        <button type="button" class="btn-add add-stage-to-row">Add Stage</button>
        <button type="button" class="btn-remove remove-stage">Remove</button>
      </div>
    </div>
    <div class="form-group">
      <label for="${stageId}-query">SQL Query</label>
      <div class="editor-container" id="${stageId}-editor-container">
        <textarea id="${stageId}-query" name="${stageId}-query">${stageData?.query || DEFAULT_SQL}</textarea>
      </div>
    </div>
    <div class="form-row">
      <div class="form-group">
        <label for="${stageId}-explain">Explain</label>
        <input type="checkbox" id="${stageId}-explain" name="${stageId}-explain" 
          ${stageData?.explain ? "checked" : ""}>
      </div>
      <div class="form-group">
        <label for="${stageId}-show">Show Rows (0 for all)</label>
        <input type="number" id="${stageId}-show" name="${stageId}-show" 
          value="${stageData?.show || '0'}">
      </div>
    </div>
  `;
  
  rowElement.appendChild(stageElement);
  
  // Initialize CodeMirror for this stage
  initializeCodeMirror(stageId);
  
  // Add event listeners
  stageElement.querySelector(".add-stage-to-row").addEventListener("click", function() {
    addStageToRow(rowId);
  });
  
  stageElement.querySelector(".remove-stage").addEventListener("click", function() {
    // Clean up the editor
    if (editors.stages[stageId]) {
      editors.stages[stageId].toTextArea();
      delete editors.stages[stageId];
    }
    
    rowElement.removeChild(stageElement);
    
    // If this was the last stage in the row, remove the row
    if (rowElement.querySelectorAll(".stage-item").length === 0) {
      document.getElementById("stages-container").removeChild(rowElement);
    }
  });
  
  return stageId;
}

// Initialize CodeMirror for a specific stage
function initializeCodeMirror(stageId) {
  const textArea = document.getElementById(`${stageId}-query`);
  
  // Initialize CodeMirror
  const editor = CodeMirror.fromTextArea(textArea, {
    mode: "text/x-sql",
    theme: "dracula",
    lineNumbers: true,
    indentUnit: 2,
    tabSize: 2,
    indentWithTabs: false,
    lineWrapping: true,
    matchBrackets: true,
    autoCloseBrackets: true,
    viewportMargin: Infinity,
    extraKeys: {
      "Tab": function(cm) {
        if (cm.somethingSelected()) {
          cm.indentSelection("add");
        } else {
          cm.replaceSelection("  ", "end");
        }
      }
    }
  });
  
  // Store the editor instance
  editors.stages[stageId] = editor;
  
  // Set height via CSS because the editor is mounted
  editor.display.wrapper.style.height = "100%";
  
  return editor;
}

// Add initial elements to the form
function addInitialSource() {
  addSource();
}

function addInitialStageRow() {
  addStageRow();
}

// Setup event listeners for the form buttons
function setupEventListeners() {
  // Add source button
  document.getElementById("add-source").addEventListener("click", () => {
    addSource();
  });
  
  // Add stage row button
  document.getElementById("add-stage-row").addEventListener("click", () => {
    addStageRow();
  });
  
  // Preview button
  document.getElementById("preview-pipeline").addEventListener("click", () => {
    const pipeline = buildPipelineFromForm();
    showPipelinePreview(pipeline);
  });
  
  // Save button
  document.getElementById("pipeline-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const pipeline = buildPipelineFromForm();
    document.getElementById("pipeline-json").value = JSON.stringify(pipeline);
    console.log("Pipeline to save:", pipeline);
    alert("Pipeline saved (demo only)");
    // In a real implementation, this would submit the form
  });
  
  // Run button
  document.getElementById("run-pipeline").addEventListener("click", () => {
    const pipeline = buildPipelineFromForm();
    console.log("Pipeline to run:", pipeline);
    alert("Pipeline execution started (demo only)");
  });
  
  // Modal close button
  document.getElementById("close-modal").addEventListener("click", () => {
    document.getElementById("preview-modal").style.display = "none";
  });
}

// Build a pipeline object from the form data
function buildPipelineFromForm() {
  const pipeline = {
    sources: [],
    stages: [],
    destination: {}
  };
  
  // Build sources
  const sourceItems = document.querySelectorAll(".source-item");
  sourceItems.forEach(sourceItem => {
    const sourceId = sourceItem.dataset.id;
    const sourceType = document.getElementById(`${sourceId}-type`).value;
    
    const source = {
      type: sourceType,
      name: document.getElementById(`${sourceId}-name`).value
    };
    
    if (sourceType === "File") {
      source.file_type = {
        type: document.getElementById(`${sourceId}-file-type`).value,
        options: {}
      };
      source.location = document.getElementById(`${sourceId}-location`).value;
    } else if (sourceType === "Odbc") {
      source.connection_string = document.getElementById(`${sourceId}-connection-string`).value;
      source.query = document.getElementById(`${sourceId}-query`).value;
    }
    
    pipeline.sources.push(source);
  });
  
  // Build stages
  const stageRows = document.querySelectorAll(".stage-row");
  stageRows.forEach(row => {
    const rowStages = [];
    
    const stageItems = row.querySelectorAll(".stage-item");
    stageItems.forEach(stageItem => {
      const stageId = stageItem.dataset.id;
      const stageName = document.getElementById(`${stageId}-name`).value;
      
      // Get the query from CodeMirror
      const stageQuery = editors.stages[stageId].getValue();
      
      const stage = {
        name: stageName,
        query: stageQuery
      };
      
      // Optional fields
      const explainChecked = document.getElementById(`${stageId}-explain`).checked;
      if (explainChecked) {
        stage.explain = true;
      }
      
      const showRows = parseInt(document.getElementById(`${stageId}-show`).value || "0", 10);
      if (showRows > 0) {
        stage.show = showRows;
      }
      
      rowStages.push(stage);
    });
    
    if (rowStages.length > 0) {
      pipeline.stages.push(rowStages);
    }
  });
  
  // Build destination
  const destinationId = "destination";
  const destinationType = document.getElementById(`${destinationId}-type`).value;
  
  const destination = {
    type: destinationType,
    name: document.getElementById(`${destinationId}-name`).value
  };
  
  if (destinationType === "File") {
    destination.file_type = {
      type: document.getElementById(`${destinationId}-file-type`).value,
      options: {}
    };
    destination.location = document.getElementById(`${destinationId}-location`).value;
  } else if (destinationType === "Odbc") {
    destination.connection_string = document.getElementById(`${destinationId}-connection-string`).value;
    destination.write_mode = {
      operation: document.getElementById(`${destinationId}-write-mode`).value
    };
    destination.batch_size = parseInt(document.getElementById(`${destinationId}-batch-size`).value || "100", 10);
  } else if (destinationType === "Delta") {
    destination.location = document.getElementById(`${destinationId}-delta-location`).value;
    destination.write_mode = {
      operation: document.getElementById(`${destinationId}-delta-write-mode`).value
    };
  }
  
  pipeline.destination = destination;
  
  return pipeline;
}

// Show a preview of the pipeline in YAML format
function showPipelinePreview(pipeline) {
  const modal = document.getElementById("preview-modal");
  const previewContent = document.getElementById("preview-content");
  
  // Display the pipeline as YAML-like string
  const yamlString = generateYamlString(pipeline);
  previewContent.textContent = yamlString;
  
  // Show the modal
  modal.style.display = "block";
}

// Generate a YAML-like string from a pipeline object
function generateYamlString(pipeline) {
  const yaml = [];
  
  // Sources
  yaml.push("sources:");
  pipeline.sources.forEach(source => {
    yaml.push(`  - type: ${source.type}`);
    yaml.push(`    name: ${source.name}`);
    
    if (source.type === "File") {
      yaml.push(`    file_type:`);
      yaml.push(`      type: ${source.file_type.type}`);
      yaml.push(`      options: {}`);
      yaml.push(`    location: ${source.location}`);
    } else if (source.type === "Odbc") {
      yaml.push(`    connection_string: ${source.connection_string}`);
      yaml.push(`    query: ${source.query}`);
    }
  });
  
  yaml.push("");
  
  // Stages
  yaml.push("stages:");
  pipeline.stages.forEach(stageRow => {
    yaml.push("  -");
    stageRow.forEach(stage => {
      yaml.push(`    - name: ${stage.name}`);
      yaml.push(`      query: >\n        ${stage.query.replace(/\n/g, "\n        ")}`);
      
      if (stage.explain) {
        yaml.push(`      explain: true`);
      }
      
      if (stage.show) {
        yaml.push(`      show: ${stage.show}`);
      }
    });
  });
  
  yaml.push("");
  
  // Destination
  yaml.push("destination:");
  yaml.push(`  type: ${pipeline.destination.type}`);
  yaml.push(`  name: ${pipeline.destination.name}`);
  
  if (pipeline.destination.type === "File") {
    yaml.push(`  file_type:`);
    yaml.push(`    type: ${pipeline.destination.file_type.type}`);
    yaml.push(`    options: {}`);
    yaml.push(`  location: ${pipeline.destination.location}`);
  } else if (pipeline.destination.type === "Odbc") {
    yaml.push(`  connection_string: ${pipeline.destination.connection_string}`);
    yaml.push(`  write_mode:`);
    yaml.push(`    operation: ${pipeline.destination.write_mode.operation}`);
    yaml.push(`  batch_size: ${pipeline.destination.batch_size}`);
  } else if (pipeline.destination.type === "Delta") {
    yaml.push(`  location: ${pipeline.destination.location}`);
    yaml.push(`  write_mode:`);
    yaml.push(`    operation: ${pipeline.destination.write_mode.operation}`);
  }
  
  return yaml.join("\n");
}

// Load a pipeline into the form
function loadPipeline(pipeline) {
  // Clear existing form elements
  document.getElementById("sources-container").innerHTML = "";
  document.getElementById("stages-container").innerHTML = "";
  document.getElementById("destination-container").innerHTML = "";
  
  // Load sources
  if (pipeline.sources && Array.isArray(pipeline.sources)) {
    pipeline.sources.forEach(source => {
      addSource(source);
    });
  } else {
    addInitialSource();
  }
  
  // Load stages
  if (pipeline.stages && Array.isArray(pipeline.stages)) {
    pipeline.stages.forEach(stageRow => {
      addStageRow(stageRow);
    });
  } else {
    addInitialStageRow();
  }
  
  // Load destination
  if (pipeline.destination && Object.keys(pipeline.destination).length > 0) {
    addDestination(pipeline.destination);
  } else {
    addDestination();
  }
}