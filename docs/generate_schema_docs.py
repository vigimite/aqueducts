#!/usr/bin/env python3
"""
MkDocs hook to generate schema documentation.
This is a simpler alternative to a full plugin.
"""

import json
import subprocess
from pathlib import Path
import jinja2


def on_startup(command, dirty):
    """Generate schema documentation when MkDocs starts."""
    print("🔧 Generating JSON schema...")
    
    # Get paths
    docs_dir = Path(__file__).parent
    project_root = docs_dir.parent
    
    # Generate the schema using the Rust binary
    try:
        result = subprocess.run(
            ["cargo", "run", "--bin", "generate-schema"],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Schema generation output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to generate schema: {e}")
        print(f"Error output: {e.stderr}")
        return
    
    # Load the generated schema
    schema_path = project_root / "json_schema" / "aqueducts.schema.json"
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        return
        
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    # Generate the documentation
    generate_docs(schema, docs_dir)


def generate_docs(schema, docs_dir):
    """Generate the schema documentation from the JSON schema."""
    print("📚 Generating schema documentation...")
    
    # Set up Jinja2 environment - use in-memory template
    template_content = get_template_content()
    
    jinja_env = jinja2.Environment(
        loader=jinja2.DictLoader({'schema_main.md.j2': template_content}),
        trim_blocks=True,
        lstrip_blocks=True
    )
    
    template = jinja_env.get_template('schema_main.md.j2')
    
    # Process the schema for easier template rendering
    processed_schema = process_schema(schema)
    
    # Render the documentation
    content = template.render(schema=processed_schema)
    
    # Write the output file
    output_path = docs_dir / "content" / "schema_reference.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write(content)
    
    print(f"✅ Schema documentation generated: {output_path}")


def process_schema(schema):
    """Process the raw JSON schema for easier template rendering."""
    processed = {
        'title': schema.get('title', 'Schema Reference'),
        'description': clean_description(schema.get('description', '')),
        'type': schema.get('type', 'object'),
        'properties': [],
        'definitions': [],
        'toc': []
    }
    
    # Process properties
    if 'properties' in schema:
        for prop_name, prop_def in schema['properties'].items():
            processed_prop = process_property(prop_name, prop_def, schema.get('required', []))
            processed['properties'].append(processed_prop)
            processed['toc'].append({'name': prop_name, 'type': 'property'})
    
    # Process definitions/components
    # Filter out DataType-specific internal types that users don't need to see
    datatype_internal_types = {'TimeUnit', 'IntervalUnit', 'UnionMode'}
    
    definitions = schema.get('definitions', schema.get('$defs', {}))
    for def_name, def_schema in definitions.items():
        # Skip internal DataType-specific types
        if def_name in datatype_internal_types:
            continue
            
        processed_def = process_definition(def_name, def_schema)
        processed['definitions'].append(processed_def)
        processed['toc'].append({'name': def_name, 'type': 'definition'})
    
    return processed


def process_property(prop_name, prop_def, required_props):
    """Process a single property definition."""
    return {
        'name': prop_name,
        'type': get_type_display(prop_def),
        'description': clean_description(prop_def.get('description', '')),
        'required': prop_name in required_props,
        'examples': prop_def.get('examples', []),
        'enum': prop_def.get('enum', []),
        'default': prop_def.get('default'),
        'format': prop_def.get('format'),
        'items': prop_def.get('items', {}),
        'oneOf': process_one_of(prop_def.get('oneOf', [])),
        'anyOf': process_any_of(prop_def.get('anyOf', [])),
        'allOf': process_all_of(prop_def.get('allOf', [])),
        'ref': extract_ref(prop_def.get('$ref', ''))
    }


def process_definition(def_name, def_schema):
    """Process a definition schema."""
    # Use special handling for DataType
    description = def_schema.get('description', '')
    if def_name == 'DataType':
        cleaned_description = clean_datatype_description(description)
    else:
        cleaned_description = clean_description(description)
    
    processed = {
        'name': def_name,
        'description': cleaned_description,
        'type': def_schema.get('type', ''),
        'properties': [],
        'oneOf': process_one_of(def_schema.get('oneOf', [])),
        'anyOf': process_any_of(def_schema.get('anyOf', [])),
        'allOf': process_all_of(def_schema.get('allOf', [])),
        'enum': def_schema.get('enum', [])
    }
    
    # Process properties if they exist
    if 'properties' in def_schema:
        required_props = def_schema.get('required', [])
        for prop_name, prop_def in def_schema['properties'].items():
            processed['properties'].append(process_property(prop_name, prop_def, required_props))
    
    return processed


def process_one_of(one_of_list):
    """Process oneOf constraint."""
    return [process_variant(variant) for variant in one_of_list]


def process_any_of(any_of_list):
    """Process anyOf constraint."""
    return [process_variant(variant) for variant in any_of_list]


def process_all_of(all_of_list):
    """Process allOf constraint."""
    return [process_variant(variant) for variant in all_of_list]


def process_variant(variant):
    """Process a variant in oneOf/anyOf/allOf."""
    processed = {
        'description': clean_description(variant.get('description', '')),
        'type': variant.get('type', ''),
        'properties': [],
        'enum': variant.get('enum', []),
        'ref': extract_ref(variant.get('$ref', ''))
    }
    
    if 'properties' in variant:
        required_props = variant.get('required', [])
        for prop_name, prop_def in variant['properties'].items():
            processed['properties'].append(process_property(prop_name, prop_def, required_props))
    
    return processed


def extract_ref(ref_string):
    """Extract the reference name from a $ref string."""
    if ref_string:
        return ref_string.split('/')[-1]
    return ''


def generate_tab_name(variant, index):
    """Generate a descriptive tab name for a oneOf/anyOf variant."""
    # Use description if available and not too long
    if variant.get('description') and len(variant['description']) < 80:
        return variant['description']
    
    # For object types with a single property, use the property name
    if (variant.get('type') == 'object' and 
        variant.get('properties') and 
        len(variant['properties']) == 1):
        prop_name = list(variant['properties'].keys())[0]
        return f"{prop_name} Type"
    
    # For enums, use "Basic Types" or "Allowed Values" 
    if variant.get('enum'):
        return "Basic Types"
    
    # Fallback to generic option name
    return f"Option {index}"


def clean_description(description):
    """Clean description by removing problematic markdown and code examples."""
    if not description:
        return ''
    
    # Split into lines for processing
    lines = description.split('\n')
    cleaned_lines = []
    in_code_block = False
    skip_examples = False
    
    for line in lines:
        stripped = line.strip()
        
        # Skip code blocks entirely
        if stripped.startswith('```'):
            in_code_block = not in_code_block
            continue
        
        if in_code_block:
            continue
            
        # Skip example sections that contain Rust code or complex markdown
        if (stripped.startswith('# Examples') or 
            stripped.startswith('# String Format Examples') or
            stripped.startswith('# YAML Configuration Example')):
            skip_examples = True
            continue
        elif skip_examples and stripped.startswith('#'):
            skip_examples = False
        elif skip_examples:
            continue
        
        # Skip markdown headings that would conflict
        if stripped.startswith('#'):
            continue
            
        # Clean the line and add if it has content
        if stripped:
            # Remove problematic characters and clean up
            cleaned_line = stripped.replace('`', '"')  # Convert backticks to quotes
            cleaned_lines.append(cleaned_line)
    
    # Join with spaces and clean up
    result = ' '.join(cleaned_lines)
    result = ' '.join(result.split())  # Normalize whitespace
    
    return result


def clean_datatype_description(description):
    """Special handling for DataType description to preserve important usage examples."""
    if not description:
        return ''
    
    # Extract the main description (everything before examples)
    main_desc_end = description.find('# String Format Examples')
    if main_desc_end == -1:
        # Fallback to regular cleaning if no examples section found
        return clean_description(description)
    
    main_description = description[:main_desc_end].strip()
    examples_section = description[main_desc_end:].strip()
    
    # Clean the main description
    main_cleaned = clean_description(main_description)
    
    # Process examples section into Material admonition format
    result_parts = [main_cleaned, '']
    result_parts.append('!!! info "Data Type Format Examples"')
    result_parts.append('')
    
    # Parse and format the examples section
    # The examples section is actually all on one line with \n characters
    # Let's extract each section manually
    examples_text = examples_section
    
    # Extract Basic Types section
    basic_start = examples_text.find('## Basic Types')
    complex_start = examples_text.find('## Complex Types')
    yaml_start = examples_text.find('# YAML Configuration Example')
    
    if basic_start != -1:
        result_parts.append('    **Basic Types:**')
        result_parts.append('')
        
        # Extract the basic type examples
        basic_types = [
            '- "string" or "utf8" - UTF-8 string',
            '- "int32", "int", or "integer" - 32-bit signed integer',
            '- "int64" or "long" - 64-bit signed integer', 
            '- "float32" or "float" - 32-bit floating point',
            '- "float64" or "double" - 64-bit floating point',
            '- "bool" or "boolean" - Boolean value',
            '- "date32" or "date" - Date as days since epoch'
        ]
        for bt in basic_types:
            result_parts.append(f'    {bt}')
        result_parts.append('')
    
    if complex_start != -1:
        result_parts.append('    **Complex Types:**')
        result_parts.append('')
        complex_types = [
            '- "list<string>" - List of strings',
            '- "struct<name:string,age:int32>" - Struct with name and age fields',
            '- "decimal<10,2>" - Decimal with precision 10, scale 2',
            '- "timestamp<millisecond,UTC>" - Timestamp with time unit and timezone',
            '- "map<string,int32>" - Map from string keys to int32 values'
        ]
        for ct in complex_types:
            result_parts.append(f'    {ct}')
        result_parts.append('')
    
    if yaml_start != -1:
        result_parts.append('    **YAML Configuration Example:**')
        result_parts.append('')
        result_parts.append('    ```yaml')
        result_parts.append('    schema:')
        result_parts.append('      - name: user_id')
        result_parts.append('        data_type: int64')
        result_parts.append('        nullable: false')
        result_parts.append('      - name: email')
        result_parts.append('        data_type: string')
        result_parts.append('        nullable: true')
        result_parts.append('      - name: scores')
        result_parts.append('        data_type: "list<float64>"')
        result_parts.append('        nullable: true')
        result_parts.append('      - name: profile')
        result_parts.append('        data_type: "struct<name:string,age:int32>"')
        result_parts.append('        nullable: true')
        result_parts.append('    ```')
    
    return '\n'.join(result_parts)


def get_type_display(prop_def):
    """Get a human-readable type display for a property."""
    if 'type' in prop_def:
        type_val = prop_def['type']
        if isinstance(type_val, list):
            return ' | '.join(type_val)
        return type_val
    elif 'oneOf' in prop_def:
        return 'oneOf'
    elif 'anyOf' in prop_def:
        return 'anyOf'
    elif 'allOf' in prop_def and len(prop_def['allOf']) == 1 and '$ref' in prop_def['allOf'][0]:
        # Handle simple allOf references
        return prop_def['allOf'][0]['$ref'].split('/')[-1]
    elif 'allOf' in prop_def:
        return 'allOf'
    elif '$ref' in prop_def:
        return prop_def['$ref'].split('/')[-1]
    else:
        return 'unknown'


def get_template_content():
    """Get the template content."""
    template_content = """# {{ schema.title }}

{{ schema.description }}

## Root Properties

{% for prop in schema.properties %}
### {{ prop.name }}

!!! info "Property Details"
    **Type:** `{{ prop.type }}`  
    **Required:** {{ "Yes" if prop.required else "No" }}
    {% if prop.default is defined and prop.default != None %}
    **Default:** `{{ prop.default }}`
    {% endif %}

{{ prop.description }}

{% if prop.ref %}
!!! note "Reference"
    See [`{{ prop.ref }}`](#{{ prop.ref | lower | replace('_', '-') }}) for details.
{% endif %}

{% if prop.anyOf %}
{% for variant in prop.anyOf %}
=== "{{ variant.description or 'Option ' + loop.index|string }}"

    {% if variant.ref %}
    **Type:** [`{{ variant.ref }}`](#{{ variant.ref | lower | replace('_', '-') }})
    {% elif variant.type %}
    **Type:** `{{ variant.type }}`
    {% endif %}

{% endfor %}
{% endif %}

{% endfor %}

## Type Definitions

{% for definition in schema.definitions %}
### {{ definition.name }}

{{ definition.description }}

{% if definition.enum %}
!!! tip "Allowed Values"
    {% for value in definition.enum %}
    - `{{ value }}`
    {% endfor %}
{% endif %}

{% if definition.oneOf %}
{% for variant in definition.oneOf %}
{% if variant.enum %}
=== "Basic Types"

    **Type:** `{{ variant.type }}`

    **Allowed Values:**
    {% for value in variant.enum %}
    - `{{ value }}`
    {% endfor %}

{% else %}
=== "{{ variant.description or 'Option ' + loop.index|string }}"

    {% if variant.type %}
    **Type:** `{{ variant.type }}`
    {% endif %}

    {% if variant.properties %}
    **Properties:**

    | Property | Type | Required | Description | Default |
    |----------|------|----------|-------------|---------|
    {% for prop in variant.properties %}
    | `{{ prop.name }}` | `{{ prop.type }}` | {{ "✓" if prop.required else "✗" }} | {{ prop.description }} | {% if prop.default is defined and prop.default != None %}`{{ prop.default }}`{% else %}-{% endif %} |
    {% endfor %}

    {% if variant.properties and variant.properties|selectattr("enum")|list %}
    **Enum Values:**
    {% for prop in variant.properties %}
    {% if prop.enum %}
    - **{{ prop.name }}:** {% for val in prop.enum %}`{{ val }}`{% if not loop.last %}, {% endif %}{% endfor %}
    {% endif %}
    {% endfor %}
    {% endif %}
    {% endif %}

{% endif %}
{% endfor %}
{% endif %}

{% if definition.properties %}
**Properties:**

| Property | Type | Required | Description | Default |
|----------|------|----------|-------------|---------|
{% for prop in definition.properties %}
| `{{ prop.name }}` | `{{ prop.type }}` | {{ "✓" if prop.required else "✗" }} | {{ prop.description }} | {% if prop.default is defined and prop.default != None %}`{{ prop.default }}`{% else %}-{% endif %} |
{% endfor %}

{% if definition.properties and definition.properties|selectattr("enum")|list %}
!!! note "Enum Values"
    {% for prop in definition.properties %}
    {% if prop.enum %}
    **{{ prop.name }}:** {% for val in prop.enum %}`{{ val }}`{% if not loop.last %}, {% endif %}{% endfor %}
    {% endif %}
    {% endfor %}
{% endif %}

{% if definition.properties and definition.properties|selectattr("ref")|list %}
!!! info "Type References"
    {% for prop in definition.properties %}
    {% if prop.ref %}
    **{{ prop.name }}:** [`{{ prop.ref }}`](#{{ prop.ref | lower | replace('_', '-') }})
    {% endif %}
    {% endfor %}
{% endif %}
{% endif %}

{% endfor %}
"""
    
    return template_content


if __name__ == "__main__":
    # Allow running as standalone script
    on_startup("build", False)
