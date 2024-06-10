import os
import glob
import shutil
from json_schema_for_humans.generate import generate_from_filename
from json_schema_for_humans.generation_configuration import GenerationConfiguration


OUTPUT_DIR = "json_schema"


def find_latest_generated_json(target_dir='target', pattern='aqueducts.schema.json'):
    # Search for the JSON file in the target directory
    search_pattern = os.path.join(target_dir, 'debug', 'build', '**', pattern)
    files = glob.glob(search_pattern, recursive=True)

    if not files:
        raise FileNotFoundError(f"No files found matching pattern: {search_pattern}")

    # Find the most recently modified file
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def on_startup(command, dirty):
    try:
        json_path = find_latest_generated_json()
        print(f"Found latest JSON schema at: {json_path}")

        # output file to destination directory
        output_path = shutil.copy(json_path, OUTPUT_DIR)

        gen_cfg = GenerationConfiguration(
                    custom_template_path="json_schema/schema_reference_template/base.md",
                    footer_show_time=False,
                    description_is_markdown=True,
                    link_to_reused_ref=False,
                    show_breadcrumbs=False,
                    show_toc=False,
                    template_md_options={
                        "badge_as_image": True,
                        "show_heading_numbers": False,
                        "show_array_restrictions": False,
                        "properties_table_columns": [
                            "Property",
                            "Pattern",
                            "Type",
                            "Title/Description"
                        ]
                    }
                 )

        generate_from_filename(output_path, "docs/schema_reference.md", config=gen_cfg)

    except Exception as e:
        print(f"An error occurred: {e}")
