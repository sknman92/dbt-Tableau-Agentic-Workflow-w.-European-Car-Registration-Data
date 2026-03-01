# PDF scrape and verification

## Goal

Parse data from PDFs saved in a user-specified directory into intermediate csvs. Ask user to verify content.

## Process

1. **Fetch all PDFs in directory**
   - There is no execution .py file for this. Come up with the best way to grab data from the PDF and store the script in `execution` folder.
   - Folder schema provided in `schema` folder for each directory. For example,
     if you are pulling data from PDFs listed under the `PDFs/ACEA` folder, then use `schema/ACEA.csv`
   - Make sure you are grabbing the region description correctly
   - Exclude 'Others' from manufacturer field as it causes duplication issues
   - Output intermediate results into the `data` folder

2. **Verification**
   - Agent (you) read the csvs
   - Wait for verification from user

3. **Uploading to Snowflake**
   - Once verified, upload to Snowflake using `execution/upload_snowflake.py`
   - All uploaded data files should be deleted

4. **Building dbt models**
   - Use dbt mcp or codegen for all tasks associated with dbt
   - Build all models in `dbt/`
   - Use project venv dbt Core CLI (not `dbt-fusion`) when running Python models, since Fusion CLI may skip Python model execution in this project
   - From repo root, run dbt with `./.venv/Scripts/dbt.exe` (Windows)
   - Example commands:
     - `cd dbt ; ../.venv/Scripts/dbt.exe build -s int_acea_metrics`
     - `cd dbt ; ../.venv/Scripts/dbt.exe build -s stg_acea_data --full-refresh` (when full rebuild requested)
   - Whenever a new model is created, add to appropriate .yml file (ex: `_int.yml`)

5. **Exporting model to Tableau**
   - Export models in `marts/` to Tableau using `execution/publish_tableau_datasource.py`

6. **Data triage**
   - Use Tableau MCP Vizql Data Service (VDS) tool to triage published Tableau data sources
    - **6a. Query + export dataset**
       - Run Tableau MCP query (`mcp_tableau_query-datasource`) for the user’s triage question
       - Export and save the returned rows as csv in the run folder
    - **6b. Plot artifacts**
       - Use the exported csv to generate plots using `skills/data-viz-plots`
       - Save generated plots in the same run folder
   - Always save triage artifacts under `analyses/` (never in temp directories)
   - At minimum, save:
     - query result dataset (csv)
     - generated plots (png)
     - short run metadata (txt or json) with datasource, filters, and timestamp

7. **Triage run output structure (required)**
   - Each triage request must be saved into its own folder under `analyses/`
   - Folder naming convention:
     - `analyses/<YYYY-MM-DD>/<title>/`
   - Date must not include timestamp
   - Title must be the agent's best short summary of the request
   - Do not overwrite previous runs; if the folder already exists, create a new one with a numeric suffix (example: `<title>_2`)
   - Keep outputs for each run together (csv, png, metadata) in that run folder
