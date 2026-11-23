# Netflix Data Engineering Project on Azure

This is an end-to-end data engineering project I built on Microsoft Azure using the Netflix titles dataset. It takes raw CSV files, moves them through a Bronze → Silver → Gold pipeline, cleans and shapes the data along the way, and ends with tables that are ready for reporting.

I built it by following Ansh Lamba's tutorial, [*The ONLY Azure End-To-End Data Engineering Project You NEED in 2025*](https://www.youtube.com/watch?v=uc-u_juRg-w). The original code is in [his repository](https://github.com/anshlambagit/Netflix_Azure_Data_Engineering_Project). Full credit to him for the project design.

---

## What the project does, in plain words

Netflix data arrives as a handful of CSV files: one big file of titles, and four smaller "lookup" files for directors, cast, countries and categories. The goal is to land those files in the cloud, keep loading new data as it shows up, clean it, and publish trustworthy tables at the end.

The data moves through three layers:

| Layer | What lives there | Why it matters |
|---|---|---|
| **Bronze** | The raw data, exactly as it arrived | A safe copy you can always go back to |
| **Silver** | Cleaned and transformed data, stored as Delta tables | Fixed types, filled gaps and new helper columns |
| **Gold** | Final tables with data quality checks applied | What analysts and dashboards should use |

---

## Tools used

- **Azure Data Lake Storage Gen2** stores every layer, in containers named `raw`, `bronze`, `silver` and `gold`.
- **Azure Data Factory** copies the lookup files into the lake with a parameterised, looping pipeline.
- **Azure Databricks** with **Unity Catalog** handles all the processing and governance.
- **PySpark and Spark SQL** do the transformations.
- **Auto Loader** picks up new files incrementally, so nothing gets loaded twice.
- **Databricks Workflows** run the notebooks in order, with loops and if/else logic.
- **Delta Live Tables (DLT)** build the Gold layer with built-in data quality rules.

---

## How the pipeline flows

```
              ┌──────────────────────────┐
 GitHub CSVs ─┤  Azure Data Factory      ├─► Bronze (lookup files)
              └──────────────────────────┘
                                                   │
 netflix_titles.csv ─► raw ─► Auto Loader ─► Bronze (titles)
                                                   │
                                                   ▼
                          Databricks Workflows + PySpark
                                                   │
                                                   ▼
                                     Silver (Delta tables)
                                                   │
                                                   ▼
                                 Delta Live Tables + quality rules
                                                   │
                                                   ▼
                                      Gold (ready for reporting)
```

1. **Ingest the lookup files.** Data Factory first checks that the main titles file has arrived. It then loops through the four lookup files and copies each one into Bronze. A single parameterised copy activity handles all four, so there's no need for four separate pipelines.
2. **Load titles incrementally.** Auto Loader watches the `raw` container. Whenever a new titles file lands, it streams only the new records into Bronze and keeps track of what it has already processed in a checkpoint.
3. **Clean the data in Silver.** Databricks notebooks read from Bronze, fix and enrich the data, and write it to Silver as Delta tables. A Databricks Workflow runs this for every lookup file in a loop.
4. **Publish to Gold.** A Delta Live Tables pipeline reads from Silver and applies quality rules. For example, any row without a `show_id` is dropped. It then writes the final Gold tables.

---

## What's in this repository

```
Netflix_Azure_Data_Engineering_Project/
├── README.md
├── Notebooks/
│   ├── 1_Autoloader.ipynb
│   ├── 2_silver.ipynb
│   ├── 3_lookupNotebook.ipynb
│   ├── 4_Silver.ipynb
│   ├── 5_lookupNotebook.ipynb
│   ├── 6_falsenotebook.ipynb
│   └── 7_DLT_Notebook.ipynb
└── RawData_AND_Notebooks/
    ├── Netflix Project.dbc        ← all notebooks in one Databricks archive
    ├── netflix_titles.csv
    ├── netflix_cast.csv
    ├── netflix_category.csv
    ├── netflix_countries.csv
    └── netflix_directors.csv
```

### The notebooks, one by one

**`1_Autoloader`** creates the schema in Unity Catalog. It then sets up Auto Loader (`cloudFiles`) to stream new CSV files from `raw` into Bronze every 10 seconds, with a checkpoint so each file is loaded only once.

**`2_silver`** is a reusable notebook that takes a source folder and a target folder as parameters, using Databricks widgets. It reads a lookup CSV from Bronze and appends it to Silver as a Delta table. One notebook handles all four lookup files.

**`3_lookupNotebook`** holds the list of the four lookup files and hands it to the workflow using `dbutils.jobs.taskValues`. The workflow loops over this list and runs `2_silver` once for each file.

**`4_Silver`** is where most of the transformation happens, on the titles data:
- It fills missing durations (0 minutes for movies, 1 season for shows) and casts them to whole numbers.
- It creates a short title by trimming everything after a colon.
- It cleans up the rating values.
- It adds a `type_flag` column, set to 1 for movies and 2 for TV shows.
- It ranks titles by duration using a window function.
- It shows how temporary views and global views work.
- It writes the result to Silver as a Delta table.

**`5_lookupNotebook`** reads a weekday value and passes it to the workflow, which uses it in an if/else condition.

**`6_falsenotebook`** is the notebook that runs when that condition is false. It simply reads and prints the value, to show how tasks in a workflow can share data.

**`7_DLT_Notebook`** is the Gold layer, built with Delta Live Tables:
- It creates Gold tables for directors, cast, countries and categories, and drops any row missing a `show_id`.
- It builds the titles table in three steps: a staging table, a transformation view and the final table.
- The final titles table checks two quality rules before any data gets through.

### The data

The five CSV files are the raw Netflix dataset used in the project. `netflix_titles.csv` is the main file, and the other four are lookup files linked to it by `show_id`.

---

## How to run it yourself

1. **Set up Azure.** Create a resource group, then a storage account with hierarchical namespace turned on. Inside it, create the `raw`, `bronze`, `silver` and `gold` containers.
2. **Create Data Factory and Databricks.** Set up a Databricks workspace with Unity Catalog enabled, and give it access to the storage account through an access connector.
3. **Update the paths.** The notebooks point at the original author's storage account (`netflixprojectdlansh`) and catalog (`netflix_catalog`). Replace these with your own names.
4. **Import the notebooks.** In Databricks, go to *Workspace → Import* and upload either the `.dbc` file or the `.ipynb` files from `Notebooks/`.
5. **Build the Data Factory pipeline.** Create the copy pipeline that brings the four lookup files into Bronze.
6. **Run the notebooks in order.** Start Auto Loader, then run the Silver workflow, and finally run the DLT pipeline.

> **Note:** The Data Factory pipeline and the Databricks Workflow settings were built in the Azure and Databricks portals, so they aren't stored as files in this repository.

---

## What I learned

- How the Bronze/Silver/Gold (medallion) pattern keeps a pipeline organised and easy to debug.
- How to build one parameterised pipeline or notebook that handles many files, instead of copying the same logic over and over.
- How Auto Loader and checkpoints make incremental loading simple and reliable.
- How Databricks Workflows can pass values between tasks and use loops and conditions.
- How Delta Live Tables add data quality rules directly into the pipeline, so bad rows never reach the final tables.

---

## Credits

- **Project and tutorial:** [Ansh Lamba](https://www.youtube.com/watch?v=uc-u_juRg-w)
- **Original repository:** [anshlambagit/Netflix_Azure_Data_Engineering_Project](https://github.com/anshlambagit/Netflix_Azure_Data_Engineering_Project)
