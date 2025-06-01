### This project involves building an automated process, with additional requirements for data processing, normalization and analysis.

## Prerequisites Before Running the Script

Before running the script, make sure you have the following installed and configured:

- **Install Python** (recommended version: 3.9.0).
- **Install Microsoft SQL Server** and verify that the server is running.
- **Check the connection string**, for example:  
  `Server=localhost\MSSQLSERVER01;`  
  This is defined in the `config.json` file under the key `"server"`.
- **Install SQL Server Management Studio (SSMS)**  
  You can download it from the official Microsoft website or install the **SQL Server (mssql)** extension in **Visual Studio Code**.
- **Use Windows Authentication** if you're running on a Windows machine.

---

## Configuration and Auxiliary Files

- All configuration settings for data retrieval and processing are stored in:  
  `config.json`
- Auxiliary files used in the project:
  - `dataset.csv`
  - `dataset.json`
  - `SQLQuery.sql` (used to verify the structure of the tables being imported into the database)

## Project Phases

### Phase I – Dataset Retrieval

- The dataset is retrieved using a **URL and Bearer token**.
- After the request, we **verify if the retrieval was successful**.
- All steps are wrapped in **try-catch blocks** to handle exceptions and log errors to the `parser_errors.log` file.
- The retrieved data is unpacked into a `.csv` file (or `.xlsx` if needed to better inspect the data structure).
- If the dataset contains **nested JSON structures** (lists, dictionaries), they are unpacked separately.
- We process the dataset using the **Pandas** library for efficient manipulation of DataFrames.
- The script includes **dataset validation checks** to ensure consistency and expected structure.
- Note: Some columns might contain **complex data types** or **unusual formats** (e.g. dates). Special attention is required when handling such fields.

### Phase II – Inserting Data into the Database

- The script uses an **ODBC driver** to connect to the SQL Server (via a supported Python library like `pyodbc`).
- A **cursor object** is created to execute SQL queries.
- Data is **loaded from the `.csv` file into a Pandas DataFrame** using chunks.
- Column names are normalized (e.g., converted to **lowercase**, and multi-word names are joined using underscores or dashes).
- If necessary, **some fields are converted to strings**, and explicit casting is performed later in SQL queries.
- The existing table (if it exists) is **dropped before inserting new data**, to avoid overloading or duplicating data in the database.
- A **parameterized SQL query** is prepared for inserting records into the table.
- The data is **inserted into the SQL table**, and the **database connection is properly closed**.

### Phase III – Preparing the Database for Normalization

- Before creating any new table, we first **check if it already exists** and **drop it if necessary** to avoid unnecessary load on the database.
- We **select the `transactions` table** and load it into a **Pandas DataFrame**.
- A new table called **`new_transactions`** is created and inserted into the database.
- This table differs from the original `transactions` table in that the **`City` column is split into two separate columns**:  
  - city  
  - country
- By doing this, we **prepare the original data for easier normalization**, specifically for **First Normal Form (1NF)**.

### Phase IV – Normalization

- Data from the existing **`new_transactions`** table is loaded into a **DataFrame**.
- Several **auxiliary (helper) tables** are created to support normalization.
- These helper tables are populated using **Python dictionaries**, through a dedicated function for tables with a **simple primary key**.
- **Tables that require a composite primary key** are handled with special logic and processing.
- A new, fully **normalized table is created and committed to the database**.

### Phase V – Datamart Table

- Columns **`city`** and **`amount`** are loaded from the **`new_transactions`** table.
- These columns are used to create an **aggregated report** (datamart).
- The data is **grouped by the `city` column**, and aggregation is performed.  
  (An equivalent **SQL query is also available** in the `SQLQuery.sql` file for reference.)
- The following aggregated metrics are calculated:
  - **Number of transactions**
  - **Total spending**
  - **Average spending**
- A new table called **`datamart_po_gradu`** is created (if it doesn't already exist) and saved to the database.
- Finally, the **database connection is closed**, marking the end of the automated data processing pipeline for this dataset.




