# 📦 Olist E-commerce Data Pipeline

> An end-to-end production data pipeline built on the Brazilian Olist e-commerce dataset — orchestrating extraction, transformation, and visualization across a modern lakehouse stack.
<p align="center">
  <img src="asset/datastackarch.png" width="800" alt="Modern Data Stack Architecture">
</p
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.x-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://getdbt.com)
[![Dagster](https://img.shields.io/badge/Dagster-Orchestration-854FFF?style=flat-square)](https://dagster.io)
[![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-4285F4?style=flat-square&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)

---

## 🗺️ Pipeline Overview

```
┌─────────────┐     ┌─────────────┐     ┌───────────────────────────────────┐     ┌─────────────┐
│   Meltano   │────▶│   Dagster   │────▶│              dbt_olist            │────▶│  BigQuery   │
│  (EL Layer) │     │(Orchestrate)│     │  Staging → Intermediate → Marts   │     │ (Warehouse) │
└─────────────┘     └─────────────┘     └───────────────────────────────────┘     └──────┬──────┘
                                                                                          │
                                                                                   ┌──────▼──────┐
                                                                                   │  Streamlit  │
                                                                                   │ (Dashboard) │
                                                                                   └─────────────┘
```

Raw CSV files from Olist are extracted and loaded by **Meltano**, orchestrated end-to-end by **Dagster** (with full asset lineage), transformed through three dbt layers into analytics-ready marts in **BigQuery**, and surfaced via a **Streamlit** executive dashboard.

---

## 🧩 Core Components

| Component | Tool | Role |
|---|---|---|
| **meltano/** | [Meltano](https://meltano.com) | Data Extraction & Loading (EL) — ingests raw Olist CSV datasets into BigQuery via configured tap/target plugins |
| **dagster/** | [Dagster](https://dagster.io) | Orchestration & Workflow Management — defines asset lineage, schedules, quality gates, and auto-generates dbt docs |
| **dbt_olist/models/staging/** | dbt | Raw source models as views — light renaming, type casting, and deduplication of source tables |
| **dbt_olist/models/intermediate/** | dbt | Business logic layer — RFV segmentation, order enrichment, customer metrics, product categorization |
| **dbt_olist/models/marts/core/** | dbt | Fact & Dimension tables for Olist sales — `fct_sales`, `dim_customers`, `dim_orders`, `dim_products`, `dim_sellers`, `dim_location`, `dim_time` |
| **eda/eda.ipynb** | Jupyter + BigQuery SDK | Exploratory data analysis — statistical profiling, null checks, geographic normalization audits, and data cleaning validation |
| **salesportal.py / .streamlit/** | [Streamlit](https://streamlit.io) + Plotly | Executive dashboard — KPIs, monthly revenue trends, state-level sales, RFM segmentation, and product category analysis |

---

## 🏗️ dbt Model Architecture

```
dbt_olist/models/
├── staging/                        # Materialized as VIEWS
│   ├── sources.yml
│   ├── stg_customers.sql/.yml
│   ├── stg_geolocation.sql/.yml
│   ├── stg_order_items.sql/.yml
│   ├── stg_order_payments.sql/.yml
│   ├── stg_order_reviews.sql/.yml
│   ├── stg_orders.sql/.yml
│   ├── stg_products.sql/.yml
│   └── stg_sellers.sql/.yml
│
├── intermediate/                   # Materialized as TABLES
│   ├── int_customer_location_mapping.sql
│   ├── int_customer_metrics.sql
│   ├── int_customer_segments.sql
│   ├── int_order_items_aggregated.sql
│   ├── int_order_payments_summary.sql
│   ├── int_orders_date_validity.sql
│   ├── int_orders_enriched.sql
│   ├── int_products_categorized.sql
│   ├── int_rfv_quartiles.sql
│   └── int_top_15_products.sql
│
└── marts/core/                     # Materialized as TABLES
    ├── fct_sales.sql               # Central fact table
    ├── dim_customers.sql           # RFM-enriched customer dimension
    ├── dim_orders.sql              # Order lifecycle & delivery metrics
    ├── dim_products.sql            # Product category enrichment
    ├── dim_sellers.sql             # Seller performance dimension
    ├── dim_location.sql            # Geographic dimension
    └── dim_time.sql                # Time dimension
```

---

## ⚙️ Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/<your-org>/2026-02-06_DS4_GP5_olist.git
cd 2026-02-06_DS4_GP5_olist
```

### 2. Create the Conda Environment

```bash
conda env create -f environment.yml
conda activate olist
```

### 3. Configure Credentials

Copy the example environment file and fill in your Google Cloud project details:

```bash
cp .env.example .env
```

Edit `.env` and set:

```dotenv
GOOGLE_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account.json
```

> **Note:** Ensure your service account has BigQuery Data Editor and Job User roles.

### 4. Verify Your Environment

```bash
python check_env.py
```

### 5. Run the Streamlit Dashboard

```bash
streamlit run salesportal.py
```

---

## 🚀 Running the Pipeline

### Start the Dagster UI (Asset Lineage & Orchestration)

```bash
cd dagster
dagster dev -f definition.py
```

Open [http://localhost:3000](http://localhost:3000) to view the full asset graph and trigger pipeline runs.

### Run the Full Pipeline Job

From the Dagster UI, launch the `run_full_pipeline` job — or trigger it via CLI:

```bash
dagster job execute -f definition.py -j run_full_pipeline
```

### Run dbt Transformations Directly

```bash
cd dbt_olist

# Run all models
dbt run

# Run a specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

### Meltano EL (Extract & Load)

```bash
cd meltano

# Install plugins
meltano install

# Run the EL pipeline
meltano run tap-csv target-bigquery
```

---

## 📁 Project Structure

```
2026-02-06_DS4_GP5_olist/
├── dagster/
│   ├── assets/
│   └── definition.py               # Dagster asset definitions & pipeline orchestration
├── data/
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   └── product_category_name_translation.csv
├── dbt_olist/
│   ├── models/
│   │   ├── staging/                # Source views
│   │   ├── intermediate/           # Business logic tables
│   │   └── marts/core/             # Fact & dimension tables
│   ├── seeds/
│   │   └── patch_missing_geolocations.csv
│   └── dbt_project.yml
├── eda/
│   └── eda.ipynb                   # EDA, data profiling & cleaning notebook
├── meltano/
│   ├── plugins/
│   └── meltano.yml
├── .env.example
├── environment.yml
├── check_env.py
└── salesportal.py                  # Streamlit executive dashboard
```
---

## 🔬 Exploratory Data Analysis (EDA)

The EDA notebook (`eda/eda.ipynb`) performs a full data quality audit and consistency check against the transformed BigQuery marts before dashboard consumption.

### Notebook Sections

| # | Section | Key Output |
|---|---|---|
| 1 | **Environment Setup & BigQuery Configuration** | Connected to BigQuery via `GOOGLE_PROJECT_ID` from `.env` using the Python SDK |
| 2 | **Data Loading & Initial Shape Analysis** | Loaded 6 core tables; `fct_sales` confirmed at **113,419 rows × 12 columns** |
| 3 | **Statistical Profiling & Distribution Curves** | Mean order value R$141 vs. median R$92 — long-tail distribution confirmed; max single order R$6,929 |
| 4 | **Dim_Customers Logic Check & Order Status Filtering** | NULL RFV: **0** — full customer base scored; `customer_id_is_invalid` uniformly `False` |
| 5 | **Dim_Products & Categorization Audit** | **591 uncategorized products** flagged; zero null physical dimensions (`weight`, `length`) |
| 6 | **Intermediate Table Patching & Monetary Logic** | Null monetary values patched to 0; long decimals rounded to 2dp (e.g. `238.99000000000004` → `238.99`) |
| 7 | **City & State Normalization Check** | **10 cities** with residual accent encoding errors (`maceia³` → `maceió`); 0 affected states |
| 8 | **Final Sales & Payments Integrity** | **0 rows dropped** from `fct_sales` — no zero-payment or missing payment method records found |

---

### 📊 Statistical Profile — `fct_sales`

| Metric | `price` | `freight_value` | `total_payment_value` |
|---|---|---|---|
| **Count** | 113,419 | 113,419 | 113,419 |
| **Mean** | R$ 121.27 | R$ 19.85 | R$ 141.15 |
| **Median (50%)** | R$ 75.00 | R$ 16.22 | R$ 92.65 |
| **Std Dev** | R$ 185.56 | R$ 15.84 | R$ 191.92 |
| **Min** | R$ 0.85 | R$ 0.00 | R$ 9.34 |
| **Max** | R$ 6,735.00 | R$ 409.68 | R$ 6,929.31 |

> **Key insight:** Mean (R$141) significantly exceeds median (R$92), indicating a right-skewed distribution driven by high-value outlier orders — log-scale applied in the dashboard to preserve visual clarity across all revenue ranges.

---

### 🔍 Key Findings

**Customer Dimension**
- `NULL RFV: 0` — all 96K+ customers successfully assigned an RFM segment; no scoring gaps remain
- `customer_id_is_invalid: [False]` — zero corrupted or duplicate customer IDs across the entire dimension table

**Product Dimension**
- **591 uncategorized products** detected; handled upstream via `int_products_categorized` intermediate model
- Zero null values for `product_weight_g` and `product_length_cm` — physical specs fully populated

**Sales Integrity**
- `fct_sales` confirmed at **113,419 records, 12 columns** — no rows dropped by the zero-payment filter
- All financial columns (`price`, `freight_value`, `total_payment_value`) confirmed as `float64` — numeric casting fix validated
- `payment_installments` confirmed as `Int64`; all ID columns correctly typed as `object`

**Geographic Normalization**
- **10 residual encoding errors** found in `customer_city` — all in Alagoas (AL), manifesting as `maceia³` instead of `maceió`
- Root cause: Latin-1 / UTF-8 encoding mismatch on a manually edited subset of source rows
- All state abbreviations (`customer_state`) confirmed accent-free ✅
- These 10 records are visible only when querying raw data via Python/dbt — BigQuery UI preview and Excel silently mask the broken byte

**Monetary Patching**
- `monetary_value` nulls → patched to `0`; no downstream NaN errors in RFM calculations
- Sample audit record confirmed: `total_item_value: 238.99`, `total_freight_value: 22.47` — 2dp rounding validated ✅
---

## 📊 Dashboard Preview

The Streamlit dashboard provides an executive-level view of the Olist dataset across four sections:

| Section | Description |
|---|---|
| **KPI Header** | Customers, New Prospects, Revenue, Order Volume, Products Sold, Average Order Value |
| **Monthly Sales Revenue** | Log-scale bar chart tracking revenue growth from Sep 2016 → Oct 2018 |
| **Sales Revenue by States** | Log-scale bar chart comparing all 27 Brazilian states |
| **Customer Loyalty Mix** | RFM-based treemap segmenting customers (Champions, Loyal, At Risk, etc.) |
| **Top 15 Product Categories** | Donut chart of best-selling categories by volume |
| **State Market Share** | Sunburst chart drilling from state → product category revenue |

<p align="center">
  <img src="asset/salesportal1.png" width="800" alt="Dashboard Overview">
</p>
<p align="center">
  <img src="asset/salesportal2.png" width="800" alt="Customer Segments & Categories">
</p>
<p align="center">
  <img src="asset/salesportal3.png" width="800" alt="State Market Share Sunburst">
</p
---

## 🧪 Data Quality

Quality gates are enforced at two levels:

- **dbt tests** — schema tests (not_null, unique, accepted_values) and `dbt_expectations` package tests run after every model execution
- **Dagster `quality_gate` asset** — downstream asset that confirms all dbt tests pass before docs generation proceeds

---

## 📦 Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Environment | Conda (`environment.yml`) |
| Extract & Load | Meltano |
| Orchestration | Dagster + dagster-dbt |
| Transformation | dbt-core + dbt-bigquery + dbt-expectations |
| Data Warehouse | Google BigQuery |
| EDA | Jupyter, Pandas, Matplotlib, Seaborn, Plotly |
| Visualization | Streamlit + Plotly Express |

---

## 📄 License

This project was developed as part of the DS4 Group Project (Group 5, 2026). Dataset sourced from the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle.
