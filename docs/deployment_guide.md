# Deployment Guide

This guide walks through deploying the Azure E-Commerce Data Pipeline into a real Azure subscription, end to end. Follow the steps in order — each stage depends on resources created in the previous one.

## Prerequisites

- An active Azure subscription with Owner or Contributor access
- Azure CLI installed (`az --version` to confirm) or access to the Azure Portal
- Python 3.9+ locally (for testing data generation / validation scripts)
- A GitHub account, if you intend to fork/clone this repo into Databricks Repos

---

## Step 1 — Create the Resource Group

```bash
az login

az group create \
  --name rg-ecommerce-data-pipeline \
  --location eastus
```

---

## Step 2 — Provision Azure Data Lake Storage Gen2

```bash
az storage account create \
  --name ecommercedatalakedev \
  --resource-group rg-ecommerce-data-pipeline \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create the containers used throughout this project
az storage container create --account-name ecommercedatalakedev --name raw
az storage container create --account-name ecommercedatalakedev --name datalake
```

Upload the sample source files to the landing zone:

```bash
az storage blob upload-batch \
  --account-name ecommercedatalakedev \
  --destination raw/landing/ecommerce \
  --source ./datasets
```

---

## Step 3 — Create an Azure Key Vault and Store Secrets

```bash
az keyvault create \
  --name ecommerce-kv-dev \
  --resource-group rg-ecommerce-data-pipeline \
  --location eastus

# Store the storage account key
STORAGE_KEY=$(az storage account keys list --account-name ecommercedatalakedev --query "[0].value" -o tsv)
az keyvault secret set --vault-name ecommerce-kv-dev --name adls-account-key --value "$STORAGE_KEY"
```

You will add `databricks-access-token` and `synapse-sql-password` secrets after provisioning those services in later steps.

---

## Step 4 — Provision Azure Databricks

```bash
az databricks workspace create \
  --resource-group rg-ecommerce-data-pipeline \
  --name ecommerce-databricks-dev \
  --location eastus \
  --sku standard
```

1. Open the workspace in the Azure Portal and launch the Databricks UI.
2. Create a cluster (Runtime 13.x LTS or later, single-node is sufficient for the sample data volumes).
3. Generate a **Personal Access Token** (User Settings → Developer → Access Tokens) and store it in Key Vault:
   ```bash
   az keyvault secret set --vault-name ecommerce-kv-dev --name databricks-access-token --value "<your-token>"
   ```
4. Under **Repos**, add this Git repository so the notebooks in `/databricks` are available at `/Repos/ecommerce-pipeline/databricks/...` (matching the paths referenced in `adf_pipeline.json`).

---

## Step 5 — Provision Azure Synapse Analytics

```bash
az synapse workspace create \
  --name ecommerce-synapse-dev \
  --resource-group rg-ecommerce-data-pipeline \
  --storage-account ecommercedatalakedev \
  --file-system datalake \
  --sql-admin-login-user synapseadmin \
  --sql-admin-login-password "<choose-a-strong-password>" \
  --location eastus

az keyvault secret set --vault-name ecommerce-kv-dev --name synapse-sql-password --value "<the-password-above>"

az synapse sql pool create \
  --name ecommerce_sqlpool \
  --workspace-name ecommerce-synapse-dev \
  --resource-group rg-ecommerce-data-pipeline \
  --performance-level DW100c
```

Run the SQL scripts **in this order** against `ecommerce_sqlpool` using Synapse Studio's SQL script editor:

1. `synapse/create_schema.sql`
2. `synapse/create_external_tables.sql` *(run after Gold data exists — see Step 7)*
3. `synapse/gold_views.sql`
4. `synapse/analytical_queries.sql` *(validation / exploration — run anytime after step 2)*

> Make sure the Synapse workspace's managed identity has **Storage Blob Data Contributor** on the `ecommercedatalakedev` storage account, or the `ADLS_Credential` database-scoped credential will fail to authenticate.

---

## Step 6 — Provision and Configure Azure Data Factory

```bash
az datafactory create \
  --resource-group rg-ecommerce-data-pipeline \
  --factory-name ecommerce-adf-dev \
  --location eastus
```

1. Open **ADF Studio** and import the JSON definitions:
   - **Manage → Linked Services → Import** → `datafactory/linked_service.json`
   - **Author → Datasets → Import** → `datafactory/datasets.json`
   - **Author → Pipelines → Import** → `datafactory/adf_pipeline.json`
2. Update the placeholder values in the linked services (storage account name, Databricks workspace URL, cluster ID, Synapse server name) to match the resources you just created.
3. Validate the pipeline (toolbar → **Validate**) and fix any reference errors.

---

## Step 7 — Run the Pipeline End to End

1. In ADF Studio, select `PL_Ecommerce_Bronze_Ingestion` and click **Debug** (or **Add Trigger → Trigger Now** for a full run).
2. Monitor execution under **Monitor → Pipeline Runs**. You should see, in order: `ForEach_Source_Entity` → `Notebook_Bronze_Layer` → `Notebook_Silver_Layer` → `Notebook_Gold_Layer` → `Log_Pipeline_Success`.
3. Once the Gold notebook succeeds, go back to Synapse Studio and run `synapse/create_external_tables.sql` (the Gold Parquet files now exist for the external tables to point at).
4. Run `synapse/gold_views.sql` to create the reporting views.
5. (Optional) Run `databricks/business_kpis.py` directly from Databricks if you want the standalone KPI tables under `gold/kpis/`.

---

## Step 8 — Validate

Run a handful of queries from `synapse/analytical_queries.sql` in Synapse Studio, for example:

```sql
SELECT TOP 10 * FROM gold.vw_customer_sales ORDER BY total_spent DESC;
SELECT * FROM gold.vw_monthly_revenue_trend ORDER BY order_month;
```

If both return rows, the pipeline is fully wired end to end.

---

## Step 9 — Connect a BI Tool (Optional)

Connect Power BI (or any SQL client) to the Synapse dedicated SQL pool using the **Azure Synapse Analytics SQL** connector, authenticate with the `synapseadmin` credentials (or a scoped read-only login), and build reports directly on `gold.vw_customer_sales`, `gold.vw_product_performance`, and `gold.vw_seller_performance`.

---

## Cleanup

To avoid ongoing charges when you're done experimenting:

```bash
az group delete --name rg-ecommerce-data-pipeline --yes --no-wait
```

This removes all resources created in this guide (storage account, Key Vault, Databricks workspace, Synapse workspace and SQL pool, and Data Factory) in one command.
