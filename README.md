# Open FinOps Transformations

An open-source dbt package for transforming multi-cloud billing data (GCP, AWS, Azure) into consistent, analytics-ready formats. Built on the FinOps FOCUS (FinOps Open Cost and Usage Specification) standard with additional opinionated transformations for practical FinOps workflows.

## Overview

This package transforms raw cloud billing data into (mostly) FOCUS-compliant cost and usage data with:
- Unified multi-cloud billing model
- Vendor-specific staging and intermediate models
- Configurable vendor enablement
- Customizable schema placement
- Pricing model classification
- Usage categorization
- Discount and credit handling

## Installation

Add to your `packages.yml`:

```yaml
packages:
  # For local development
  # - local: ../../packages/dbt-finops-focus

  # For production (once published to GitHub)
  - git: "https://github.com/your-org/dbt-finops-focus"
    revision: v0.1.0
```

## Configuration

### Enable Vendors

In your project's `dbt_project.yml`:

```yaml
vars:
  open_finops:
    # Enable only the vendors you use
    enabled_vendors: ['gcp', 'aws']  # Options: 'gcp', 'aws', 'azure'
```

### Configure Schema Placement (Optional)

Override default schema names:

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp']
    schemas:
      staging: backroom        # Default: 'staging'
      intermediate: backroom   # Default: 'staging'
      unified: analytics       # Default: 'marts'
```

### Configure Vendor-Specific Labels/Tags

#### GCP Label Extraction

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp']
    gcp:
      labels:
        - your_team_label
        - your_owner_label
      project_labels:
        - owner
        - department
      system_labels:
        - key: compute.googleapis.com/machine_spec
          name: machine_spec
```

#### GCP Export Type

Configure which GCP billing export type you use:

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp']
    gcp:
      export_type: standard  # Options: 'standard' or 'resource'
```

**Options:**
- **`standard`** (default): Standard billing export. Resource columns (`resource_name`, `resource_global_name`) will be NULL.
- **`resource`**: Resource-level billing export with detailed usage data. Includes full resource identification.

**Note:** Resource-level exports provide more granular data but are larger and may incur additional BigQuery costs.

#### GCP Timezone Configuration (Optional)

Configure timezone for date conversions to match your GCP Console billing reports:

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp']
    gcp:
      timezone: America/Los_Angeles  # Match your GCP Console timezone
```

When configured, adds timezone-aware columns: `usage_start_time_local`, `usage_end_time_local`, and `usage_date_local` (recommended for filtering).

**Why?** Raw billing timestamps are in UTC, but GCP Console uses your configured timezone for reports. Without matching timezones, your queries may include/exclude a few hours of data at month boundaries, causing slight discrepancies.

#### AWS Tag Extraction

```yaml
vars:
  open_finops:
    enabled_vendors: ['aws']
    aws:
      user_tags:
        - your_team_tag
        - your_cost_center_tag
      system_tags:
        - created_by
```

#### Azure Tag Extraction

```yaml
vars:
  open_finops:
    enabled_vendors: ['azure']
    azure:
      user_tags:
        - your_team_tag
        - your_project_tag
```

### Configure Source Tables

Source tables are configured in your **project's `dbt_project.yml`** under the vendor-specific configuration:

#### GCP Source Configuration

Add to your `dbt_project.yml`:

```yaml
vars:
  open_finops:
    gcp:
      database: your-gcp-project-id
      schema: your_billing_dataset
      table_identifier: gcp_billing_export_resource_v1_XXXXXX_XXXXXX_XXXXXX
```

#### AWS Source Configuration

Add to your `dbt_project.yml`:

```yaml
vars:
  open_finops:
    aws:
      database: your-project-id
      schema: your_cur_dataset
      table_identifier: your_actual_cur_table_name
```

#### Azure Source Configuration

Add to your `dbt_project.yml`:

```yaml
vars:
  open_finops:
    azure:
      database: your-project-id
      schema: your_focus_dataset
      table_identifier: your_actual_focus_table_name
```

**Note:** If not specified, the package uses sensible defaults (target.database for database, vendor-specific schema names for schema).

## Models

### Staging Layer

Per-vendor staging models that unnest and flatten raw billing data:
- `stg_gcp_billing__billing_exports` - GCP billing with label extraction
- `stg_aws_billing__cur_data` - AWS CUR with tag extraction
- `stg_azure_billing__focus_data` - Azure FOCUS with tag extraction

### Intermediate Layer

Per-vendor enriched models with FOCUS transformations:
- `int_cloud_billing_gcp_enriched` - GCP with pricing model classification, coverage percentages
- `int_cloud_billing_aws_enriched` - AWS with RI/SP classification
- `int_cloud_billing_azure_enriched` - Azure with reservation classification

### Unified Layer

- `cloud_billing_unified` - Multi-cloud unified view with FOCUS-compliant schema
  - Combines all enabled vendors
  - Standardizes column names across vendors
  - Adds `cloud_provider` column
  - Maps vendor-specific concepts to common fields

## Usage Examples

### Single Vendor (GCP only)

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp']
    gcp:
      labels: [team, owner, cost_center]
```

### Multi-Cloud with Custom Schemas

```yaml
vars:
  open_finops:
    enabled_vendors: ['gcp', 'aws', 'azure']
    schemas:
      staging: raw
      intermediate: transformed
      unified: analytics
    gcp:
      labels: [team, owner]
    aws:
      user_tags: [Team, Owner]
    azure:
      user_tags: [team, owner]
```

### Query the Unified Model

```sql
-- Get total cost by cloud provider
SELECT
  cloud_provider,
  DATE_TRUNC(billing_period_start, MONTH) as month,
  SUM(billed_cost) as total_cost
FROM {{ ref('cloud_billing_unified') }}
GROUP BY 1, 2
ORDER BY 2 DESC, 3 DESC
```

## Package Structure

```
dbt-finops-focus/
├── models/
│   ├── staging/
│   │   ├── gcp/           # GCP staging models
│   │   ├── aws/           # AWS staging models
│   │   └── azure/         # Azure staging models
│   ├── intermediate/
│   │   ├── gcp/           # GCP intermediate models
│   │   ├── aws/           # AWS intermediate models
│   │   └── azure/         # Azure intermediate models
│   └── unified/
│       └── cloud_billing_unified.sql  # Multi-cloud unified model
└── dbt_project.yml        # Package configuration
```

## Requirements

- DBT Core >= 1.0.0
- BigQuery adapter (for GCP data warehouse)
- Billing exports configured per vendor

## License

MIT
