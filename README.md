# DBT FinOps FOCUS

A unified DBT package implementing the FinOps FOCUS (FinOps Open Cost and Usage Specification) for multi-cloud billing data across GCP, AWS, and Azure.

## Overview

This package transforms raw cloud billing data into FOCUS-compliant cost and usage data with:
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
  - local: ../../packages/dbt-finops-focus

  # For production (once published to GitHub)
  # - git: "https://github.com/your-org/dbt-finops-focus"
  #   revision: v0.1.0
```

## Configuration

### Enable Vendors

In your project's `dbt_project.yml`:

```yaml
vars:
  finops_focus:
    # Enable only the vendors you use
    enabled_vendors: ['gcp', 'aws']  # Options: 'gcp', 'aws', 'azure'
```

### Configure Schema Placement (Optional)

Override default schema names:

```yaml
vars:
  finops_focus:
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
  finops_focus:
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

#### AWS Tag Extraction

```yaml
vars:
  finops_focus:
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
  finops_focus:
    enabled_vendors: ['azure']
    azure:
      user_tags:
        - your_team_tag
        - your_project_tag
```

### Define Source Tables

Each enabled vendor requires source definition in your project's `models/staging/_sources.yml`:

#### GCP Source

```yaml
sources:
  - name: gcp_billing
    database: your-gcp-project-id
    schema: your_billing_dataset
    tables:
      - name: gcp_billing_export
        identifier: gcp_billing_export_resource_v1_XXXXXX_XXXXXX_XXXXXX
```

#### AWS Source

```yaml
sources:
  - name: aws_billing
    database: your-project-id
    schema: your_cur_dataset
    tables:
      - name: aws_cur_data
        identifier: your_actual_cur_table_name
```

#### Azure Source

```yaml
sources:
  - name: azure_billing
    database: your-project-id
    schema: your_focus_dataset
    tables:
      - name: azure_focus_data
        identifier: your_actual_focus_table_name
```

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
  finops_focus:
    enabled_vendors: ['gcp']
    gcp:
      labels: [team, owner, cost_center]
```

### Multi-Cloud with Custom Schemas

```yaml
vars:
  finops_focus:
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
