WITH staging AS (
    SELECT * FROM {{ ref('stg_gcp_billing__billing_exports') }}
),

final AS (
    SELECT
        -- Primary identifiers
        billing_account_id,
        project_id,
        project_number,
        project_name,

        -- Resource identifiers
        resource_name,
        resource_global_name,
        resource_global_name AS resource_id,

        -- Time dimensions
        usage_start_time,
        usage_end_time,
        export_time,
        invoice_publisher_type,
        service_id,

        -- Service and SKU
        service_description,
        sku_id,
        sku_description,
        location_location,

        -- Location
        location_country,
        location_region,
        location_zone,
        cost,

        -- Cost columns (native GCP)
        currency,
        cost_at_list,
        cost_at_effective_price_default,
        cost_at_list_consumption_model,
        cost_type,
        currency_conversion_rate,
        credit_committed_usage_amount,

        -- Credits (individual types from staging)
        credit_flex_cud_amount,
        credit_sustained_usage_amount,
        credit_promotion_amount,
        credit_discount_amount,
        credits_total,
        credits_total_less_promotions,
        usage_amount,

        -- Usage information
        usage_unit,
        usage_amount_in_pricing_units,
        usage_pricing_unit,
        effective_price,

        -- Price information
        tier_start_amount,
        price_unit,
        pricing_unit_quantity,
        list_price,
        effective_price_default,
        list_price_consumption_model,
        transaction_type,

        -- Transaction details
        seller_name,
        adjustment_info_id,
        adjustment_info_description,
        adjustment_info_mode,
        adjustment_info_type,
        consumption_model_id,

        -- Consumption model
        consumption_model_description,
        subscription_instance_id,
        project_ancestry_numbers,

        -- Project metadata
        project_ancestors,
        {% for label in var('finops_focus', {}).get('gcp', {}).get('labels', []) %}
        label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('-', '_') }}{% endif %},
        {% endfor %}
        {% for label in var('finops_focus', {}).get('gcp', {}).get('project_labels', []) %}
        project_label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('-', '_') }}{% endif %},
        {% endfor %}
        {% for label in var('finops_focus', {}).get('gcp', {}).get('system_labels', []) %}
        system_label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('/', '_') | replace('-', '_') }}{% endif %},
        {% endfor %}
        cost AS list_cost,

        -- Calculated cost columns
        PARSE_DATE('%Y%m', invoice_month) AS invoice_month,
        cost + COALESCE(credits_total, 0) AS effective_cost,
        cost + COALESCE(credits_total, 0) AS billed_cost,

        -- Absolute values for coverage analysis
        ABS(COALESCE(credit_committed_usage_amount, 0)) AS abs_cud_discount,
        ABS(COALESCE(credit_flex_cud_amount, 0)) AS abs_flex_cud_discount,
        ABS(COALESCE(credit_sustained_usage_amount, 0)) AS abs_sud_discount,
        ABS(COALESCE(credit_promotion_amount, 0)) AS abs_promotion_discount,
        ABS(COALESCE(credit_discount_amount, 0)) AS abs_other_discount,

        -- Coverage percentages (for aggregate analysis)
        SAFE_DIVIDE(
            ABS(COALESCE(credit_committed_usage_amount, 0)),
            NULLIF(cost, 0)
        ) AS cud_coverage_pct,
        SAFE_DIVIDE(
            ABS(COALESCE(credit_flex_cud_amount, 0)),
            NULLIF(cost, 0)
        ) AS flex_cud_coverage_pct,
        SAFE_DIVIDE(
            ABS(COALESCE(credit_sustained_usage_amount, 0)),
            NULLIF(cost, 0)
        ) AS sud_coverage_pct,

        -- Plotly Business Logic: Pricing Model
        CASE
            WHEN
                ABS(COALESCE(credit_committed_usage_amount, 0)) > 0
                THEN 'Committed Use Discount'
            WHEN
                ABS(COALESCE(credit_flex_cud_amount, 0)) > 0
                THEN 'Flexible Committed Use Discount'
            WHEN
                ABS(COALESCE(credit_sustained_usage_amount, 0)) > 0
                THEN 'Sustained Use Discount'
            ELSE 'On Demand'
        END AS finops_pricing_model,

        -- Plotly Business Logic: Usage Class
        CASE
            WHEN service_description = 'Compute Engine' AND (
                LOWER(sku_description) LIKE '%instance%'
                OR LOWER(sku_description) LIKE 'compute optimized%'
                OR sku_description LIKE '%SSD backed Local Storage%'
            ) THEN 'Reservable'
            ELSE 'Non-Reservable'
        END AS finops_usage_class,

        -- Plotly Business Logic: Usage Category
        CASE
            WHEN sku_description = 'Tax' THEN 'Tax'
            WHEN service_description = 'Compute Engine' AND (
                LOWER(sku_description) LIKE '%instance%'
                OR LOWER(sku_description) LIKE 'compute optimized%'
                OR LOWER(sku_description) LIKE 'reserved compute optimized%'
                OR LOWER(sku_description) LIKE 'commitment v1: cpu%'
                OR LOWER(sku_description) LIKE 'commitment v1: ram%'
                OR LOWER(sku_description) LIKE 'commitment - dollar based%'
                OR LOWER(sku_description) LIKE 'commitment: compute optimized%'
            ) THEN 'Compute'
            WHEN (
                service_description = 'Compute Engine' AND (
                    sku_description LIKE 'SSD backed Local Storage%'
                    OR sku_description LIKE 'SSD backed PD Capacity%'
                    OR sku_description LIKE 'Extreme PD IOPS%'
                    OR sku_description LIKE '%PD Capacity%'
                    OR sku_description LIKE 'Storage PD Snapshot%'
                    OR LOWER(sku_description) LIKE 'commitment v1: local ssd%'
                )
                OR service_description = 'Cloud Storage'
            ) THEN 'Storage'
            WHEN service_description = 'Compute Engine' AND (
                sku_description LIKE '%Egress%'
                OR sku_description LIKE '%Ingress%'
                OR sku_description LIKE '%Data Transfer%'
            ) THEN 'Bandwidth'
            WHEN
                service_description LIKE 'BigQuery%'
                OR service_description IN (
                    'Cloud Pub/Sub',
                    'Fivetran Data Pipelines',
                    'Cloud Dataflow',
                    'Dataplex',
                    'Data Catalog',
                    'Notebooks',
                    'Vertex AI'
                ) THEN 'Data and Analysis'
            -- Container Platform (GKE cluster management fees)
            WHEN service_description = 'Kubernetes Engine'
                AND sku_description IN (
                    'Zonal Kubernetes Clusters',
                    'Regional Kubernetes Clusters'
                )
                THEN 'Container Platform'
            -- Serverless containers
            WHEN service_description IN (
                'Cloud Run',
                'App Engine'
            ) THEN 'Serverless'
            WHEN service_description IN (
                'Cloud Monitoring',
                'Cloud Logging'
            ) THEN 'Management and Governance'
            WHEN
                service_description = 'Networking'
                OR LOWER(sku_description) LIKE '%ip charge%'
                OR LOWER(sku_description) LIKE '%load balancer%'
                OR LOWER(sku_description) LIKE 'network load balancing%'
                OR service_description = 'Cloud DNS'
                THEN 'Networking'
            WHEN service_description IN (
                'Cloud Key Management Service (KMS)',
                'Secret Manager',
                'Cloud IDS'
            ) THEN 'Security'
            WHEN service_description LIKE 'Claude%'
                OR service_description = 'Gemini API'
                THEN 'AI and Machine Learning'
            WHEN service_description IN (
                'Cloud SQL',
                'Cloud Memorystore for Redis',
                'Cloud Memorystore for Memcached'
            ) THEN 'Databases'
            WHEN service_description IN (
                'Artifact Registry',
                'Cloud Build'
            ) THEN 'Developer Tools'
            WHEN service_description IN (
                'Backup and DR Service'
            ) THEN 'Storage'
            ELSE 'Other'
        END AS finops_usage_category

    FROM staging
)

SELECT * FROM final
