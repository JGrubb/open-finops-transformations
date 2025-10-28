{{
  config(
    enabled=var('finops_focus', {}).get('enabled_vendors', [])|length > 0
  )
}}

{% set enabled_vendors = var('finops_focus', {}).get('enabled_vendors', []) %}

{% if 'gcp' in enabled_vendors %}
WITH gcp_data AS (
    SELECT
        -- Cloud provider identifier
        'gcp' AS cloud_provider,

        -- FOCUS-aligned charge origination columns
        'GCP' AS provider_name,
        seller_name AS publisher_name,
        invoice_publisher_type AS invoice_issuer_name,

        -- FOCUS-aligned timeframe columns
        TIMESTAMP(invoice_month) AS billing_period_start,
        CAST(NULL AS TIMESTAMP) AS billing_period_end,
        usage_start_time AS charge_period_start,
        usage_end_time AS charge_period_end,

        -- FOCUS-aligned account columns
        billing_account_id AS billing_account_id,
        CAST(NULL AS STRING) AS billing_account_name,
        project_id AS sub_account_id,
        project_name AS sub_account_name,

        -- FOCUS-aligned service and SKU columns
        service_description AS service_name,
        service_description AS service_category,
        sku_id AS sku_id,
        sku_description AS sku_description,
        sku_id AS sku_price_id,
        finops_usage_category AS resource_type,

        -- FOCUS-aligned location columns
        location_region AS region_id,
        location_region AS region_name,
        location_zone AS availability_zone,

        -- FOCUS-aligned cost columns
        billed_cost AS billed_cost,
        effective_cost AS effective_cost,
        list_cost AS list_cost,
        currency AS billing_currency,

        -- FOCUS-aligned consumption columns
        usage_amount AS consumed_quantity,
        usage_unit AS consumed_unit,

        -- FOCUS-aligned pricing columns
        usage_amount_in_pricing_units AS pricing_quantity,
        usage_pricing_unit AS pricing_unit,

        -- Pricing model
        pricing_model

    FROM {{ ref('int_cloud_billing_gcp_enriched') }}
)
{% endif %}

{% if 'aws' in enabled_vendors %}
{{ ',' if 'gcp' in enabled_vendors else 'WITH' }} aws_data AS (
    SELECT
        -- Cloud provider identifier
        'aws' AS cloud_provider,

        -- FOCUS-aligned charge origination columns
        'AWS' AS provider_name,
        billing_entity AS publisher_name,
        invoicing_entity AS invoice_issuer_name,

        -- FOCUS-aligned timeframe columns
        billing_period_start_date AS billing_period_start,
        billing_period_end_date AS billing_period_end,
        line_item_usage_start_date AS charge_period_start,
        line_item_usage_end_date AS charge_period_end,

        -- FOCUS-aligned account columns
        bill_payer_account_id AS billing_account_id,
        CAST(NULL AS STRING) AS billing_account_name,
        line_item_usage_account_id AS sub_account_id,
        line_item_usage_account_name AS sub_account_name,

        -- FOCUS-aligned service and SKU columns
        product_servicename AS service_name,
        product_family AS service_category,
        product_sku AS sku_id,
        line_item_description AS sku_description,
        pricing_rate_id AS sku_price_id,
        finops_usage_category AS resource_type,

        -- FOCUS-aligned location columns
        product_region_code AS region_id,
        product_region AS region_name,
        line_item_availability_zone AS availability_zone,

        -- FOCUS-aligned cost columns
        line_item_unblended_cost AS billed_cost,
        focus_effective_cost AS effective_cost,
        pricing_public_on_demand_cost AS list_cost,
        line_item_currency_code AS billing_currency,

        -- FOCUS-aligned consumption columns
        line_item_usage_amount AS consumed_quantity,
        pricing_unit AS consumed_unit,

        -- FOCUS-aligned pricing columns
        line_item_usage_amount AS pricing_quantity,
        pricing_unit AS pricing_unit,

        -- Pricing model
        pricing_model

    FROM {{ ref('int_cloud_billing_aws_enriched') }}
)
{% endif %}

{% if 'azure' in enabled_vendors %}
{{ ',' if ('gcp' in enabled_vendors or 'aws' in enabled_vendors) else 'WITH' }} azure_data AS (
    SELECT
        -- Cloud provider identifier
        'azure' AS cloud_provider,

        -- FOCUS-aligned charge origination columns
        provider_name,
        publisher_name,
        invoice_issuer_name,

        -- FOCUS-aligned timeframe columns
        billing_period_start,
        billing_period_end,
        charge_period_start,
        charge_period_end,

        -- FOCUS-aligned account columns
        billing_account_id,
        billing_account_name,
        sub_account_id,
        sub_account_name,

        -- FOCUS-aligned service and SKU columns
        service_name,
        service_category,
        sku_id,
        x_sku_description AS sku_description,
        sku_price_id,
        finops_usage_category AS resource_type,

        -- FOCUS-aligned location columns
        region_id,
        region_name,
        CAST(NULL AS STRING) AS availability_zone,

        -- FOCUS-aligned cost columns
        billed_cost,
        effective_cost,
        list_cost,
        billing_currency,

        -- FOCUS-aligned consumption columns
        consumed_quantity,
        consumed_unit,

        -- FOCUS-aligned pricing columns
        pricing_quantity,
        pricing_unit,

        -- Pricing model
        pricing_model

    FROM {{ ref('int_cloud_billing_azure_enriched') }}
)
{% endif %}

SELECT * FROM {{ enabled_vendors[0] }}_data
{% for vendor in enabled_vendors[1:] %}
UNION ALL
SELECT * FROM {{ vendor }}_data
{% endfor %}
