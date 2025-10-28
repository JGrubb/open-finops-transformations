WITH source AS (
    SELECT *
    FROM {{ source('gcp_billing', 'gcp_billing_export') }}
),

renamed AS (
    SELECT
        billing_account_id,
        service.id AS service_id,
        service.description AS service_description,
        sku.id AS sku_id,
        sku.description AS sku_description,
        usage_start_time,
        usage_end_time,
        export_time,
        project.id AS project_id,
        project.number AS project_number,
        project.name AS project_name,
        project.ancestry_numbers AS project_ancestry_numbers,
        project.ancestors AS project_ancestors,
        resource.name AS resource_name,
        resource.global_name AS resource_global_name,
        location.location AS location_location,
        location.country AS location_country,
        location.region AS location_region,
        location.zone AS location_zone,
        price.effective_price,
        price.tier_start_amount,
        price.unit AS price_unit,
        price.pricing_unit_quantity,
        price.list_price,
        price.effective_price_default,
        price.list_price_consumption_model,
        transaction_type,
        seller_name,
        cost,
        currency,
        currency_conversion_rate,
        cost_at_list,
        cost_at_effective_price_default,
        cost_at_list_consumption_model,
        cost_type,
        usage.amount AS usage_amount,
        usage.unit AS usage_unit,
        usage.amount_in_pricing_units AS usage_amount_in_pricing_units,
        usage.pricing_unit AS usage_pricing_unit,
        invoice.month AS invoice_month,
        invoice.publisher_type AS invoice_publisher_type,
        adjustment_info.id AS adjustment_info_id,
        adjustment_info.description AS adjustment_info_description,
        adjustment_info.mode AS adjustment_info_mode,
        adjustment_info.type AS adjustment_info_type,
        consumption_model.id AS consumption_model_id,
        consumption_model.description AS consumption_model_description,
        subscription.instance_id AS subscription_instance_id,

        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type = 'COMMITTED_USAGE_DISCOUNT'
        ) AS credit_committed_usage_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type IN ('COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE', 'FEE_UTILIZATION_OFFSET')
        ) AS credit_flex_cud_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type = 'SUSTAINED_USAGE_DISCOUNT'
        ) AS credit_sustained_usage_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type = 'PROMOTION'
        ) AS credit_promotion_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type = 'DISCOUNT'
        ) AS credit_discount_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type = 'SUBSCRIPTION_BENEFIT'
        ) AS credit_subscription_benefit_amount,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
        ) AS credits_total,
        (
            SELECT SUM(CAST(amount AS NUMERIC))
            FROM UNNEST(credits)
            WHERE type != 'PROMOTION'
        ) AS credits_total_less_promotions,
        {% for label in var('gcp_billing', {}).get('labels', []) %}
        (SELECT value FROM UNNEST(labels) WHERE key = '{% if label is mapping %}{{ label.key }}{% else %}{{ label }}{% endif %}')
            AS label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('-', '_') }}{% endif %},
        {% endfor %}
        {% for label in var('gcp_billing', {}).get('project_labels', []) %}
        (SELECT value FROM UNNEST(project.labels) WHERE key = '{% if label is mapping %}{{ label.key }}{% else %}{{ label }}{% endif %}')
            AS project_label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('-', '_') }}{% endif %},
        {% endfor %}
        {% for label in var('gcp_billing', {}).get('system_labels', []) %}
        (SELECT value FROM UNNEST(system_labels) WHERE key = '{% if label is mapping %}{{ label.key }}{% else %}{{ label }}{% endif %}')
            AS system_label_{% if label is mapping %}{{ label.name }}{% else %}{{ label | replace('.', '_') | replace('/', '_') | replace('-', '_') }}{% endif %}{{ "," if not loop.last else "" }}
        {% endfor %}
    FROM source
)

SELECT * FROM renamed
