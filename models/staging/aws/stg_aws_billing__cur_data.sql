WITH source AS (
    SELECT * FROM {{ source('aws_billing', 'aws_cur_data') }}
),

renamed AS (
    SELECT
        -- identity
        identity_line_item_id,
        identity_time_interval,

        -- bill information
        bill_invoice_id AS invoice_id,
        bill_invoicing_entity AS invoicing_entity,
        bill_billing_entity AS billing_entity,
        bill_bill_type AS bill_type,
        bill_payer_account_id,
        bill_billing_period_start_date AS billing_period_start_date,
        bill_billing_period_end_date AS billing_period_end_date,

        -- line item core fields
        line_item_usage_account_id,
        line_item_line_item_type AS line_item_type,
        line_item_usage_start_date,
        line_item_usage_end_date,
        line_item_product_code,
        line_item_usage_type,
        line_item_operation,
        line_item_availability_zone,
        line_item_usage_amount,
        line_item_normalization_factor,
        line_item_normalized_usage_amount,
        line_item_currency_code,
        line_item_unblended_rate,
        line_item_unblended_cost,
        line_item_blended_rate,
        line_item_blended_cost,
        line_item_line_item_description AS line_item_description,
        line_item_tax_type,
        line_item_legal_entity,
        line_item_net_unblended_rate,
        line_item_net_unblended_cost,

        -- product key fields
        product_product_name AS product_name,
        product_region,
        product_region_code,
        product_location,
        product_location_type,
        product_availability_zone,
        product_instance_type,
        product_instance_type_family,
        product_operating_system,
        product_product_family AS product_family,
        product_servicecode,
        product_servicename,
        product_sku,
        product_vcpu,
        product_memory,
        product_memory_gib,
        product_storage,
        product_network_performance,

        -- pricing information
        pricing_currency,
        pricing_public_on_demand_cost,
        pricing_public_on_demand_rate,
        pricing_term,
        pricing_unit,
        pricing_rate_code,
        pricing_rate_id,
        pricing_lease_contract_length,
        pricing_offering_class,
        pricing_purchase_option,

        -- reservation information
        reservation_amortized_upfront_cost_for_usage,
        reservation_amortized_upfront_fee_for_billing_period,
        reservation_effective_cost,
        reservation_recurring_fee_for_usage,
        reservation_unused_amortized_upfront_fee_for_billing_period,
        reservation_unused_recurring_fee,
        reservation_reservation_arn,
        reservation_net_amortized_upfront_cost_for_usage,
        reservation_net_amortized_upfront_fee_for_billing_period,
        reservation_net_effective_cost,
        reservation_net_recurring_fee_for_usage,
        reservation_net_unused_amortized_upfront_fee_for_billing_period,
        reservation_net_unused_recurring_fee,

        -- savings plan information
        savings_plan_savings_plan_arn,
        savings_plan_savings_plan_rate,
        savings_plan_used_commitment,
        savings_plan_total_commitment_to_date,
        savings_plan_savings_plan_effective_cost,
        savings_plan_amortized_upfront_commitment_for_billing_period,
        savings_plan_recurring_commitment_for_billing_period,
        savings_plan_net_savings_plan_effective_cost,
        savings_plan_net_amortized_upfront_commitment_for_billing_period,
        savings_plan_net_recurring_commitment_for_billing_period,
        savings_plan_offering_type,
        savings_plan_region,

        -- discount information
        discount_bundled_discount,
        discount_spp_discount,
        discount_total_discount,

        {% for tag in var('open_finops', {}).get('aws', {}).get('user_tags', []) %}
        resource_tags_user_{% if tag is mapping %}{{ tag.source }}{% else %}{{ tag }}{% endif %} AS tag_{% if tag is mapping %}{{ tag.alias }}{% else %}{{ tag }}{% endif %},
        {% endfor %}
        {% for tag in var('open_finops', {}).get('aws', {}).get('system_tags', []) %}
        resource_tags_aws_{{ tag }} AS tag_aws_{{ tag }}{{ "," if not loop.last else "" }}
        {% endfor %}

    FROM source
)

SELECT * FROM renamed
