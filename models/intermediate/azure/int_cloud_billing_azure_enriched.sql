WITH staging AS (
    SELECT * FROM {{ ref('stg_azure_billing__focus_data') }}
),

final AS (
    SELECT
        -- FOCUS standard columns (already in correct format)
        billed_cost,
        billing_account_id,
        billing_account_name,
        billing_account_type,
        billing_currency,
        billing_period_end,
        billing_period_start,
        charge_category,
        charge_class,
        charge_description,
        charge_frequency,
        charge_period_end,
        charge_period_start,
        commitment_discount_category,
        commitment_discount_id,
        commitment_discount_name,
        commitment_discount_status,
        commitment_discount_type,
        consumed_quantity,
        consumed_unit,
        contracted_cost,
        contracted_unit_price,
        effective_cost,
        invoice_issuer_name,
        list_cost,
        list_unit_price,
        pricing_category,
        pricing_quantity,
        pricing_unit,
        provider_name,
        publisher_name,
        region_id,
        region_name,
        resource_id,
        resource_name,
        resource_type,
        service_category,
        service_name,

        -- FinOps usage category (normalize Azure resource types to match AWS/GCP)
        CASE
            -- Bandwidth (check first before resource type)
            WHEN x_sku_meter_category = 'Bandwidth'
                OR LOWER(x_sku_meter_name) LIKE '%data transfer%'
                OR LOWER(x_sku_meter_name) LIKE '%egress%'
                OR LOWER(x_sku_meter_name) LIKE '%ingress%'
                THEN 'Bandwidth'

            -- Compute (VMs and scale sets)
            WHEN resource_type IN (
                'Virtual machine',
                'Virtual machine scale set',
                'Virtual machine scale set instance'
            ) THEN 'Compute'

            -- Container Platform (AKS cluster management)
            WHEN resource_type = 'Kubernetes service'
                THEN 'Container Platform'

            -- Storage (disks, snapshots, storage accounts)
            WHEN resource_type IN (
                'Disk',
                'Storage account',
                'Snapshot',
                'Image',
                'Restore Point Collection'
            ) THEN 'Storage'

            -- Databases
            WHEN resource_type IN (
                'Azure Database for MySQL flexible server',
                'Azure Database for PostgreSQL flexible server',
                'PostgreSQL server',
                'SQL database',
                'SQL server',
                'Redis cache'
            ) THEN 'Databases'

            -- Networking
            WHEN resource_type IN (
                'Application gateway',
                'Load balancer',
                'Public IP address',
                'Private endpoint',
                'Firewall',
                'DNS zone',
                'Private DNS zone'
            ) THEN 'Networking'

            -- Management and Governance
            WHEN resource_type IN (
                'Log Analytics workspace',
                'Log search alert rule',
                'Metric alert rule',
                'Action group',
                'Automation account',
                'Azure Managed Grafana'
            ) THEN 'Management and Governance'

            -- Security
            WHEN resource_type IN (
                'Defender for Cloud',
                'Key vault',
                'Bastion'
            ) THEN 'Security'

            -- Developer Tools
            WHEN resource_type IN (
                'Container registry',
                'Event Grid extension topic'
            ) THEN 'Developer Tools'

            -- Serverless
            WHEN resource_type IN (
                'App Service web app'
            ) THEN 'Serverless'

            -- Commitments and reservations
            WHEN resource_type IN (
                'Savings plan',
                'Reservation order'
            ) THEN 'Compute'

            -- Backup
            WHEN resource_type = 'Recovery Services vault'
                THEN 'Storage'

            ELSE 'Other'
        END AS finops_usage_category,
        sku_id,
        sku_price_id,
        sub_account_id,
        sub_account_name,
        sub_account_type,

        -- Raw tags column
        tags,

        -- Extracted plotly tags
        JSON_EXTRACT_SCALAR(tags, '$.plotly_team') AS tag_plotly_team,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_department') AS tag_plotly_department,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_owner') AS tag_plotly_owner,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_type') AS tag_plotly_type,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_usage') AS tag_plotly_usage,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_environment') AS tag_plotly_environment,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_resource_type') AS tag_plotly_resource_type,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_group') AS tag_plotly_group,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_creation_date') AS tag_plotly_creation_date,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_expiration_date') AS tag_plotly_expiration_date,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_trigger_by') AS tag_plotly_trigger_by,
        JSON_EXTRACT_SCALAR(tags, '$.plotly_project') AS tag_plotly_project,

        -- Azure-specific x_ columns
        x_account_id,
        x_account_name,
        x_account_owner_id,
        x_billed_cost_in_usd,
        x_billed_unit_price,
        x_billing_account_id,
        x_billing_account_name,
        x_billing_exchange_rate,
        x_billing_exchange_rate_date,
        x_billing_profile_id,
        x_billing_profile_name,
        x_contracted_cost_in_usd,
        x_cost_allocation_rule_name,
        x_cost_center,
        x_customer_id,
        x_customer_name,
        x_effective_cost_in_usd,
        x_effective_unit_price,
        x_invoice_id,
        x_invoice_issuer_id,
        x_invoice_section_id,
        x_invoice_section_name,
        x_list_cost_in_usd,
        x_partner_credit_applied,
        x_partner_credit_rate,
        x_pricing_block_size,
        x_pricing_currency,
        x_pricing_subcategory,
        x_pricing_unit_description,
        x_publisher_category,
        x_publisher_id,
        x_reseller_id,
        x_reseller_name,
        x_resource_group_name,
        x_resource_type,
        x_service_period_end,
        x_service_period_start,
        x_sku_description,
        x_sku_details,
        x_sku_is_credit_eligible,
        x_sku_meter_category,
        x_sku_meter_id,
        x_sku_meter_name,
        x_sku_meter_subcategory,
        x_sku_offer_id,
        x_sku_order_id,
        x_sku_order_name,
        x_sku_part_number,
        x_sku_region,
        x_sku_service_family,
        x_sku_term,
        x_sku_tier

    FROM staging
)

SELECT * FROM final
