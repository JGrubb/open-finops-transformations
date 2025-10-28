WITH staging AS (
    SELECT * FROM {{ ref('stg_aws_billing__cur_data') }}
),

final AS (
    SELECT

        -- Billing Account
        bill_payer_account_id,
        line_item_usage_account_id,
        billing_period_start_date,

        -- Billing Period
        billing_period_end_date,
        line_item_usage_start_date,
        line_item_usage_end_date,
        line_item_currency_code,
        invoicing_entity,

        invoice_id,
        billing_entity,
        bill_type,
        line_item_type,

        -- Charge Period
        line_item_description,

        -- Charge Metadata
        line_item_tax_type,
        line_item_legal_entity,
        line_item_product_code,
        product_servicename,

        product_servicecode,
        product_name,

        -- Service & Resource
        product_family,
        product_sku,
        line_item_usage_type,
        product_region_code,
        product_region,
        product_location,
        product_location_type,

        -- Location
        line_item_availability_zone,
        product_availability_zone,
        savings_plan_savings_plan_arn,
        -- reservation_reservation_arn,

        savings_plan_offering_type,
        savings_plan_region,
        pricing_term,

        -- Commitment Discounts
        pricing_lease_contract_length,
        pricing_offering_class,
        pricing_purchase_option,
        pricing_currency,
        pricing_rate_code,

        -- Pricing Info
        pricing_rate_id,
        pricing_public_on_demand_rate,
        line_item_unblended_rate,
        line_item_net_unblended_rate,
        line_item_blended_rate,
        savings_plan_savings_plan_rate,
        discount_bundled_discount,
        discount_spp_discount,
        discount_total_discount,
        pricing_unit,
        line_item_usage_amount,
        line_item_normalization_factor,
        line_item_normalized_usage_amount,

        -- Usage Metrics
        line_item_operation,
        product_instance_type,
        product_instance_type_family,
        product_operating_system,
        product_vcpu,
        product_memory,
        product_memory_gib,
        product_storage,
        product_network_performance,
        line_item_net_unblended_cost,

        -- Cost Columns
        line_item_unblended_cost,
        line_item_blended_cost,
        pricing_public_on_demand_cost,
        savings_plan_savings_plan_effective_cost,
        -- reservation_effective_cost,
        -- reservation_net_effective_cost,
        -- reservation_amortized_upfront_cost_for_usage,
        -- reservation_amortized_upfront_fee_for_billing_period,
        -- reservation_recurring_fee_for_usage,
        -- reservation_unused_amortized_upfront_fee_for_billing_period,
        -- reservation_unused_recurring_fee,
        -- reservation_net_amortized_upfront_cost_for_usage,
        -- reservation_net_amortized_upfront_fee_for_billing_period,
        -- reservation_net_recurring_fee_for_usage,
        -- reservation_net_unused_amortized_upfront_fee_for_billing_period,
        -- reservation_net_unused_recurring_fee,
        savings_plan_net_savings_plan_effective_cost,
        savings_plan_used_commitment,
        savings_plan_total_commitment_to_date,
        savings_plan_amortized_upfront_commitment_for_billing_period,
        savings_plan_recurring_commitment_for_billing_period,
        savings_plan_net_amortized_upfront_commitment_for_billing_period,
        savings_plan_net_recurring_commitment_for_billing_period,
        {% for tag in var('aws_billing', {}).get('user_tags', []) %}
        tag_{% if tag is mapping %}{{ tag.alias }}{% else %}{{ tag }}{% endif %},
        {% endfor %}
        {% for tag in var('aws_billing', {}).get('system_tags', []) %}
        tag_aws_{{ tag }},
        {% endfor %}
        identity_line_item_id,

        -- Calculated FOCUS columns (keep these - they have logic)
        identity_time_interval,
        CASE
            WHEN line_item_usage_account_id = '661924842005' THEN 'plotly'
            WHEN
                line_item_usage_account_id = '982534397000'
                THEN 'plotly_cloud_dev'
            WHEN
                line_item_usage_account_id = '641737107127'
                THEN 'plotly_cloud_production'
            WHEN
                line_item_usage_account_id = '442042532868'
                THEN 'plotly_cloud_staging'
            WHEN
                line_item_usage_account_id = '257394486564'
                THEN 'plotly_cs_infra'
            WHEN
                line_item_usage_account_id = '975049956928'
                THEN 'plotly_internal_production'
            WHEN
                line_item_usage_account_id = '565393064395'
                THEN 'plotly_platform_experimental'
            WHEN
                line_item_usage_account_id = '396913729522'
                THEN 'plotly_privatelink_experimental'
            WHEN
                line_item_usage_account_id = '905418453901'
                THEN 'plotly_qa_s3_testing'
            WHEN
                line_item_usage_account_id = '170442856950'
                THEN 'plotly_studio_ai_experimental'
            WHEN
                line_item_usage_account_id = '841162664890'
                THEN 'plotly_telemetry'
            WHEN
                line_item_usage_account_id = '634488302191'
                THEN 'plotly_sales_engineering'
            WHEN
                line_item_usage_account_id = '730335287555'
                THEN 'plotly_internal_tools'
            ELSE CONCAT('unmapped - ', line_item_usage_account_id)
        END AS line_item_usage_account_name,
        CAST(null AS STRING) AS focus_charge_class,
        CAST(null AS FLOAT64) AS focus_contracted_cost,

        -- Vendor Tags: AWS
        CASE
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                THEN 'Usage'
            WHEN
                line_item_type IN ('RIFee', 'SavingsPlanRecurringFee')
                THEN 'Purchase'
            WHEN line_item_type = 'Tax' THEN 'Tax'
            WHEN line_item_type IN ('Credit', 'Refund') THEN 'Credit'
            ELSE line_item_type
        END AS focus_charge_category,
        CASE
            WHEN
                line_item_type IN (
                    'RIFee', 'SavingsPlanRecurringFee', 'SavingsPlanUpfrontFee'
                )
                THEN 'Recurring'
            WHEN
                line_item_type IN (
                    'Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage'
                )
                THEN 'Usage-Based'
            WHEN line_item_type IN ('Tax', 'Credit', 'Refund') THEN 'One-Time'
        END AS focus_charge_frequency,
        COALESCE(reservation_reservation_arn, savings_plan_savings_plan_arn)
            AS focus_commitment_discount_id,

        -- Vendor Tags: Kubernetes
        CASE
            WHEN line_item_type = 'DiscountedUsage' THEN 'Committed'
            WHEN line_item_type = 'SavingsPlanCoveredUsage' THEN 'Committed'
            WHEN line_item_usage_type LIKE '%SpotUsage%' THEN 'Dynamic'
            WHEN line_item_type = 'Usage' THEN 'Standard'
        END AS focus_pricing_category,
        CASE
            WHEN line_item_type = 'DiscountedUsage' THEN 'Reservation'
            WHEN line_item_type = 'SavingsPlanCoveredUsage' THEN 'Savings Plan'
            WHEN line_item_usage_type LIKE '%SpotUsage%' THEN 'Spot'
            WHEN line_item_type = 'Usage' THEN 'On Demand'
        END AS finops_pricing_model,
        CASE
            -- Reservable: Compute instances (EC2, ECS, Lambda)
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                AND line_item_product_code IN (
                    'AWSLambda', 'AmazonEC2', 'AmazonECS'
                )
                AND product_family IN (
                    'Compute Instance',
                    'Compute Instance (bare metal)',
                    'Serverless',
                    'Compute'
                )
                AND (
                    line_item_operation IN (
                        'RunInstances', 'Invoke', 'FargateTask'
                    )
                    OR line_item_usage_type LIKE '%BoxUsage%'
                    OR line_item_usage_type LIKE '%EBSOptimized%'
                )
                AND line_item_usage_type NOT LIKE '%SpotUsage%'
                THEN 'Reservable'

            -- Reservable: Database instances (RDS, Redshift, ElastiCache, OpenSearch)
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                AND product_family IN (
                    'Database Instance',
                    'Cache Instance',
                    'Compute Instance (bare metal)',
                    'Amazon OpenSearch Service Instance'
                )
                THEN 'Reservable'

            -- Reservable: Redshift compute nodes
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                AND line_item_product_code = 'AmazonRedshift'
                AND line_item_usage_type LIKE '%Node:%'
                THEN 'Reservable'

            -- Non-Reservable: Spot instances (dynamic pricing)
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                AND line_item_usage_type LIKE '%SpotUsage%'
                THEN 'Non-Reservable'

            -- Non-Reservable: Everything else that is usage
            WHEN
                line_item_type IN (
                    'DiscountedUsage', 'SavingsPlanCoveredUsage', 'Usage'
                )
                THEN 'Non-Reservable'
        END AS finops_usage_class,
        CASE
            -- Tax category (always check first)
            WHEN line_item_type = 'Tax'
                THEN 'Tax'

            -- Container Platform (before Compute to catch EKS management fees)
            WHEN
                line_item_product_code = 'AmazonEKS'
                AND line_item_operation IN (
                    'CreateOperation',
                    'EKSAutoUsage',
                    'ExtendedSupport'
                )
                THEN 'Container Platform'

            -- ALL product_family checks FIRST (to avoid conflicts with fallback logic)
            -- Compute
            WHEN
                product_family IN (
                    'Compute Instance',
                    'Compute Instance (bare metal)',
                    'Compute',
                    'ComputeSavingsPlans',
                    'CPU Credits',
                    'Serverless'
                )
                THEN 'Compute'

            -- Databases
            WHEN
                product_family IN (
                    'Database Instance',
                    'Database Storage',
                    'Database Utilization',
                    'Cache Instance',
                    'ElastiCache Serverless',
                    'RDSProxy',
                    'Amazon OpenSearch Service Instance',
                    'Amazon OpenSearch Service Volume',
                    'Amazon DynamoDB PayPerRequest Throughput'
                )
                THEN 'Databases'

            -- Storage
            WHEN
                product_family IN (
                    'Storage',
                    'Storage Snapshot',
                    'Provisioned IOPS',
                    'Provisioned Throughput',
                    'EC2 Container Registry',
                    'AWS Backup Storage',
                    'Redshift Managed Storage'
                )
                THEN 'Storage'

            -- Networking
            WHEN
                product_family IN (
                    'NAT Gateway',
                    'Load Balancer',
                    'Load Balancer-Application',
                    'Load Balancer-Network',
                    'VpcEndpoint',
                    'DNS Query',
                    'DNS Zone',
                    'AWS Firewall',
                    'Web Application Firewall',
                    'Private Networks'
                )
                THEN 'Networking'

            -- Bandwidth (must be before Storage/Compute fallback logic)
            WHEN
                product_family IN (
                    'Data Transfer',
                    'Data Payload'
                )
                THEN 'Bandwidth'

            -- Management and Governance
            WHEN
                product_family IN (
                    'System Operation',
                    'Metric',
                    'Metric Streams',
                    'Alarm',
                    'Dashboard',
                    'Management Tools - AWS Config',
                    'Management Tools - AWS CloudTrail Paid Events Recorded',
                    'Management Tools - AWS CloudTrail Data Events Recorded',
                    'Management Tools - AWS CloudTrail Free Events Recorded',
                    'AWS Systems Manager'
                )
                THEN 'Management and Governance'

            -- Security
            WHEN
                product_family IN (
                    'GuardDuty',
                    'Amazon Inspector',
                    'Findings',
                    'Secret',
                    'Encryption Key',
                    'ACM'
                )
                THEN 'Security'

            -- Data and Analysis
            WHEN
                product_family IN (
                    'Kinesis Firehose',
                    'AWS Glue',
                    'Athena Queries'
                )
                THEN 'Data and Analysis'

            -- API Operations (must be before other fallback logic)
            WHEN
                product_family IN (
                    'API Request',
                    'API Calls',
                    'Request'
                )
                THEN 'API Operations'

            -- Support
            WHEN product_family = 'AWSSupportBusiness'
                THEN 'Support'

            -- Fee/Other
            WHEN product_family = 'Fee'
                THEN 'Other'

            -- NOW fallback logic for rows without product_family or with unmapped values
            -- Compute fallback
            WHEN
                line_item_product_code IN (
                    'AmazonEC2', 'AmazonECS', 'AmazonEKS', 'AWSLambda',
                    'AmazonElastiCache', 'AmazonRedshift'
                )
                AND (
                    line_item_usage_type LIKE '%BoxUsage%'
                    OR line_item_usage_type LIKE '%SpotUsage%'
                    OR line_item_usage_type LIKE '%Fargate%'
                    OR line_item_operation IN (
                        'RunInstances', 'FargateTask', 'Invoke'
                    )
                )
                THEN 'Compute'

            -- Databases fallback
            WHEN
                line_item_product_code IN (
                    'AmazonRDS', 'AmazonDynamoDB', 'AmazonDocumentDB'
                )
                THEN 'Databases'

            -- Storage fallback
            WHEN
                line_item_product_code IN (
                    'AmazonS3', 'AmazonEBS', 'AmazonEFS', 'AmazonFSx'
                )
                THEN 'Storage'
            WHEN
                line_item_usage_type LIKE '%VolumeUsage%'
                OR line_item_usage_type LIKE '%Snapshot%'
                OR line_item_usage_type LIKE '%TimedStorage%'
                THEN 'Storage'

            -- Networking fallback
            WHEN
                line_item_product_code IN (
                    'AmazonVPC', 'AWSELB', 'AmazonRoute53',
                    'AmazonCloudFront', 'AWSNetworkFirewall'
                )
                THEN 'Networking'

            -- Bandwidth fallback (only for rows without product_family)
            WHEN
                line_item_usage_type LIKE '%DataTransfer%'
                OR line_item_usage_type LIKE '%Out-Bytes%'
                OR line_item_usage_type LIKE '%In-Bytes%'
                OR line_item_usage_type LIKE '%Egress%'
                THEN 'Bandwidth'

            -- Management and Governance fallback
            WHEN
                line_item_product_code IN (
                    'AmazonCloudWatch', 'AWSCloudTrail',
                    'AWSConfig', 'AWSSystemsManager'
                )
                THEN 'Management and Governance'

            -- Security fallback
            WHEN
                line_item_product_code IN (
                    'AmazonGuardDuty', 'AmazonInspector', 'AWSWAF',
                    'AWSSecretsManager', 'awskms', 'AWSCertificateManager'
                )
                THEN 'Security'

            -- AI and Machine Learning fallback
            WHEN
                line_item_product_code IN (
                    'AmazonBedrock', 'AmazonSageMaker',
                    'AmazonRekognition', 'AmazonComprehend'
                )
                THEN 'AI and Machine Learning'

            -- Default to Other for anything not categorized
            ELSE 'Other'
        END AS finops_usage_category,

        -- Metadata
        REGEXP_EXTRACT(product_instance_type, r'^[a-z0-9]+')
            AS finops_instance_family,
        SAFE_MULTIPLY(line_item_usage_amount, CAST(product_vcpu AS FLOAT64))
            AS finops_line_item_cpu_hours,
        CASE
            -- Reserved Instance usage
            WHEN line_item_type = 'DiscountedUsage'
                THEN
                    COALESCE(
                        reservation_net_effective_cost,
                        reservation_effective_cost
                    )
            -- Savings Plan covered usage
            WHEN line_item_type = 'SavingsPlanCoveredUsage'
                THEN
                    CASE
                        -- Use effective DoIT discount of 27% off list.
                        WHEN line_item_usage_start_date BETWEEN TIMESTAMP('2024-08-29') AND TIMESTAMP('2025-10-10')
                            THEN pricing_public_on_demand_cost * 0.73
                        -- After Oct 10, 2025: Use standard SP effective cost
                        ELSE
                            COALESCE(
                                savings_plan_net_savings_plan_effective_cost,
                                savings_plan_savings_plan_effective_cost
                            )
                    END
            -- Reserved Instance fees (unused amortized upfront + recurring)
            WHEN line_item_type = 'RIFee'
                THEN
                    COALESCE(
                        reservation_net_unused_amortized_upfront_fee_for_billing_period,
                        0
                    )
                    + COALESCE(reservation_net_unused_recurring_fee, 0)
            -- Savings Plan recurring commitment fee
            WHEN line_item_type = 'SavingsPlanRecurringFee'
                THEN
                    COALESCE(savings_plan_total_commitment_to_date, 0)
                    - COALESCE(savings_plan_used_commitment, 0)
            -- Savings Plan negation and upfront fees should be 0 to avoid double counting
            WHEN
                line_item_type IN (
                    'SavingsPlanNegation', 'SavingsPlanUpfrontFee'
                )
                THEN 0
            -- Standard usage, taxes, credits, refunds use net unblended cost
            WHEN line_item_type IN ('Usage', 'Tax', 'Credit', 'Refund')
                THEN
                    COALESCE(
                        line_item_net_unblended_cost, line_item_unblended_cost
                    )
            ELSE 0
        END AS focus_effective_cost

    FROM staging
)

SELECT * FROM final
