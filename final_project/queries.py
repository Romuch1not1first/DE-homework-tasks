populate_gold_user_profile_enriched = """
        CREATE OR REPLACE TABLE gold.user_profiles_enriched AS
        SELECT
            c.client_id,
            COALESCE(SPLIT(p.full_name, ' ')[OFFSET(0)], c.first_name) AS first_name,
            COALESCE(SPLIT(p.full_name, ' ')[OFFSET(1)], c.last_name) AS last_name,
            c.email,
            c.registration_date,
            COALESCE(p.state, c.state) AS state,
            p.birth_date,
            p.phone_number
        FROM
            silver.customers c
        LEFT JOIN
            silver.user_profiles p
        ON
            c.email = p.email;
"""

populate_silver_customers = '''
        CREATE OR REPLACE TABLE silver.customers AS
        SELECT
            CAST(ClientId AS STRING) AS client_id,
            CAST(FirstName AS STRING) AS first_name,
            CAST(LastName AS STRING) AS last_name,
            CAST(Email AS STRING) AS email,
            CAST(RegistrationDate AS TIMESTAMP) AS registration_date,
            CAST(State AS STRING) AS state
        FROM bronze.customers
'''

process_sales_to_silver = '''
        CREATE OR REPLACE TABLE silver.sales AS
        SELECT
            CAST(CustomerId AS STRING) AS client_id,
            CASE
                WHEN SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate) IS NOT NULL 
                    THEN SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate)
                WHEN SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate) IS NOT NULL 
                    THEN SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate)
                ELSE NULL
            END AS purchase_date,
            CAST(Product AS STRING) AS product_name,
            CAST(Price AS FLOAT64) AS price
        FROM bronze.sales
        WHERE SAFE_CAST(Price AS FLOAT64) IS NOT NULL
          AND (SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate) IS NOT NULL 
          OR SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate) IS NOT NULL)
'''