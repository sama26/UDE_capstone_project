class SqlQueries:
    test_result_table_insert = ("""
        SELECT
            test_id,
            vehicle_id,
            test_date,
            test_class_id,
            test_type,
            test_result,
            test_mileage,
            postcode_area
        FROM staging_results
        WHERE test_mileage IS NOT NULL
    """)

    test_item_table_insert = ("""
        SELECT *
        FROM staging_items
    """)

    vehicle_table_insert = ("""
        SELECT
            vehicle_id,
            make,
            model,
            colour,
            fuel_type,
            cylinder_capacity,
            first_use_date
        FROM staging_results
        WHERE cylinder_capacity IS NOT NULL
        AND first_use_date IS NOT NULL
        """)

    test_item_table_update = ("""
        UPDATE test_item_table
        SET dangerous_mark = 'N'
        WHERE dangerous_mark IS Null
        """)
