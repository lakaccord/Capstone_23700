{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='employee_id'
) }}


WITH snapshot_employees AS (

    SELECT

        employee_id,
        last_modified_date,
        raw_employee_data,

        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,

        DBT_VALID_FROM,
        DBT_VALID_TO

    FROM {{ ref('bronze_employees') }}

    /*
        The snapshot contains historical versions of employees.

        Only retain the currently active version of each employee.
    */
    WHERE DBT_VALID_TO IS NULL

    {% if is_incremental() %}

        AND last_modified_date >= (
            SELECT COALESCE(
                MAX(last_modified_date),
                '1900-01-01'::TIMESTAMP_NTZ
            )
            FROM {{ this }}
        )

    {% endif %}

),


/*
    CLEAN + STANDARDIZE EMPLOYEE ATTRIBUTES
*/
cleaned AS (

    SELECT

        /*
            EMPLOYEE ID
        */
        NULLIF(
            TRIM(
                employee_id
            ),
            ''
        ) AS employee_id,


        /*
            FIRST NAME
        */
        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    raw_employee_data:first_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS first_name,


        /*
            LAST NAME
        */
        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    raw_employee_data:last_name::VARCHAR
                ),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS last_name,


        /*
            EMAIL

            Valid emails are standardized to lowercase.
            Invalid emails are converted to NULL.
        */
        CASE
            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        raw_employee_data:email::VARCHAR
                    )
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN LOWER(
                TRIM(
                    raw_employee_data:email::VARCHAR
                )
            )
            ELSE NULL
        END AS email,


        /*
            INVALID EMAIL FLAG
        */
        CASE
            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        raw_employee_data:email::VARCHAR
                    )
                ),
                '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$',
                'i'
            )
            THEN FALSE
            ELSE TRUE
        END AS invalid_email_flag,


        /*
            PHONE

            US phone number normalization.

            Accepted:
                10 digits
                11 digits beginning with 1

            Output:
                (XXX) XXX-XXXX
        */
        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    1,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    4,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    7,
                    4
                )
            )


            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN CONCAT(
                '(',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    2,
                    3
                ),
                ') ',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                        ),
                        '[^0-9]',
                        ''
                    ),
                    5,
                    3
                ),
                '-',
                SUBSTR(
                    REGEXP_REPLACE(
                        TRIM(
                            raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                    ),
                    8,
                    4
                )
            )

            ELSE NULL

        END AS phone,


        /*
            INVALID PHONE FLAG
        */
        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN FALSE


            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        raw_employee_data:phone::VARCHAR
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN FALSE


            ELSE TRUE

        END AS invalid_phone_flag,


        /*
            JOB TITLE

            Source JSON field = role
        */
        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    raw_employee_data:role::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS job_title,


        /*
            DEPARTMENT
        */
        INITCAP(
            REGEXP_REPLACE(
                TRIM(
                    raw_employee_data:department::VARCHAR
                ),
                '[^A-Za-z0-9 ''&/-]',
                ''
            )
        ) AS department,


        /*
            STORE ID

            Source JSON field = work_location
        */
        NULLIF(
            TRIM(
                raw_employee_data:work_location::VARCHAR
            ),
            ''
        ) AS store_id,


        /*
            HIRE DATE
        */
        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    raw_employee_data:hire_date::VARCHAR
                ),
                ''
            )
        ) AS hire_date,


        /*
            SALARY
        */
        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    raw_employee_data:salary::VARCHAR
                ),
                ''
            ),
            18,
            2
        ) AS salary,


        /*
            LAST MODIFIED DATE

            Keep timestamp precision rather than converting
            the value to DATE.
        */
        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(
                    last_modified_date::VARCHAR
                ),
                ''
            )
        ) AS last_modified_date,


        /*
            PIPELINE METADATA
        */
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,


        /*
            SNAPSHOT METADATA
        */
        DBT_VALID_FROM,
        DBT_VALID_TO

    FROM snapshot_employees

),


/*
    DERIVED ATTRIBUTES
*/
derived AS (

    SELECT

        e.*,

        /*
            FULL NAME
        */
        TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(e.first_name, ''),
                NULLIF(e.last_name, '')
            )
        ) AS full_name

    FROM cleaned e

),


/*
    FINAL SILVER EMPLOYEE TABLE
*/
final AS (

    SELECT

        /*
            EMPLOYEE ID
        */
        employee_id,


        /*
            PERSONAL INFORMATION
        */
        first_name,
        last_name,
        full_name,
        email,
        invalid_email_flag,
        phone,
        invalid_phone_flag,


        /*
            EMPLOYMENT INFORMATION
        */
        job_title,
        department,
        store_id,
        hire_date,
        salary,
        last_modified_date,


        /*
            PIPELINE METADATA
        */
        SOURCE_FILE,
        LOADED_AT,
        BATCH_ID,


        /*
            SNAPSHOT METADATA
        */
        DBT_VALID_FROM,
        DBT_VALID_TO

    FROM derived

)


SELECT *

FROM final
