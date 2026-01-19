-- Define expected records counts for each table

{% set expected_counts = {
    'customers':  50,
    'employees': 20,
    'orders': 1000,
    'orderitems': 5000,
    'products': 100,
    'stores': 10,
    'suppliers': 5
} %}

-- Test the count of records in each table

{% for table, expected_count in expected_counts.items() %}

    select '{{ table }}' as table_name,
    (select count(*) from {{ source('landing', table) }}) as record_count,
    {{ expected_count }} as expected_count
    where (select count(*) from {{ source('landing', table) }}) < {{ expected_count }}
    {% if not loop.last %} union all {% endif %}

{% endfor %}