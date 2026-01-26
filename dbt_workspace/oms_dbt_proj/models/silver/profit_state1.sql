{{ generate_profit_model("
    select c.state 
    from customers c 
    order by state asc limit 1 ") }}