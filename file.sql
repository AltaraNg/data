SELECT showroom_name, `Product Bought`, order_date, monthly_gains, `ID`, CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name, 
  employment_status, gender, date_of_birth, civil_status, occupation, year_together, type_of_home, 
  duration_of_residence, depend_on_you, level_of_education, number_of_children, `business type`, 
  `Category`, actual_payment_date, actual_amount, `no repayments`, `expected repayments`, 
  `total repayment`, `Downpayment`, `Amount Owing`, `last date of payment`, `last_exp_date`, 
  `missed days`, DATEDIFF(`last_exp_date`, `last date of payment`) AS `days_btw_last_payment`,
  CASE 
        WHEN (`no repayments` < `expected repayments` AND `last_exp_date` < CURDATE()) THEN 1 
        ELSE 0 
    END AS `default_status`
FROM (
  SELECT branches.name AS 'showroom_name', products.name 'Product Bought', order_date, monthly_gains,
    customers.id AS 'ID', customers.first_name, customers.middle_name, customers.last_name, 
    customers.employment_status, gender, date_of_birth, civil_status, occupation, year_together, 
    type_of_home, duration_of_residence, depend_on_you, level_of_education, number_of_children, 
    business_types.name as 'business type', order_types.name AS 'Category', 
    amortizations.actual_payment_date, amortizations.actual_amount, 
    COUNT(amortizations.actual_payment_date) AS 'no repayments', 
    COUNT(amortizations.expected_amount) AS 'expected repayments', 
    SUM(expected_amount) AS "total repayment", new_orders.down_payment AS "Downpayment", 
    (SUM(CASE WHEN expected_payment_date > CURDATE() THEN expected_amount ELSE 0 END) - 
      SUM(CASE WHEN expected_payment_date < CURDATE() THEN actual_amount ELSE 0 END)) AS "Amount Owing", 
    max(actual_payment_date) as 'last date of payment', max(expected_payment_date) as last_exp_date, 
    datediff(max(actual_payment_date), expected_payment_date) as 'missed days', 
    new_orders.id AS new_order_id
  FROM altaraone.customers
  INNER JOIN altaraone.new_orders ON new_orders.customer_id = customers.id
  INNER JOIN altaraone.products ON new_orders.product_id = products.id  
  INNER JOIN altaraone.order_types ON new_orders.order_type_id = order_types.id
  INNER JOIN altaraone.business_types on business_types.id = new_orders.business_type_id  
  INNER JOIN altaraone.sales_categories ON new_orders.sales_category_id = sales_categories.id
  INNER JOIN altaraone.amortizations ON amortizations.new_order_id = new_orders.id
  INNER JOIN altaraone.branches ON new_orders.branch_id = branches.id
  WHERE products.name LIKE "Cash%"
  GROUP BY new_order_id
) AS subquery