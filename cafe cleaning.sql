# Synthetic Cafe Data set is from: https://www.kaggle.com/datasets/ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training/data
# The data set contains information for orders such as item sold, quantity, unit price, total spent, payment method and transaction date.

# MySQL's table data import wizard was taking really long for some reason. So I imported the data manually.
create table dirty_cafe_sales(
	`Transaction ID` text,
    Item text,
    Quantity int,
    `Price Per Unit` double,
    `Total Spent` double,
    `Payment Method` text,
    Location text,
    `Transaction Date` text
);
# We will alter the transaction date to be of type date later on. Importing this column as a date didn't seem to work.

LOAD DATA LOCAL INFILE 'dirty_cafe_sales.csv'
INTO TABLE dirty_cafe_sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

# I created a copy of the data table so I wouldn't have to re-import the data if something went wrong.
create table dirty_cafe_sales_copy as select * from dirty_cafe_sales;
select * from dirty_cafe_sales_copy limit 10;

# I checked to see if any transaction ids don't match the general pattern. They all fit.
select `transaction id` from dirty_cafe_sales_copy
where `transaction id` not like 'TXN________' limit 20;

# I looked for duplicate transaction ids. There are none.
select `transaction id` from dirty_cafe_sales_copy
group by `transaction id`
having count(`transaction id`) > 1 
limit 10;

# Standardizing the categorical columns:

# The item column has '', ERROR and UNKNOWN as values. We'll turn these all into nulls.
select distinct item from dirty_cafe_sales_copy;

update dirty_cafe_sales_copy
set item = null
where item in ('', 'ERROR', 'UNKNOWN');

# We'll do the same thing for the location and payment method columns.
select distinct location from dirty_cafe_sales_copy;

update dirty_cafe_sales_copy
set location = NULL
where location in ('ERROR', '', 'UNKNOWN');

select distinct `payment method` from dirty_cafe_sales_copy;

update dirty_cafe_sales_copy
set `payment method` = null
where `payment method` in ('ERROR', '', 'UNKNOWN');

# The only date values that don't match the general pattern are also of the form '', ERROR or UNKOWN.
select distinct `transaction date` from dirty_cafe_sales_copy
where `transaction date` not like '____-__-__';

# We'll set all of these error values to be null.
update dirty_cafe_sales_copy
set `transaction date` = null
where `transaction date` not like '____-__-__';

# Now we change change the transaction date column to be of type date.
alter table dirty_cafe_sales_copy
modify column `transaction date` date;

# Standardizing numeric columns:

# We have 0 as a unit price for some rows. We'll be able to fix this using values in other columns for some cases.
select distinct `price per unit` from dirty_cafe_sales_copy;

# If we know the item for the row, then we can determine what the unit price should be if the unit price is recorded as zero.
select item, `price per unit` from dirty_cafe_sales_copy
group by item, `price per unit`
order by item desc
limit 20;

# To do this we'll create a temporary table that has all the listed items and their prices.
create temporary table cafe_menu (
	`item` text,
    `price` double
);

insert cafe_menu select * from (
select distinct item, `price per unit` from dirty_cafe_sales_copy
where item is not null and `price per unit` > 0) as dummy;

# We don't end up with any price conflicts, so our table has all items and their unit prices.
# And there are no items in the data set with multiple prices that we would have try to fix.
select * from cafe_menu;

# This is just to make sure the join is done properly.
select dirty_cafe_sales_copy.item, `price per unit`, price 
from dirty_cafe_sales_copy
join cafe_menu on dirty_cafe_sales_copy.item = cafe_menu.item
where `price per unit` = 0
limit 5;

# Now we can fix any unit prices recorded as zero where the item is known.
update dirty_cafe_sales_copy
join cafe_menu on dirty_cafe_sales_copy.item = cafe_menu.item
set `Price Per Unit` = price
where `price per unit` = 0;

# And if we know both the quantity and total spent for the order, we can calculate the unit price as total = quantity * price.
update dirty_cafe_sales_copy
set `price per unit` = `total spent` / quantity
where `total spent` <> 0 and quantity <> 0;

# We can determine null items if we know the unit price (provided the price has a unique item associated with it). From our temporary table,
# we see that the only price with multiple items is 4 (it is the price for both Smoothies and Sandwiches).
select dirty_cafe_sales_copy.item, `price per unit`, price 
from dirty_cafe_sales_copy
join cafe_menu on `price per unit` = price
where dirty_cafe_sales_copy.item is null and price <> 4
limit 10;

update dirty_cafe_sales_copy
join cafe_menu on `price per unit` = price
set dirty_cafe_sales_copy.item = cafe_menu.item
where dirty_cafe_sales_copy.item is null and price <> 4;

# We have some quantities with a value of zero.
select distinct quantity from dirty_cafe_sales_copy;

# We can determine quantities with zero values if the corresponding unit price and total spent are non-zero.
update dirty_cafe_sales_copy
set quantity = `total spent` / `price per unit`
where `total spent` <> 0 and `price per unit` <> 0;

# And we can do basically the same thing with total spent.
select distinct `total spent` from dirty_cafe_sales_copy;

update dirty_cafe_sales_copy
set `total spent` = quantity * `price per unit`
where quantity <> 0 and `price per unit` <> 0;

# We can't determine the remaining unknown numeric data.
select item, quantity, `price per unit`, `total spent` from dirty_cafe_sales_copy
where quantity = 0 or `price per unit` = 0 or `total spent` = 0
limit 10;

# So we'll set them all to be null.
update dirty_cafe_sales_copy
set quantity = null
where quantity = 0;

update dirty_cafe_sales_copy
set `price per unit` = null
where `price per unit` = 0;

update dirty_cafe_sales_copy
set `total spent` = null
where `total spent` = 0;

# Some data exploration:

# Now we can look at which items were the most poplular. The top 3 most purchased items were Cake, Coffee and Salad.
select item, sum(quantity) as quantities_sold from dirty_cafe_sales_copy
group by item
order by quantities_sold desc;

# If we look at an item's total revenue instead, we find that Salads, Sandwiches and Smoothies bring in the most money.
select item, sum(`total spent`) as revenue from dirty_cafe_sales_copy
group by item
order by revenue desc;

# We may be interested in the most common payment methods. Digital Wallet, Credit Card and Cash all have fairly similar counts with 
# Digital Wallet slightly ahead the other two.
select `payment method`, count(`payment method`) as payment_method_count from dirty_cafe_sales_copy
group by `payment method`
order by  payment_method_count desc;

# For location, takeaway is more common than in-store but not by much.
select location, count(location) as location_count from dirty_cafe_sales_copy
group by location
order by  location_count desc;

# We can see if any days of the week have significantly more orders than other days. They seem to be relatively uniform with
# Saturday, Wednesday and Tuesday having a bit less than the other days.
with week_day_cte as (
select dayname(`transaction date`) as week_day from dirty_cafe_sales_copy
)
select week_day, count(week_day) as day_count from week_day_cte
group by week_day
order by day_count desc
;
