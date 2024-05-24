# Multinational-Retail-Data-Centralisation

## Table of Contents
- [Description](#description)
- [Installation](#installation)
- [Usage](#usage)
- [Business Insights](#Business_Insights)
- [File Structure](#file-structure)
- [License](#license)

## Description

### Overview
The **Multinational Retail Data Centralisation** project aims to streamline and centralize the sales data of a global retail company. With sales data dispersed across multiple sources including PDF documents, AWS RDS databases, RESTful APIs, JSON, and CSV files, the objective was to integrate these data points into a single, accessible PostgreSQL database. This centralization facilitates efficient data management, accurate business insights, and data-driven decision-making.

### Aim of the Project
The primary goal of this project was to design and implement a robust system that consolidates disparate sales data into a unified database. This system serves as the single source of truth for the company's sales metrics, ensuring consistency and reliability in data reporting and analysis. Additionally, the project aimed to enhance data accessibility, improve query performance, and provide valuable business insights through complex SQL queries.

### Key Features
- **Data Extraction**: Developed a system to extract sales data from five distinct sources: PDF documents, AWS RDS database, RESTful API, JSON, and CSV files.
- **Data Cleaning and Transformation**: Created custom Python scripts to clean and transform over 120,000 rows of data, ensuring data quality and consistency before loading it into the database.
- **Database Design**: Designed a star-schema database with five dimension tables, optimizing the database for rapid querying and efficient data storage.
- **Query Performance**: Achieved data querying times with less than 1 millisecond latency, demonstrating high performance and scalability.
- **Business Insights**: Designed and implemented over 10 complex SQL queries to extract actionable business insights, such as sales velocity, yearly revenue, and regional sales performance.

### What I Learned
This project provided valuable experience in various aspects of data engineering and analytics:
- **Data Integration**: Gained expertise in integrating and consolidating data from multiple heterogeneous sources.
- **Python Scripting**: Enhanced skills in Python programming for data cleaning, transformation, and automation.
- **Database Design**: Learned the principles of designing efficient, high-performance databases using a star-schema architecture.
- **SQL Proficiency**: Developed advanced SQL querying techniques to derive meaningful insights from complex datasets.
- **Performance Optimization**: Implemented strategies to optimize database performance and reduce query latency.

This project not only improved my technical skills in data engineering and analytics but also reinforced the importance of centralized data systems in driving business intelligence and informed decision-making.

### Technology
The project leveraged a variety of technologies and tools to achieve its objectives:

- **Python**: For data extraction, transformation, and loading (ETL) processes.
- **PostgreSQL**: As the central database for storing cleaned and structured data.
- **pandas**: For data manipulation and analysis during the transformation phase.
- **AWS**: Specifically AWS S3 for storage and AWS RDS for database management.
- **PGAdmin**: For database administration and query execution.


## Installation
TBC 
## Usage
TBC
## Business Insights

### 1. Countries with the Most Stores

**Objective:**
We would like to know which countries we are operating in and which country has the most stores.

**SQL Query:**
```sql
SELECT country_code AS country,
       COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;
```

**Explanation:**

This query retrieves the number of stores in each country, utilizing the dim_store_details table. By counting the number of stores and grouping by country_code, we can identify which country has the highest number of stores. The results are ordered to display the country with the most stores first.

**Result:**

country	total_no_stores
GB	265
DE	141
US	34
[Image will be added.png]

**Analysis and Recommendations:**

**Analysis:**

The query results show that Great Britain (GB) has the most stores with 265, followed by Germany (DE) with 141 stores, and the United States (US) with 34 stores. This distribution indicates a strong market presence and brand recognition in Great Britain.

**Recommendations:**

**Maintain and Optimize Operations in GB:**
- Focus on maintaining high service standards and optimizing store operations to sustain and grow market share in GB.
- Leverage data analytics to identify high-performing stores and replicate successful strategies across other locations.

**Strategic Expansion in DE and US:**
- With fewer stores in Germany and the US, there is an opportunity for strategic expansion.
- Conduct market research to understand customer preferences and market conditions in these regions to tailor expansion efforts.

**Product Localization:**
- Consider localizing product offerings based on regional preferences to enhance customer satisfaction and drive sales.
- Implement region-specific marketing campaigns to attract local customers and increase brand loyalty.


### 2. Location with the most stores

**Objective:**  Identifying Which locations have the highest concentration of stores, so we can make informed decisions on potential store closures and future expansion.

**SQL Query:**
```sql
SELECT locality,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC;
```

**Explanation:**

In this query, we determine which locality has the highest number of stores by counting the number of stores in each locality from the `dim_store_details` table. Grouping by locality and ordering by the count in descending order reveals the locality with the most stores.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

**Analysis:**

The data reveals that Chapletown has the highest number of stores (14), followed by Belper (13), and Bushley (12). These high-density areas might be experiencing market saturation, leading to diminishing returns on investment.

**Recommendations:**

**Evaluate Store Performance:**
- Conduct a detailed performance review of stores in these high-density areas to identify underperforming locations.
- Use key performance indicators (KPIs) such as sales per square foot, customer footfall, and profit margins to assess store viability.

**Consider Store Consolidation:**
- If certain stores are underperforming, consider consolidating them to optimize operational efficiency and reduce costs.
- Reinforce marketing efforts to draw customers from consolidated stores to remaining ones.

**Reinvest Savings:**
- Utilize savings from consolidations to invest in new locations with higher growth potential.
- Enhance the in-store experience in high-performing areas to increase customer satisfaction and loyalty.

### 3. Months with the largest sales

**Objective:**
Identifying which months have produced the most sales.

**SQL Query:**
```sql
SELECT 
    dates.month,
    ROUND(SUM((products."product_price(£)" * orders.product_quantity)::numeric), 2) AS total_sales
FROM
    orders_table orders
JOIN
    dim_date_times dates ON orders.date_uuid = dates.date_uuid
JOIN
    dim_products products ON orders.product_code = products.product_code
GROUP BY
    dates.month
ORDER BY
    total_sales DESC;
```

**Explanation:**

This query calculates the total sales for each month by joining the `orders_table`, `dim_date_times`, and `dim_products` tables. The product price is multiplied by the quantity sold to get the total sales. By grouping the results by month and summing the sales, we can identify which months have the highest sales.
We use `::numeric` to ensure precise arithmetic operations and `ROUND` to format the total sales to two decimal places.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

**Analysis:**

The results indicate that August (month 8) has the highest sales, followed closely by January (month 1) and October (month 10). This trend suggests seasonal peaks in sales during these months, which could be driven by factors such as holiday shopping, promotional events, and seasonal demand variations

**Recommendations:**

**Optimize Inventory Levels:**
- Increase inventory for high-demand products in the lead-up to peak sales months to avoid stockouts and maximize sales.
- Use historical sales data to forecast demand and adjust inventory levels accordingly.

**Targeted Marketing Campaigns:**
- Plan and execute targeted marketing campaigns to boost sales during peak months.
- Utilize promotions, discounts, and special offers to attract customers during these high-demand periods.

**Seasonal Product Offerings:**
- Introduce seasonal product offerings and exclusive deals to capitalize on increased consumer spending.
- Align product launches and promotional events with peak sales periods to maximize impact.

### 4. Sales from Online vs Offline Channels

**Objective:**
We need to understand how many sales are happening online versus offline.

**SQL Query:**
```sql
SELECT 
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    CASE 
        WHEN store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
    orders_table
GROUP BY 
    location;
```

**Explanation:**

This query categorizes sales into **'Web'** or **'Offline'** based on the store_code. It then counts the number of sales and sums the product quantities for each category. This is done by grouping the sales from the `orders_table` and using a conditional statement to classify the sales channels.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**


The data shows a significantly higher number of sales and product quantities sold offline compared to online. This indicates a strong reliance on physical store sales despite the growing trend of online shopping.

**Recomendations**

**Enhance Online Presence:**
- Invest in improving the online shopping experience through better website functionality, faster loading times, and a mobile-friendly design.

- Offer features like personalized recommendations, user reviews, and easy returns to enhance customer satisfaction.

**Digital Marketing:**

- Increase digital marketing efforts to attract more online customers.
- Utilize social media, email marketing, search engine optimization (SEO), and pay-per-click (PPC) advertising to drive traffic to the online store.

**Omnichannel Integration:**

- Develop an omnichannel strategy that integrates online and offline sales, providing a seamless shopping experience.
- Implement click-and-collect services, in-store returns for online purchases, and real-time inventory updates to enhance customer convenience.


### 5. Percentage of Sales by Store Type

**Objective:**
We need to determine the total and percentage of sales coming from each of the different store types to identify which store type generates the most revenue.

**SQL Query:**
```sql
WITH orders_store_products_cte AS (
    SELECT 
        s.store_type,
        (o.product_quantity * p."product_price(£)")::numeric AS amount_paid
    FROM
        orders_table o
    JOIN
        dim_products p ON p.product_code = o.product_code
    JOIN
        dim_store_details s ON s.store_code = o.store_code
)
SELECT 
    store_type,
    ROUND(SUM(amount_paid), 2) AS total_sales,
    ROUND((SUM(amount_paid) / SUM(SUM(amount_paid)) OVER ()) * 100, 2) AS percentage_total
FROM 
    orders_store_products_cte
GROUP BY 
    store_type
ORDER BY 
    percentage_total DESC;
```

**Explanation:**

Using a Common Table Expression (CTE), this query first calculates the total sales amount for each store type by joining `orders_table`, `dim_products`, and `dim_store_details`. The CTE simplifies the subsequent query, which calculates the percentage of total sales for each store type.

This query uses Common Table Expression (CTE), orders_store_products_cte, to calculate the total sales amount (amount_paid) for each store type. The CTE joins three tables:

- `orders_table` (order details, including product_quantity and product_code),
- `dim_products` (product prices, joined on product_code),
- `dim_store_details` (store types, joined on store_code).

By joining these tables, we link each order to its product price and store type. The amount_paid is calculated by multiplying product_quantity by product_price(£), cast to numeric for precision.

The main query then aggregates amount_paid by store_type and calculates the percentage of total sales using the `OVER ()` clause. This clause computes the overall sum of amount_paid across all store types, allowing us to determine each store type's sales percentage.
The results are rounded to two decimal places to reflect currency format, ensuring clarity in financial reporting.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

The results show that Local stores contribute the highest percentage of sales (44.87%), followed by Web portals (22.44%), Super Stores (15.63%), Mall Kiosks (8.96%), and Outlets (8.10%). This indicates that Local stores are the primary revenue drivers.

**Recommendations**

**Focus on Local Stores:**

- Given their high contribution to total sales, prioritize maintaining and enhancing the performance of Local stores.
- Implement customer loyalty programs and community engagement initiatives to strengthen customer relationships.

**Strengthen Online Sales:**

- Although Web portal sales are significant, there is room for growth.
- Enhance the online shopping experience and invest in digital marketing strategies to increase online sales.

**Evaluate Super Stores and Malls:**

- Assess the performance of Super Stores and Mall Kiosks to determine their profitability.
- Consider whether consolidating or optimizing these store types could improve overall efficiency and profitability.


### 6. Months with the Highest Sales by Year

**Objective:**
Identifying which months have historically have had the highest sales

**SQL Query:**
```sql
WITH highest_selling_month_cte AS (
    SELECT DISTINCT ON (d.month)
        SUM(ROUND((p."product_price(£)" * o.product_quantity)::numeric, 2)) AS total_sales,
        d.year,
        d.month
    FROM
        orders_table o
    JOIN
        dim_products p ON p.product_code = o.product_code
    JOIN
        dim_date_times d ON d.date_uuid = o.date_uuid
    GROUP BY
        d.year,
        d.month
    ORDER BY
        d.month DESC,
        total_sales DESC
)
SELECT * 
FROM 
    highest_selling_month_cte
ORDER BY
    total_sales DESC;
```

**Explanation:**


Here we uses a Common Table Expression find the highest selling month for each month across all years. It calculates the total sales for each month and year combination by joining three tables:

- `orders_table` (order details, including product_quantity and date_uuid)
- `dim_products` (product prices, joined on product_code)
- `dim_date_times` (date information, joined on date_uuid)

The total sales for each month and year are calculated by multiplying the product price `p."product_price(£)"` by the product quantity `o.product_quantity`, casting the result to numeric for precision, and rounding to two decimal places using `ROUND()`. The `SUM()` function is then used to aggregate the total sales for each month and year combination.

The `DISTINCT ON (d.month)` clause is used to select only the first row for each distinct month value. This is combined with the `ORDER BY` clause inside the CTE, which sorts the results by month in descending order and then by total sales in descending order. 
This ensures that for each month, we get the year with the highest total sales.
Finally, the main query selects all columns from the `highest_selling_month_cte` and orders the results by `total_sales` in descending order, giving us the highest selling month across all years at the top of the result set.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

**Analysis:**

The results highlight the months and years with the highest total sales, indicating periods of strong performance. For instance, the highest sales were recorded in March 1994, January 2019, and August 2009.

**Recommendations:**

**Historical Trend Analysis:**
- Perform a deeper analysis of these peak periods to understand the factors driving high sales, such as successful marketing campaigns, product launches, or seasonal trends.
- Identify and document best practices and strategies from these periods for future reference.

**Replicate Success:**

- Replicate successful strategies and campaigns from high-performing periods in future sales and marketing plans.
- Leverage historical data to plan and execute campaigns during similar periods to maximize sales.

**Stakeholder Assurance:**

- Use this data to assure stakeholders of the company’s ability to achieve high sales, reinforcing confidence in the company’s strategic direction.
- Present detailed reports and analyses to stakeholders, highlighting key success factors and future opportunities.


### 7. Staff Headcount by Country

**Objective:**
Presenting the overall staff number in each location around the world

**SQL Query:**
```sql
SELECT SUM(staff_numbers) AS total_staff_number, 
    country_code 
FROM 
    dim_store_details 
GROUP BY 
    country_code 
ORDER BY 
    total_staff_number DESC;
```

**Explanation:**

We calculate the total number of staff for each country by summing the `staff_numbers` from the `dim_store_details` table. The `GROUP BY` clause is used to group the results by `country_code`, while the `ORDER BY` clause sorts the results by the total staff number in descending order.

**Result:**

[Image will be added.png]

**Analysis and Recommendations:**

**Analysis**:

The data indicates that the majority of the company’s staff is based in Great Britain (GB) with 13,307 employees, followed by Germany (DE) with 6,123 employees, and the United States (US) with 1,384 employees. This distribution suggests a significant operational focus in GB.

**Recommendations:**

**Resource Allocation:**

- Ensure adequate resources and support for the large workforce in GB to maintain operational efficiency and service standards.
- Consider reallocating resources to support growth in regions with lower staff numbers, such as the US.

**Staff Training:**

- Invest in training and development programs to enhance staff skills and productivity across all regions.
- Implement continuous learning initiatives to keep employees updated with the latest industry trends and best practices.

**Strategic Hiring:**

- Plan strategic hiring initiatives in regions with growth potential to support expansion efforts and maintain service standards.
- Focus on hiring skilled professionals who can drive innovation and operational excellence.

### 8. Top-Selling Store Types in Germany

**Objective:**
Which type of store generates the most sales in Germany, so we can make informed decision regarding business expansions.

**SQL Query:**
```sql
SELECT
    SUM(ROUND((o.product_quantity * p."product_price(£)")::numeric, 2)) AS total_sales,
    s.store_type,
    s.country_code
FROM
    orders_table o
JOIN
    dim_products p ON p.product_code = o.product_code
JOIN
    dim_store_details s ON s.store_code = o.store_code
WHERE
    s.country_code = 'DE'
GROUP BY
    s.store_type,
    s.country_code
ORDER BY
    total_sales ASC;
```

**Explanation:**

This query calculates the total sales for each store type in Germany (country_code 'DE'). 

It joins three tables to connect the necessary data:

- `orders_table` (order details, including product_quantity and product_code)
- `dim_products` (product prices, joined on product_code to calculate total sales)
- `dim_store_details` (store types and country codes, joined on store_code to filter by country)

The total sales are calculated by multiplying the product quantity `o.product_quantity` by the product price `p."product_price(£)"`, casting the result to numeric for precision, and rounding to two decimal places using `ROUND()`. 

The `SUM()` function aggregates the total sales for each store type within Germany.
The `WHERE` clause filters the results to include only stores in Germany. The `GROUP BY` clause groups the results by store type and country code, while the `ORDER BY` clause sorts the results by total sales in ascending order.

This query efficiently combines data from multiple tables to provide meaningful insights into store performance within a specific country.

**Result:**

[image-will-be-added.png]

**Analysis and Recommendations:**

**Analysis:**

The results show that Local stores generate the most sales in Germany, followed by Super Stores, Mall Kiosks, and Outlets. This indicates that Local stores are the most effective sales channel in Germany.

**Recommendations:**

**Focus on Local Stores:**
- Prioritize the expansion of Local stores in Germany, as they generate the highest sales.
- Enhance customer experience in Local stores through personalized services, loyalty programs, and community engagement initiatives.

**Evaluate Super Stores:**

- Consider strategies to boost sales in Super Stores, such as improving product assortments, optimizing store layouts, and conducting targeted marketing campaigns.
- Implement performance monitoring to identify areas for improvement and ensure profitability.

**Mall Kiosk and Outlet Strategy:**

- Assess the performance of Mall Kiosks and Outlets to determine their profitability and potential for growth.
- If necessary, reallocate resources from underperforming Mall Kiosks and Outlets to more profitable store types.

### 9. Average Time Between Sales by Year

**Objective:**
Calculating how quickly the company is making sales

**SQL Query:**
```sql
WITH next_time_stamp_cte AS (
    SELECT
        d.timestamp AS current_timestamp,
        d.year,
        (LEAD(d.timestamp) OVER (ORDER BY d.timestamp)) - d.timestamp AS time_until_next_transaction
    FROM 
        dim_date_times d
)
SELECT 
    year,
    CONCAT(
        '"hours": ', EXTRACT(HOUR FROM AVG(time_until_next_transaction)), ', ',
        '"minutes": ', EXTRACT(MINUTE FROM AVG(time_until_next_transaction)), ', ',
        '"seconds": ', EXTRACT(SECOND FROM AVG(time_until_next_transaction)), ', ',
        '"milliseconds": ', EXTRACT(MILLISECOND FROM AVG(time_until_next_transaction))
    ) AS actual_time_taken
FROM
    next_time_stamp_cte
GROUP BY
    year
ORDER BY
    AVG(time_until_next_transaction) DESC;
```

**Explanation:**

This query calculates the average time between consecutive transactions for each year. 
It uses a Common Table Expression named `next_time_stamp_cte` to determine the time difference between each transaction and the following one.

The `LEAD()` window function is used to access the timestamp of the next transaction. It looks ahead to the next row within the partition (ordered by timestamp) and returns the value of the specified column.  By subtracting the current timestamp from the next one, we obtain the time until the next transaction.

The main query then aggregates the data by year and calculates the average time between transactions using `AVG()`. The `EXTRACT()` function is used to extract the hours, minutes, seconds, and milliseconds from the average time interval. The results are concatenated into a single string using `CONCAT()` for readability.

Finally, the results are ordered by the average time between transactions in descending order, showing the years with the longest average time between transactions first.

This query combines advanced SQL techniques, such as using CTEs, window functions `LEAD()`, and date/time functions `EXTRACT()` to to perform complex calculations and data transformations to gain insights into transaction patterns over time.

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

**Analysis:**

The data shows a steady decrease in the average time between sales over the years, indicating improved sales velocity. This suggests that the company’s sales strategies have become more effective over time.

**Recommendations:**

**Sustain Momentum:**
- Continue to implement and enhance sales strategies that have contributed to reduced time between sales.
- Regularly review and optimize sales processes to maintain high sales velocity.

**Monitor Trends:**
- Regularly monitor sales velocity to identify potential slowdowns and address them promptly.
- Use advanced analytics to forecast sales trends and make data-driven decisions.

**Customer Engagement:**
- Enhance customer engagement initiatives to maintain high sales velocity, leveraging loyalty programs, personalized marketing, and exceptional customer service.
- Implement feedback mechanisms to gather customer insights and continuously improve the sales process.

### 10. To be added

**Objective:**
To be added

**SQL Query:**
```sql
SELECT To be added
```

**Explanation:**

To be added

**Result:**


[Image will be added.png]

**Analysis and Recommendations:**

To be added

