# Scala_Discounts_RealTime_RuleEngine
A simple Scala Real-Time Rule Engine project that applies different discount rules to real-time transaction data and writes the results to a MySQL database. It also keeps tracking the logs data, and backups all data into CSV files.

## Problem Statement:
A huge retail store wants a rule engine that qualifies order transactions for discounts based on a set of qualifying rules.

They need the system to be up and running for 24 hours. Raw transactions’ files will be pushed to the following machine (IP: X.X.X.X) on the following path (……/raw_data). They need the calculations to be done automatically once the file is pushed.

## Project Details:
Scala Real-Time Rule Engine processes the data CSV files once they are pushed to the directory that is being watched. It applies different discount rules on data based on some predefined calculation rules. Then, it writes the result into MySQL database tables. It also keeps tracking the log data and storing it in the MySQL database tables. Finally, it backup all data into CSV files for any unexpecting errors.

**Discount Rules:**
The following discount rules are applied to the transactions data:

 **-Expiration Date Discount:** If the number of days remaining between the purchasing date and the expiration date is less than 30 days, a discount of (30 - (days between)) is applied. Otherwise, no discount is applied.

For example:

If 29 days remain ‐> 1% discount.

If 28 days remain ‐> 2% discount.

If 27 days remain ‐> 3% discount. etc …

**- Product Category Discount:** If the product category is "Cheese", a discount of 10% is applied. And If the product category is "Wine", a discount of 5% is applied. Otherwise, no discount is applied.

**- Specific Day of Month Discount:** If the purchasing date is March 23, a special discount of 50% is applied. Otherwise, no discount is applied.

**- Quantity Discount:** If the quantity of products purchased is more than 5 of the same product, a discount will be applied.

If the quantity is between 6 and 9, a discount of 5% is applied.
If the quantity of products purchased is between 10 and 14, a discount of 7% is applied.
If the quantity of products purchased is 15 or more, a discount of 10% is applied.
Otherwise, no discount is applied.
**- Purchasing Channel Discount:** Sales that are made through the "App" will have a special discount. A discount of the quantity rounded up to the nearest multiple of 5 is applied. Otherwise, no discount is applied.

For example:

If quantity: 1, 2, 3, 4, 5 ‐> discount 5%.
If quantity 6, 7, 8, 9, 10 ‐> discount 10%.
If quantity 11, 12, 13, 14, 15 ‐> discount 15%. etc …
**- Payment Method Discount:** If the payment method is "Visa", a discount of 5% is applied. Otherwise, no discount is applied.

**Main Rules:**
- Transactions that didn't qualify for any discount will have a 0% discount.
- Transactions that qualified for more than one discount will get the top 2 and get their average.
- After ingesting the raw data and calculating the discount the final price will be calculated and loaded into the output files.
- The raw data needs to be removed from the source directory after successfully processing and writing it in the database.
- It is required to log the engine’s events in a log file.

**Project Technicality:**
- The project is written using Scala programming language, in a purely functional manner.
- It follows the Functional Programming approach, by using pure, immutable, and predictable behavior functions and data structures.
- No loops and no Null values are used.
- All functions are documented.

#### Project Main Functions: ####
**expiration_fn:** Calculates the discount to be applied to a product based on its expiration date.

**Product_category:** Determines the discount to be applied to a product based on its category.

**exact_day:** Determines the discount to be applied to a purchase made on a specific day.

**quantityDiscount:** Determines the discount to be applied to a purchase based on the quantity of items bought.

**appDiscount:** Determines the discount to be applied to a purchase based on the purchasing channel.

**visaDiscount:** Determines the discount to be applied to a purchase based on the payment method.

**appliedDiscount:** Determines the discount to be applied to a purchase based on the highest two discount values.

**finalPrice:** Determines the final price after applying the discount.
