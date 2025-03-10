# User Guide

## Overview

This documentation describes the **Project dxdy** database schema, implemented in DuckDB. It covers:

- **Core tables** (securities, trades, portfolios, etc.).
- **Views** that compute:
  - Daily market data with split adjustments.
  - Intraday + day‐end marked P&L (both “trade‐level” and aggregated “security‐level” or “portfolio‐level”).
  - Sector, currency, and strategy allocations.
- The **intraday + day‐end mark** accounting approach.

---

## Database Schema

### Table Summaries

Below are the main tables, each designed for a specific role.

1. **`securities`**  
   - Stores information about tradable instruments (stocks, ETFs, options, etc.).  
   - Key columns:
     - `security_id` (primary key)
     - `base_ticker`, `exch_code`
     - `security_type_2` (e.g. `Equity`, `Option`, etc.)
     - `ccy` (reference to `currencies` table)
   - Unique constraint on `figi`.

2. **`portfolios`**  
   - Each row represents a distinct portfolio.  
   - Key columns:
     - `portfolio_id` (primary key)
     - `portfolio_name`
     - `portfolio_ccy` (reference to `currencies`)

3. **`trades`**  
   - Stores raw trade data (buy/sell transactions).  
   - Key columns:
     - `trade_id` (primary key)
     - `portfolio_id` (references `portfolios`)
     - `security_id` (references `securities`)
     - `trade_date`, `settlement_date`
     - `quantity`, `price`, `commission`

4. **`market_data`**  
   - Daily close prices for securities.  
   - Key columns:
     - `market_data_id`
     - `security_id`, `trade_date`
     - `close_price`  
   - Unique `(security_id, trade_date)` constraint.

5. **`stock_splits`**  
   - Records stock split events.  
   - Key columns:
     - `stock_split_id`
     - `security_id`, `split_date`
     - `split_from`, `split_to`

6. **`dividends`**  
   - Dividend payments with ex‐dividend and pay dates.  
   - Key columns:
     - `dividend_id`
     - `security_id`
     - `ex_dividend_date`, `record_date`, `pay_date`
     - `cash_amount`, `ccy`

7. **`fx_rates_data`**  
   - Daily foreign‐exchange rates for each currency.  
   - Key columns:
     - `fx_rate_id`
     - `fx_date`, `ccy`, `fx_rate`

8. **`calendar_data`**  
   - Contains timestamped open/close times for an exchange.  
   - Used to build a “trading day” calendar.  

### Sequences

We define multiple DuckDB sequences, for example:
```
CREATE SEQUENCE IF NOT EXISTS seq_security_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_trade_id START 1;
...
```
Each table that needs an auto‐incrementing integer primary key references these sequences via `DEFAULT NEXTVAL(...)`.

---

## Key Concepts & Usage

### Intraday + EOD P&L

This system accounts for trades using **intraday + day‐end mark** logic and calculates lot‐based, day‐by‐day economic (but not strictly GAAP/FIFO) P&L. Each trade stands alone in `trade_level_pnl,` we effectively track each trade as a separate lot but always reference the difference from trade_price to daily close. That is a lot‐based marking approach not necessarily matching standard accounting. 

- **Trade‐Day Realization**: On the day of the trade, the difference between the fill price and the same day’s close is treated as *realized P&L*.  
- **Overnight Holding**: The next day (and every subsequent day), the position’s profit or loss is measured relative to the **original** trade price. This difference is considered *unrealized P&L*.  

For instance:
1. You buy 100 shares of ABC at \$10, and the market closes at \$11 on the same day.  
   - Realized P&L that day: \$(11 - 10) \* 100 = \$100 (minus commission).  
2. If next day’s close is \$12, you show an *unrealized* P&L of \$(12 - 10) \* 100 = \$200.  

**Important Note**: This is *not* the standard “buy today, realize only if you sell tomorrow.” Instead, *dxdy* treats each day’s closing price as a “mark” (that is to say, mark-to-market or MTM) effectively “settling” the day’s difference. This is consistent from the standpoint of “each day is a fresh day with a fresh mark,”



### Split Adjustments

- **`market_data_view`** adjusts *historical close prices* for any splits that occur *after* that trade date.  
- **`adj_trades`** adjusts *quantities* and *trade prices* so that the position is always in post‐split terms.  
- We store splits in `stock_splits` with fields like `(split_from, split_to)`. For a 2‐for‐1 split, store `(split_from=1, split_to=2)`.

### Data Entry

The following database tables reference each other. Key relationships:
- `trades.portfolio_id → portfolios(portfolio_id)`
- `trades.security_id → securities(security_id)`
- `fx_rates_data.ccy → currencies(ccy)`
- `securities.ccy → currencies(ccy)`

**Data Entry Best Practices**:
1. Insert into `currencies` first, if new currency.  
2. Insert into `securities`, referencing a valid currency.  
3. Insert trades referencing valid `portfolio_id` and `security_id`.  

---

## Views

### Calendar Views

1. **`calendar`**  
   - Simple listing of day‐by‐day “close‐of‐business” dates, generated from `calendar_data`.
   ```sql
   CREATE OR REPLACE VIEW calendar AS (
       SELECT DISTINCT date_trunc('d', market_close) AS cob_date
       FROM calendar_data
       ORDER BY cob_date
   );
   ```

2. **`calendar_eom_view`**  
   - For monthly aggregates, it returns the last trading date of each month.  

3. **`calendar_months`**  
   - Provides `(som_cob_date, eom_cob_date)` pairs for each month.  

These are used to standardize date references, e.g. daily vs. monthly intervals.

### Market Data Views

1. **`fx_rates`**  
   - Fills forward FX rates so each date in `calendar` has an FX rate, even if `fx_rates_data` is missing that day’s entry.  

2. **`market_data_view`**  
   - **Split‐adjusted** closing prices.  
   - For each security & date, it calculates a *forward product* of `(split_from / split_to)` to adjust older prices in case of subsequent splits.

3. **`market_daily_returns`**  
   - Presents day‐over‐day returns: `(close_price - previous_close_price) / previous_close_price`.  

### Trade & PnL Views

1. **`adj_trades`**  
   - Adjusts each trade for stock splits.  
   - Changes `(quantity, price, commission)` so they are consistently in post‐split terms.  

2. **`trade_level_pnl`**  
   - The core logic for intraday + day‐end marking.  
   - For each trade, it creates row expansions for **each** `cob_date >= trade_date`.  
   - **Key fields**:  
     - `rpt_realized_pnl`: recognized only on the trade date.  
     - `rpt_unrealized_pnl_local`: recognized on subsequent days (difference to the trade price).  
   - Also does currency conversions (local ccy → portfolio ccy) using day-of-trade and day-of-cob exchange rates.  

3. **`security_level_pnl`**  
   - Aggregates trade‐level detail by security for each `portfolio_id` and `cob_date`.  
   - Summaries:
     - `quantity`
     - `cost_basis` (average cost across all trades on or before that date)
     - `close_price`
     - `dividends_local`, `realized_pnl_local`, `unrealized_pnl_local`, etc.
   - Also includes portfolio‐currency aggregates.  

4. **`portfolio_level_pnl`**  
   - Aggregates the security‐level P&L to a single line per `portfolio_id` per `cob_date`.  

5. **`trades_view`**  
   - A simpler joined view of trades + adjusted quantities for convenience.

### Allocation Views

1. **`sector_allocations`**  
   - Summarizes exposure by sector, based on `sector_mappings` and `sectors`.  

2. **`fx_allocations`**  
   - Summarizes market value by security currency.  

3. **`strategy_allocations`**  
   - Groups positions by `security_type_2` and “Long” vs. “Short” classification, then aggregates.  

### Macros

A **macro** that returns position snapshots for a specified “as of” date. It pulls from `security_level_pnl` and includes:

- `portfolio_id`
- `security_id`
- `figi`
- `base_ticker`
- `security_type_2`
- `quantity`
- `avg_cost` (in local currency as of that date)

Usage example:
```sql
SELECT *
FROM positions('2025-01-31');
```

---

## FAQ

1. **How do I insert new trades?**  
   - Insert into the `trades` table with the correct `quantity`, `price`, and reference to a valid `portfolio_id` and `security_id`. Make sure you specify a `trade_date`.

2. **What if I have multiple trades in the same security on the same day?**  
   - Each trade is recognized as a separate event. For an intraday + day‐end approach, you’ll see “realized” P&L for each trade on that same day.

3. **How are short trades handled?**  
   - By using **negative** `quantity` in the `trades` table. The day‐end mark logic will treat that as a short position.

4. **How do stock splits affect my positions or market data historically?**  
   - The views `market_data_view` and `adj_trades` dynamically recalculate historical prices and trades. You do not need to rewrite historical data; simply insert a row into `stock_splits`.

5. **How do I handle currency conversions?**  
   - Ensure you have a row in `fx_rates_data` for each date and currency. The `fx_rates` view will fill forward rates, and the P&L views will automatically multiply or divide to convert from local to portfolio currency.

6. **Why does the system call it “realized P&L” if I haven’t closed my position yet?**  
   - This approach considers the day’s close as effectively “flattening” your position for P&L purposes. It’s an **intraday + day‐end mark** method, so the term “realized” here means “recognized on the day of the trade versus that day’s close,” rather than the typical “closed a prior open position.”

---

_**Last Updated**: January 31, 2025_

Copyright © 2025 **Spaghetti Software Inc.** All rights reserved.