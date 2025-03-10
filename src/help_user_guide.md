# DxDy TUI Application – User Guide

Welcome to the **dxdy** Textual User Interface (TUI)! This guide will show you how to install, run, and operate the TUI to interact with your **dxdy** DuckDB database, manage trading records, view real‐time updates, generate P&L reports, analyze risk, and more. 

---

## Introduction

The **DxDy** TUI is a terminal‐based application that allows you to:
- Manage a DuckDB database of trades, securities, portfolios, corporate actions, and more.  
- See real‐time positions and P&L updates for multiple portfolios.  
- Generate interactive P&L reports, sector allocations, FX exposures, and strategy breakdowns.  
- Perform risk analysis (such as sector allocations and currency exposure) with textual charts and the `plotext` library.  
- Enter trades or corporate actions (stock splits, dividends, etc.) via a built‐in form.  
- Inspect application logs and read help documentation without leaving the TUI.

This application is built using the [Textual](https://github.com/Textualize/textual) framework in Python.  

---

## Quick Start

1. **Install Requirements**  
   - Python 3.9+  
   - `textual`, `duckdb`, `pandas`, `plotext`, `loguru`, etc.  

2. **Run the TUI**  
   1. Clone or copy the repository containing the TUI code.  
   2. Ensure you have a DuckDB database file (see the `db` folder or the sample script for building the schema).  
   3. In a terminal, navigate to the application’s root folder and run:
      ```bash
      python app.py
      ```
   4. The TUI will launch in your terminal.  

3. **Basic Interactions**  
   - Use your arrow keys or mouse (if your terminal supports it) to highlight items in tree views.  
   - Press **Enter** to expand/collapse nodes or select an item.  
   - Use the **key bindings** (e.g., `q`, `r`, `k`, `s`) to jump between different screens.  

---

## Overall Navigation

### Key Bindings

| Key | Action                                     | Effect                                   |
|-----|--------------------------------------------|------------------------------------------|
| `q` | Quit                                       | Closes the application.                  |
| `r` | **Reports** Screen                         | Switches to the P&L Reports interface.   |
| `k` | **Risk** Screen                            | Switches to the Risk analysis interface. |
| `s` | **Dashboard** Screen                       | Switches to the real‐time Dashboard.     |
| `d` | **Database** Screen                        | Switches to the DuckDB viewer.           |
| `e` | **Data Entry** (trade ticket) Screen       | Opens the trade ticket form.             |
| `o` | **Log** Screen                             | Displays the log viewer.                 |
| `h` | **Help** Screen                            | Shows Help/Documentation.                |
| `x` | Copy (when in certain DataTables)          | Copy the current DataFrame to clipboard. |

> **Tip**: Additional key commands may be shown in the footer or in each screen’s help text.

### Main Menu

When the TUI first opens, or after you close the initial splash, you’ll see a set of hotkeys in the footer. These hotkeys map to each “section” of the TUI:
- `Dashboard` (`s`)
- `Reports` (`r`)
- `Risk` (`k`)
- `Database` (`d`)
- `Data Entry` (`e`)
- `Copy` (`x`)  
- `Log` (`o`)
- `Help` (`h`)

Pressing a hotkey immediately takes you to that screen.  

---

## Screens / Workflows

Below are the main screens you can explore:

### 1. Splash Screen
- Displays briefly on startup. Shows an animated gradient in the terminal.
- Automatically transitions to the default screen (by default, the `Help` screen or the `Dashboard`).

### 2. Dashboard Screen
- A real‐time “positions” viewer.  
- Shows your portfolios and their holdings, quantity, cost basis, market value, and day’s P&L.
- Also includes a small tree for portfolio selection and a content switcher that can show a textual chart for intraday P&L.

### 3. Reports Screen
- A tree of *portfolio-level* and *security-level* P&L reports.
- Drill down to see daily P&L, sector allocations, strategy allocations, FX allocations, and line charts of P&L over time.

### 4. Risk Screen
- A simpler tree of portfolios, plus three tab panes (`Sectors`, `Strategies`, `FX`).
- Provides bar charts or multi-bar charts to show exposures by sector, strategy (long vs. short positions), or currency.  

### 5. Database Screen
- A “DuckDB viewer”: on the left is a tree of the tables in the database, and on the right is a paginated table display of the selected table.  
- You can sort columns, copy data to your clipboard, and so forth.  

### 6. Data Entry Screen
- A form for creating new trades (buy/sell of stock or options), logging stock splits, or entering dividend information.  
- Each set of fields corresponds to a tab: “Stock,” “Option,” “Stock Split,” “Dividend.”  
- You can press “Buy” / “Sell” or “Save” to confirm.  

### 7. Log Screen
- Displays the application’s main log file in a textual widget.  
- Automatically updates when you return to the screen.  

### 8. Help Screen
- Renders a Markdown help file from `online_help.md` in your project root.  
- Contains table of contents for quick navigation.  

---

## How to Use Each Screen

### Dashboard Usage

1. **Select a Portfolio**  
   - On the left, you’ll see a tree of your portfolios. Selecting a portfolio node will filter the table for that portfolio’s positions.  
   - Under each portfolio node, you may see “Stocks,” “Options,” or “P&L Chart.”  
2. **Positions Table**  
   - The main table shows columns like Ticker, Qty, Cost Basis, Close Price, Market Value, % Change, and P&L.  
   - This data updates automatically (or on a short polling interval) if real‐time data is piped in via ZMQ.  
3. **Switching to Intraday Chart**  
   - Selecting “P&L Chart” from the portfolio tree or certain nodes toggles a textual chart of that portfolio’s intraday or daily P&L.  

### Reports Usage

1. **Select a Portfolio**  
   - Under the portfolio node, you’ll see sub‐nodes: “P&L Report,” “P&L Drilldown,” “P&L Chart,” “Sector Report,” “Strategy Report,” “FX Report,” plus separate “Stocks” and “Options” sub‐trees.  
2. **P&L Report**  
   - A tabular summary of either portfolio-level or security-level P&L on each date.  
3. **P&L Drilldown**  
   - Explodes the P&L into security-level detail. Use `<` (comma) and `>` (period) keys to go backward or forward in time.  
4. **P&L Chart**  
   - A textual line chart over time for the selected portfolio or security.  
5. **Sector / Strategy / FX Report**  
   - These pivot the data by sector, strategy (long vs. short), or currency.  
   - In the table, you can see how the portfolio exposure or P&L breaks down.  

### Risk Usage

1. **Select a Portfolio**  
   - The left tree lists each portfolio.  
2. **Tab Pane: “Sectors,” “Strategies,” “FX”**  
   - Each tab displays a textual bar chart or multi‐bar chart representing your exposures.  
   - Use the `,` (comma) or `.` (period) keys to change the effective “date” for risk calculations if you want to look historically.  

### Database Usage

1. **DuckDB Table Tree**  
   - On the left, a tree of all tables in the DuckDB (plus columns).  
   - Click (or press Enter) on a table name to load it in the right panel.  
2. **Table Data**  
   - The right side is a paginated `DataTable`.  
   - Press “Next” / “Prev” to change pages.  
   - Click a column header to sort by that column.  
3. **Copy to Clipboard**  
   - If you press the `x` (Copy) key, the entire DataFrame is copied to your system clipboard in CSV format.  

### Data Entry Usage

1. **Tabs for Stock, Option, Stock Split, and Dividend**  
   - Switch between these tabs to fill in the form relevant to your data entry.  
2. **Portfolio & Ticker**  
   - For trades, choose your portfolio, trade date, ticker, exchange, quantity, and price.  
   - Press “Buy” or “Sell.” You will be prompted to press again to confirm.  
3. **Confirmation**  
   - After confirming, the trade (or stock split/dividend) will be inserted into the database.  

### Log Usage

- When you switch to **Log** (`o` key), the application reads from `log_dxdy.log` and displays the lines in a scrolling widget.  
- Helpful for debugging or tracing your actions.  

### Help Usage

- The **Help** screen (`h` key) displays a Markdown viewer loaded from `online_help.md`.  
- Press arrow keys or use the mouse to scroll.  
- Includes a table of contents for quick navigation.  

---

## Common Operations

1. **Viewing a Portfolio’s P&L**  
   - Press `r` for **Reports** → expand the portfolio node → select “P&L Report” or “P&L Chart.”  
2. **Looking at All DB Tables**  
   - Press `d` for **Database** → expand the left “db” tree → pick a table.  
3. **Entering a New Trade**  
   - Press `e` for **Data Entry** → fill in fields under “Stock” or “Option” tab → click “Buy” or “Sell.”  
4. **Checking Real‐Time Positions**  
   - Press `s` for **Dashboard** → pick a portfolio in the left tree → see the table of current positions.  
5. **Analyzing Risk**  
   - Press `k` for **Risk** → pick a portfolio → switch tabs between “Sectors,” “Strategies,” and “FX.”  

---

## Tips & Troubleshooting

- **Resizing Terminal**: If the layout looks misaligned, try resizing your terminal and re‐selecting the screen.  
- **Missing Data**: If certain columns or data appear blank, ensure your DuckDB has data for those fields (e.g., `securities` or `market_data`).  
- **Keyboard Input**: Some terminals may require you to press **Enter** to confirm you’ve highlighted a node in a tree.  
- **Error Messages**: Check the **Log** screen or the `log_dxdy.log` file if you suspect an error.  

---

## Additional Information

- **Textual**: The TUI is powered by [Textual](https://github.com/Textualize/textual).  
- **Plotext**: Charts in the TUI come from the [plotext](https://github.com/piccolomo/plotext) library for ASCII‐based plotting.  
- **DuckDB**: All data is stored in [DuckDB](https://duckdb.org/). The schema creation scripts and utility functions are in the `db` folder.  
- **Loguru**: Logging is handled by [Loguru](https://loguru.readthedocs.io/).  

Thank you for using DxDy! For any advanced configuration, see the `settings.py` and `saas_settings.py` or open the project’s `Help` screen from within the TUI.
