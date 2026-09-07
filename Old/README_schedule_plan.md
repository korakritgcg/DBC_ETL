# Optimized Airflow DAG Schedule Plan

## Goals
- Incremental jobs that need near realtime data run every 15 minutes.
- Heavy full-refresh jobs do not overlap.
- SQL Server write pressure is controlled by Airflow pools.
- `catchup=False`, `max_active_runs=1`, and `max_active_tasks=1` are enabled in every DAG.

## Required pools
Run `setup_airflow_pools.sh` from `~/airflow` after copying files.

| Pool | Slots | Purpose |
|---|---:|---|
| bc_realtime_pool | 1 | Near realtime incremental SQL writers |
| bc_incremental_pool | 1 | Reserved incremental pool |
| bc_full_heavy_pool | 1 | Heavy full-refresh jobs |
| bc_full_medium_pool | 1 | Medium full-refresh jobs |
| bc_full_light_pool | 2 | Small master/reference refresh jobs |

## Realtime / Incremental schedules
| DAG | Schedule | Frequency |
|---|---|---|
| Change_Log_Entries | `2,17,32,47 * * * *` | Every 15 min |
| Warehouse_Entries_Excel | `5,20,35,50 * * * *` | Every 15 min |
| General_Ledger_Entries | `8,23,38,53 * * * *` | Every 15 min |
| Item_Ledger_Entries | `11,26,41,56 * * * *` | Every 15 min |

## Full-refresh schedules
| DAG | Schedule |
|---|---|
| Posted_Sales_Invoice_Excel | `0 0 * * *` |
| Posted_Sales_Credit_Memo_ExcelSalesCrMemoLines | `30 2 * * *` |
| Purchase_Order_Line_Excel | `30 3 * * *` |
| Sales_Order_Line | `30 4 * * *` |
| Purchase_Lines_Excel | `30 5 * * *` |
| Sales_Order | `0 6 * * *` |
| Page_posted_sales_credit_memo | `20 6 * * *` |
| Posted_sales_credit_memos | `40 6 * * *` |
| Consignment_Return_Order_Line | `0 7 * * *` |
| Consignment_Return_Order | `20 7 * * *` |
| items | `40 7 * * *` |
| Item_Card | `0 8 * * *` |
| Item_Categories | `15 8 * * *` |
| Item_Units_of_Measure_Excel | `30 8 * * *` |
| Default_Dimenstions | `45 8 * * *` |
| Sales_Person | `0 9 * * *` |
| Table_Information | `15 9 * * *` |
