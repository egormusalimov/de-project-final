CREATE TABLE IF NOT EXISTS STV202506132__DWH.global_metrics (
    date_update timestamp NOT NULL,
    currency_from varchar NULL,
    cnt_transactions int NULL,
    amount_total numeric(20, 2) NULL,
    avg_transactions_per_account numeric(10, 3) NULL,
    cnt_accounts_make_transactions int NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update::date) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
