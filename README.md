# project_dbt

README sintetico per il progetto dbt "project_dbt".

## Overview
Progetto dbt per il data staging e l'implementazione di logiche incremental e SCD (Type 1 e Type 2) su Customers e Products.

## Struttura principale
- Configurazione:
  - [dbt_project.yml](dbt_project.yml)
  - [profiles.yml](profiles.yml)
  - [packages.yml](packages.yml)
  - [package-lock.yml](package-lock.yml)

- Modelli (staging):
  - [`models.staging.project.stg_scd1_project__customers`](models/staging/project/stg_scd1_project__customers.sql) — SCD Type 1 (delete+insert, unique_key: customer_cd). Vedi anche: [models/staging/project/stg_scd1_project__customers.yml](models/staging/project/stg_scd1_project__customers.yml)
  - [`models.staging.project.stg_scd1_project__products`](models/staging/project/stg_scd1_project__products.sql) — SCD Type 1 (merge, unique_key: product_cd). Vedi anche: [models/staging/project/stg_scd1_project__products.yml](models/staging/project/stg_scd1_project__products.yml)
  - [`models.staging.project.stg_scd2_project__customers`](models/staging/project/stg_scd2_project__customers.sql) — SCD Type 2 (merge, unique_key: customer_update_id). Vedi anche: [models/staging/project/stg_scd2_project__customers.yml](models/staging/project/stg_scd2_project__customers.yml)
  - [`models.staging.project.stg_scd2_project__products`](models/staging/project/stg_scd2_project__products.sql) — SCD Type 2 (merge, unique_key: [product_cd, is_current]). Vedi anche: [models/staging/project/stg_scd2_project__products.yml](models/staging/project/stg_scd2_project__products.yml)

- Sources / schema:
  - [models/staging/project/schema.yml](models/staging/project/schema.yml)

- Seeds (dati di esempio):
  - [seeds/customers_t0.csv](seeds/customers_t0.csv)
  - [seeds/customers_t1.csv](seeds/customers_t1.csv)
  - [seeds/products_t0.csv](seeds/products_t0.csv)
  - [seeds/products_t1.csv](seeds/products_t1.csv)

- Cartelle generate / output:
  - [target/](target/) (catalog, manifest, run_results, ecc.)

## Logiche implementate
- I modelli usano `is_incremental()` per switchare tra la sorgente T0 e T1:
  - Vedi implementazione in [`models/staging/project/stg_scd1_project__customers.sql`](models/staging/project/stg_scd1_project__customers.sql) e [`models/staging/project/stg_scd1_project__products.sql`](models/staging/project/stg_scd1_project__products.sql).
- SCD1: sovrascrittura/merge dei record cambiati. (products: MINUS per isolare changed records).
- SCD2: inserimento di nuove versioni + chiusura versioni precedenti (valid_to, is_current). Vedi implementazioni in:
  - [`models/staging/project/stg_scd2_project__customers.sql`](models/staging/project/stg_scd2_project__customers.sql)
  - [`models/staging/project/stg_scd2_project__products.sql`](models/staging/project/stg_scd2_project__products.sql)

## Dipendenze
- Definite in [packages.yml](packages.yml) e lock in [package-lock.yml](package-lock.yml).
  - package example: dbt-labs/codegen (v0.13.1)
  - Vengono anche usate macro di utilità (`dbt_utils.generate_surrogate_key`) — controllare `dbt_packages/` se presente.

## Comandi rapidi
- Installare dipendenze:
```sh
dbt deps
```
- Caricare seed:
```sh
dbt seed
```
- Eseguire tutti i modelli:
```sh
dbt run
```
- Eseguire solo lo staging:
```sh
dbt run --models staging.project.*
```
- Eseguire i test:
```sh
dbt test
```

## Note operative
- Configurare le credenziali in [profiles.yml](profiles.yml) (attualmente presente in workspace).
- I modelli incremental usano logiche diverse per SCD1/SCD2; verificare i campi surrogate key e unique_key definiti in ciascun file `.sql`.
- Controllare i risultati in [target/run_results.json](target/run_results.json) e il manifest in [target/manifest.json](target/manifest.json).

## Dove guardare nel progetto
- File di esempio / entrypoint: [README.md](README.md)
- Modelli SQL: [models/staging/project/](models/staging/project/)
- Seeds: [seeds/](seeds/)
- Config e profili: [dbt_project.yml](dbt_project.yml), [profiles.yml](profiles.yml)
