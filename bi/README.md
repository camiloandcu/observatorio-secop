# Contract analytics dashboard

Native Power BI Desktop report over one validated SECOP publication. The three
pages are **Panorama**, **Entidades y proveedores** and **Calidad**, with a hidden
contract drill-through page. Labels are Spanish; file and measure identifiers
are stable English names.

The report explores observed contracts and concentration. It does not identify
wrongdoing, represent all Antioquia procurement, or provide a priority score.

## Reconstruct a local project

Use the project's Python3.12 environment, Node20+ and Power BI Desktop on Windows.
The generator pins Power BI Report Authoring CLI0.5.0. The current native test
environment is Desktop2.159.883.0, compatibility1606; report schema versions are
recorded in the generated definitions. Other Desktop versions need verification.
No Power BI Service account is required. Use `--name observatorio-secop-us09`
when generating a separately named project; keep its PBIP and Report folder stems
consistent for Desktop tooling.

1. Obtain an existing validated publication and credentials for `secop_bi_reader`
   through the [publication procedure](../docs/analytical-publication.md).
   Do not put operator credentials into Desktop.
2. Resolve **once** using the reader environment. Keep the resulting descriptor
   private, and build immediately; its server-defined lifetime is one hour:

   ```bash
   uv run --locked --extra warehouse secop-publication resolve \
     > reports/publication-descriptor.json
   python -m bi.build \
     --descriptor reports/publication-descriptor.json \
     --server '<Windows-reachable-host>:<port>' \
     --database '<publication-database>' \
     --output reports/dashboard-project
   ```

3. Copy the complete generated project to a Windows-local folder. Open its
   `observatorio-secop.pbip` in Desktop. Keep the `.Report` and `.SemanticModel`
   folders together. The output directory must be new or empty; the generator
   refuses to overwrite existing Desktop state.
4. In Desktop, enter the reader credentials at the PostgreSQL prompt. Approve
   only the generated read-only SQL for the chosen server/database. Run native
   **Home → Refresh** and wait for completion without pressing Cancel.
5. Check the imported publication ID, observation UTC, Import start time,
   coverage and contract totals. A generated project contains definitions only;
   it is not evidence of a successful Import.
6. Save the local project after successful validation. Keep a backup of the
   complete project/cache or local workbook before changing its definitions.
   Desktop may convert `model.bim` to a TMDL `definition/` folder on save.

For synthetic fixtures, add `--synthetic` to the build command. Every page then
shows **Datos de prueba**; these are test records, not actual
procurement. Do not present fixture captures as live source evidence.

Generated projects, credentials, caches, model data and binary workbooks stay
local. Use an ignored output location such as `reports/`. Never commit `.pbi`,
`.pbix`, connection files, descriptors or screenshots containing private data.

## Model and filters

The nine imported tables are contract detail, five dimensions, global quality,
publication metadata and ImportContext. Metadata derives scope dates and extracts
publication-level acquisition coverage once from a bounded one-row detail read.
Five additional calculated tables supply
measures and disconnected supplier/date controls. Five active single-direction
relationships filter detail from its dimensions; global evidence and supplier
focus remain disconnected.

- Reference **R** is entity, registered municipality, modality, contractual
  status, calendar and explicit signing mode. All observed valid statuses remain
  eligible; the report does not silently reinterpret them as awards.
- Supplier focus **S** is set by the synchronized **Proveedor destacado**
  selector. It changes highlighted contracts and supplier measures, while the
  full comparison ranking, reference HHI and identity coverage remain R.
- Global evidence **G** describes the imported publication. Contract filters do
  not acquire attribution for global rejects or acquisition lanes.
- Signing modes are all dates, interval `[start,end)` and missing only. A finite
  interval excludes missing signatures; inactive endpoints are ignored. Filters
  cannot recover records excluded from the publication's scope.

COP values are nominal observed amounts. Municipality describes the entity's
registration, not the location where spending occurred. Signing periods can be
revised as later observations arrive. Missing, empty and valid zero values have
different meanings. See [measure semantics](measures/README.md).

## Refresh and recovery

Each imported query embeds the same frozen descriptor and a local copy of the
[shared reader source](../docs/examples/publication-import.pq). There is no
per-table resolution of latest. SQL arrays/JSON serialize to text; timezone-aware
instants serialize explicitly as UTC; COP travels as exact text before fixed
four-decimal conversion. A server-side sum-of-absolute-values bound rejects
amounts or possible totals outside the fixed-decimal domain.

The reader checks availability/server time before reading and obtains a fresh,
relation-specific server clock after buffering all rows. Expired completion and
cell-conversion errors fail the query, including empty-result operations.
The completion query uses constant SQL with a bound per-evaluation nonce and
checks its echo. This avoids changing SQL comments on every evaluation, which
previously generated repeated native-query approvals. A new query can still
require approval; do not disable the global security setting to hide that prompt.
Repeated minimal native reads and a full Import verified parameter binding and
advancing server timestamps. Deadline-crossing behavior requires separate proof.

A failed refresh is communicated by Desktop. The report's loaded timestamp
cannot describe an unsuccessful refresh. In the native fixture, a denied read
preserved every previously imported table and its timestamp; always verify this
for the current environment and keep a recoverable saved copy.

1. Record the native error and close its summary. Avoid **View errors**, which
   creates diagnostic queries and may leave a pending Power Query draft.
2. For permission errors, have the operator restore the required publication
   read privileges. For expiry/unavailable errors, explicitly choose a retained
   publication and resolve a new descriptor. Never extend the old deadline.
3. Update the shared `PublicationDescriptor` literal in the existing model;
   keep it fixed for the entire next native Refresh. Retain the reader/server
   constants unless the intended connection actually changes.
4. Retry the **whole** refresh. Confirm all table counts, IDs, times and totals
   before saving. Do not apply only the successful subset or replace errors with
   nulls. If the loaded state is uncertain, restore the saved project/workbook
   and repeat the complete operation.

Run only one test Desktop project at a time and close saved diagnostic worker
processes before another Import. On the tested8GB host, accumulated evaluation
workers exhausted Windows commit memory; closing them recovered enough headroom
for a successful native refresh. Microsoft documents lower evaluation/concurrent
job limits under Options → Data Load for machines under resource pressure.

Updating on-disk definitions can discard the live Import. To reconstruct from a
new descriptor, generate a separate project and retain the last valid one.
Supported definition reload and model processing are distinct from native data
refresh. XMLA processing alone does not prove Desktop Refresh behavior.

## Reconciliation and review

Use the same descriptor and installed compatible governed SQL code for both
sides. For example, `secop-publication query --descriptor <file> --metric overview`
returns the reference count, total and recomputed median. `--filters <json>`
selects entity/date/status/modality; concentration uses the full reference even
when a supplier is highlighted. See the publication guide for supported filters.

In DAX Query view, compare exact monetary **text**, especially for large amounts:

```dax
EVALUATE
ROW (
    "Contracts", [Contract Count],
    "COP", FORMAT ( [Contract Value COP], "0.00", "en-US" ),
    "Median COP", FORMAT ( [Median Value COP], "0.0000", "en-US" ),
    "HHI", [HHI],
    "HHI state", [HHI State]
)
```

Apply equivalent `TREATAS` entity/date/focus filters for each scenario. Compare
money exactly; compare HHI/ratios with absolute tolerance1e-12. Do not compare
large currency through a JSON client's floating-point number representation.

Native verification includes publication activation during refresh, failure and
retry, expiry, large cent-bearing values, half-cent median, empty/zero/missing
states, reference/focus invariance, global-quality grains, safe source links,
drill-through return, keyboard order and actual rendering. Passing source/schema
checks is not native or human acceptance. Record unexecuted cases explicitly.

Before relying on the report, a reviewer should complete Panorama → supplier
comparison → highlighted contracts → detail → permitted source, then return and
explain denominator, identity coverage, geography, COP and revisions. The native
UI, accessibility and human journey still require their own evidence.
