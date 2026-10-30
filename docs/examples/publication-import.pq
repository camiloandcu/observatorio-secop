// Reusable Power Query function. All queries receive the SAME frozen literal
// descriptor record. This template requires actual Power BI Desktop verification.
// Use fields copied from secop-publication resolve; do not call resolve per query.
(Publication as record, Relation as text, Server as text, DatabaseName as text,
 optional Projection as nullable list, optional FirstRowOnly as nullable logical) as table =>
let
    AllowedRelations = {
        "fact_contract", "dim_entity", "dim_supplier", "dim_municipality",
        "dim_modality", "dim_date", "mart_contract_overview", "mart_entity_monthly",
        "mart_supplier_concentration", "mart_contract_detail", "mart_data_quality",
        "publication_metadata"
    },
    RawId = Publication[publication_id],
    ValidId = Value.Is(RawId, type text) and Text.Length(RawId) = 32 and
        Text.Select(RawId, List.Combine({{"0".."9"}, {"a".."f"}})) = RawId,
    Id = if ValidId then RawId else error Error.Record(
        "PUBLICATION_INVALID", "El identificador de publicación no es válido.", null),
    ReadNamespace = "publication_" & Id,
    ResolvedAt = DateTimeZone.FromText(Publication[resolved_at]),
    ExpiresAt = DateTimeZone.FromText(Publication[operation_expires_at]),
    LifetimeValid = ExpiresAt - ResolvedAt = #duration(0, 1, 0, 0),
    // Only validated hexadecimal ID enters this SQL value. Relation names below
    // come from an explicit allowlist, never from report text or external SQL.
    CatalogQuery =
        "SELECT publication_id,read_namespace,state," &
        "to_char(clock_timestamp() AT TIME ZONE 'UTC', " &
        "'YYYY-MM-DD""T""HH24:MI:SS.US""Z""') AS server_now_utc " &
        "FROM published.catalog WHERE publication_id='" & Id & "'",
    Catalog = Table.Buffer(PostgreSQL.Database(Server, DatabaseName,
        [Query = CatalogQuery, CommandTimeout = #duration(0, 0, 1, 0)])),
    Entry = if Table.RowCount(Catalog) = 1 then Catalog{0} else error Error.Record(
        "PUBLICATION_UNAVAILABLE", "Reinicia el refresh completo con otra publicación.", null),
    // PostgreSQL connector mappings can drop the timezone from timestamptz.
    // Parse an explicit UTC instant instead of attaching the Desktop's zone.
    ServerNow = DateTimeZone.FromText(Entry[server_now_utc]),
    Available = Entry[state] = "retained" and Entry[read_namespace] = ReadNamespace,
    CheckedNamespace = if not LifetimeValid or ResolvedAt > ServerNow then error Error.Record(
        "PUBLICATION_INVALID", "El plazo del descriptor no es válido.", null)
        else if ServerNow >= ExpiresAt then error Error.Record(
            "PUBLICATION_EXPIRED", "Selecciona otra publicación y reinicia todo el refresh.", null)
        else if not Available then error Error.Record(
            "PUBLICATION_UNAVAILABLE", "Reinicia el refresh completo con otra publicación.", null)
        else ReadNamespace,
    CheckedRelation = if List.Contains(AllowedRelations, Relation) then Relation else
        error Error.Record("PUBLICATION_INVALID", "La relación no está permitida.", null),
    Fields = if Projection = null then {} else Projection,
    SafeFields = List.AllTrue(List.Transform(Fields, each
        Value.Is([name], type text) and Text.Length([name]) > 0 and
        Text.Select([name], List.Combine({{"a".."z"}, {"A".."Z"}, {"0".."9"}, {"_"}})) = [name]
        and List.Contains({"plain", "utc", "currency", "json"}, [kind]))),
    CheckedFields = if SafeFields then Fields else error Error.Record(
        "PUBLICATION_INVALID", "La proyección de columnas no es válida.", null),
    Quote = (Name as text) as text => """" & Name & """",
    QualifiedRelation = Quote(CheckedNamespace) & "." & Quote(CheckedRelation),
    MoneyFields = List.Select(CheckedFields, each [kind] = "currency"),
    CurrencySafe = if List.IsEmpty(MoneyFields) then true else
        Table.First(Table.Buffer(PostgreSQL.Database(Server, DatabaseName,
            [Query = "SELECT " & Text.Combine(List.Transform(MoneyFields, each
                "coalesce(sum(abs(" & Quote([name]) & ")),0) <= 922337203685477.5807"), " AND ") &
                " AS within_currency_range FROM " & QualifiedRelation,
             CommandTimeout = #duration(0, 0, 1, 0)])))[within_currency_range],
    BoundedRelation = if CurrencySafe then QualifiedRelation else error Error.Record(
        "PUBLICATION_CURRENCY_RANGE", "El valor monetario excede el dominio decimal exacto del reporte.", null),
    SelectList = if List.IsEmpty(CheckedFields) then "*" else
        Text.Combine(List.Transform(CheckedFields, each
            if [kind] = "utc" then "to_char(" & Quote([name]) &
                " AT TIME ZONE 'UTC', 'YYYY-MM-DD""T""HH24:MI:SS.US""Z""') AS " & Quote([name])
            else if [kind] = "json" then
                "CAST(to_json(" & Quote([name]) & ") AS text) AS " & Quote([name])
            else if [kind] = "currency" then
                "CAST(" & Quote([name]) & " AS text) AS " & Quote([name])
            else Quote([name])), ", "),
    FirstOnly = if FirstRowOnly = null then false else FirstRowOnly,
    FirstClause = if not FirstOnly then "" else if CheckedRelation = "mart_contract_detail" then
        " ORDER BY contract_key LIMIT 1" else error Error.Record(
            "PUBLICATION_INVALID", "La lectura de cobertura requiere el detalle contractual.", null),
    Rows = PostgreSQL.Database(Server, DatabaseName,
        [Query = "SELECT " & SelectList & " FROM " & BoundedRelation & FirstClause,
         CommandTimeout = #duration(0, 0, 1, 0)]),
    BufferedRows = Table.Buffer(Rows),
    // Force all rows before checking a distinct, relation-specific server clock.
    // This also runs for empty relations and rejects expiry during any SQL read.
    CompletionNonce = if Table.RowCount(BufferedRows) >= 0 then
        Id & ":" & CheckedRelation & ":" &
        (if FirstOnly then "first" else "all") & ":" &
        DateTimeZone.ToText(DateTimeZone.UtcNow(), "yyyyMMddHHmmss.fffffff", "en-US")
        else error Error.Record("PUBLICATION_INVALID", "No se pudo completar la lectura.", null),
    // Keep SQL text stable; native approval reuse with changing bound parameters
    // requires Desktop verification. Bind the cache discriminator, not a comment.
    CompletionQuery =
        "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', " &
        "'YYYY-MM-DD""T""HH24:MI:SS.US""Z""') AS server_now_utc, " &
        "CAST(@nonce AS text) AS completion_nonce",
    Completion = Table.First(Table.Buffer(Value.NativeQuery(
        PostgreSQL.Database(Server, DatabaseName,
            [CommandTimeout = #duration(0, 0, 1, 0)]),
        CompletionQuery, [nonce = CompletionNonce], [EnableFolding = false]))),
    CompletedAt = if Completion[completion_nonce] = CompletionNonce then
        DateTimeZone.FromText(Completion[server_now_utc]) else error Error.Record(
            "PUBLICATION_INVALID", "No se pudo verificar el cierre de la lectura.", null),
    CheckedRows = if CompletedAt >= ExpiresAt then error Error.Record(
        "PUBLICATION_EXPIRED", "Selecciona otra publicación y reinicia todo el refresh.", null)
        else BufferedRows
in
    CheckedRows
