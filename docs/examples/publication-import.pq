// Reusable Power Query function. All queries receive the SAME frozen literal
// descriptor record. This template requires actual Power BI Desktop verification.
// Use fields copied from secop-publication resolve; do not call resolve per query.
(Publication as record, Relation as text, Server as text, DatabaseName as text) as table =>
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
    Source = PostgreSQL.Database(Server, DatabaseName, [CommandTimeout = #duration(0, 0, 1, 0)]),
    // Only validated hexadecimal ID enters this SQL value. Relation names below
    // come from an explicit allowlist, never from report text or external SQL.
    Catalog = Value.NativeQuery(Source,
        "SELECT publication_id,read_namespace,state,clock_timestamp() AS server_now " &
        "FROM published.catalog WHERE publication_id='" & Id & "'", null),
    Entry = if Table.RowCount(Catalog) = 1 then Catalog{0} else error Error.Record(
        "PUBLICATION_UNAVAILABLE", "Reinicia el refresh completo con otra publicación.", null),
    ServerNow = DateTimeZone.From(Entry[server_now]),
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
    Rows = Value.NativeQuery(Source,
        "SELECT * FROM """ & CheckedNamespace & """.""" & CheckedRelation & """", null)
in
    Rows
