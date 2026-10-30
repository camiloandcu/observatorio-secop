"""Consumer-only metadata fields; published source schemas remain unchanged."""

DERIVED_FIELDS = {
    "scope_signing_from": "scope.signing_from",
    "scope_signing_to": "scope.signing_to",
    "scope_missing_signing": "scope.missing_signing",
    "scope_source_clock": "scope.source_clock",
    "coverage_metadata": "detail.coverage_metadata",
    "coverage_summary": "detail.coverage_summary",
}

# The extra detail read is limited to one row and uses the same frozen descriptor.
# It extracts publication-level evidence once, without joining it to contract rows.
EXPRESSION = """
, GetField = (Value as any, Name as text) as any =>
    if Value.Is(Value, type record) then Record.FieldOrDefault(Value, Name, null) else null
, Display = (Value as any) as text =>
    if Value = null then "no disponible" else Text.From(Value, "es-CO")
, UtcDisplay = (Input as any) as text =>
    if Input = null then "no disponible" else DateTimeZone.ToText(
        DateTimeZone.SwitchZone(DateTimeZone.FromText(Input), 0), "d MMM yyyy, HH:mm:ss", "es-CO")
, ScopeDocument = if Table.RowCount(RawSource) = 1 then
    Json.Document(RawSource{0}[scope]) else error Error.Record(
        "PUBLICATION_METADATA_INVALID", "La publicación requiere una fila de metadatos.", null)
, ScopeCompatible = GetField(ScopeDocument, "schema_version") = "population-scope-v1" and
    GetField(ScopeDocument, "basis") = "signing_interval"
, CoverageRows = if Table.RowCount(RawSource) = 1 then ReadPublication(
    PublicationDescriptor, "mart_contract_detail", PublicationServer, PublicationDatabase,
    {[name = "coverage_metadata", kind = "json"]}, true)
    else error Error.Record("PUBLICATION_METADATA_INVALID", "Los metadatos no son únicos.", null)
, CoverageText = if Table.IsEmpty(CoverageRows) then null else CoverageRows{0}[coverage_metadata]
, CoverageDocument = if CoverageText = null then null else Json.Document(CoverageText)
, AcquisitionValue = GetField(CoverageDocument, "acquisitions")
, Acquisitions = if Value.Is(AcquisitionValue, type list) then AcquisitionValue else {}
, CoverageSummary = if List.IsEmpty(Acquisitions) then
    "Ventanas y límites de adquisición no disponibles para esta publicación."
    else Text.Combine(List.Transform(List.Positions(Acquisitions), (Index) => let
        Acquisition = Acquisitions{Index},
        Limits = GetField(Acquisition, "limits"),
        Consumed = GetField(Acquisition, "consumed"),
        Truncated = GetField(Acquisition, "truncated"),
        TruncationText = if Truncated = true then "sí" else if Truncated = false then "no"
            else "no determinada"
        in "Adquisición " & Text.From(Index + 1) & "; UTC " &
            UtcDisplay(GetField(Acquisition, "observed_from_utc")) & " → " &
            UtcDisplay(GetField(Acquisition, "observed_to_utc")) & "; filas " &
            Display(GetField(Consumed, "rows")) & "/" & Display(GetField(Limits, "rows")) &
            "; intentos " & Display(GetField(Consumed, "attempts")) &
            "/" & Display(GetField(Limits, "attempts")) & "; truncada: " & TruncationText
    ), "#(lf)")
, ScopeFields = {
    {"scope_signing_from", "signing_from"}, {"scope_signing_to", "signing_to"},
    {"scope_missing_signing", "missing_signing"}, {"scope_source_clock", "source_clock"}}
, WithScope = List.Accumulate(ScopeFields, RawSource, (State, Field) =>
    Table.AddColumn(State, Field{0}, each if ScopeCompatible then
        GetField(ScopeDocument, Field{1}) else null, type nullable text))
, WithCoverage = Table.AddColumn(WithScope, "coverage_metadata", each CoverageText,
    type nullable text)
, Source = if Table.RowCount(RawSource) <> 1 then error Error.Record(
    "PUBLICATION_METADATA_INVALID", "La publicación requiere una fila de metadatos.", null)
    else Table.AddColumn(WithCoverage, "coverage_summary", each CoverageSummary,
        type nullable text)
"""
