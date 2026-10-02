# Embedded Metadata Schemas

`MetronInfo-1.1.xsd` is the unmodified MetronInfo 1.1 schema from
[Metron-Project/metroninfo](https://github.com/Metron-Project/metroninfo), pinned
at commit `77b9fdbe489568b9e047206005e7f6b0700bb3f0`, path
`schema/v1.1/MetronInfo.xsd`. The upstream MIT license is included as
`METRONINFO-LICENSE`.

SHA-256: `c0af59fc39e17e32c1a01714edc42d920bddc9d14e880ffc55ab960b8243a092`.

These files ship in the Python package. Validation uses `xmlschema.XMLSchema11`
offline; do not replace it with an XSD 1.0 validator or omit `xs:assert` rules.
The schema is not downloaded at runtime. Instance schema hints, remote resource
locations, and external entity expansion must never be followed.

`xmlschema` 4.3.2 incorrectly counts explicit `primary="false"` / `primary="0"`
attributes in these assertion contexts. The write validator normalizes valid
false flags to absence only on its private parsed `IDS/ID` and `URLs/URL` nodes.
Those flags are optional and have no default in the pinned schema, so this does
not change their meaning. The source XML and upstream schema stay unchanged;
true flags, malformed booleans, required attributes, and all other schema rules
still receive full XSD validation. Regression tests cover both representations,
multiple true flags, and false flags alongside unrelated schema errors.

This schema checks representation, not identity, ownership, or file safety.
Callers still have to reconcile both embedded documents against the canonical
snapshot and obey the archive mutation/ownership contract before writing.
