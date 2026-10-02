# Synthetic MetronInfo Fixtures

These documents contain invented titles, publishers, and identifiers. They are
not provider responses, real comic metadata, or licensed comic content.

Field shapes were checked against the upstream MetronInfo schemas at commit
`77b9fdbe489568b9e047206005e7f6b0700bb3f0`:

- [MetronInfo 1.0 schema](https://github.com/Metron-Project/metroninfo/blob/77b9fdbe489568b9e047206005e7f6b0700bb3f0/schema/v1.0/MetronInfo.xsd)
- [MetronInfo 1.1 schema](https://github.com/Metron-Project/metroninfo/blob/77b9fdbe489568b9e047206005e7f6b0700bb3f0/schema/v1.1/MetronInfo.xsd)

The root has no required version attribute. The fixtures exercise the core
reader, not full schema validation or lossless XML rewriting. The 1.1 fixture
includes an unmapped community field deliberately; it must not become canonical
issue identity or silently disappear in a later rewrite.

`descriptive.xml` adds credits, opaque creator/role IDs, aliases/languages,
publisher/imprint resources, summary, notes, page count, collection fields and
named resources. `test_metroninfo_descriptive.py` verifies bounded immutable reads,
unknown-content diagnostics, absent versus empty credits, and no partial credit
lists after malformed or over-limit input. Unknown role text remains evidence;
reading it is not a claim that it passes the output schema. Generic resource IDs
do not establish provider identities. Missing language does not invent a user
value from the schema default.

`test_metroninfo.py` covers parsed fields, scoped identities, LOCG discovery
references, conflicts, supported encodings, and XML/resource boundaries.
`test_archive_metadata.py` builds temporary ZIP, TAR, and 7z archives with
synthetic XML and page bytes. RAR stream behavior uses a mocked backend because
the test environment does not create RAR archives. It also proves that a ZIP
with a `.cbr` suffix is detected by its container header.

The paired archive probe returns bounded bytes and read diagnostics, not a
match, safety approval, or permission to rewrite. Existing import consumers
remain on their current read path until multi-source reconciliation is wired.
