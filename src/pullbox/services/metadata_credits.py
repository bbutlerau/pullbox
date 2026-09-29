"""Batch descriptive credits inside the caller's locked metadata transaction."""

from collections.abc import Mapping, Sequence

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models.creator import Creator, IssueCreator
from pullbox.schemas.metadata_credits import MAX_CREDITS, MetadataCredit, parse_credits

_BATCH = 200


async def _rows(session: AsyncSession, issue_ids: Sequence[int]) -> list[tuple[int, int, str, str]]:
    rows = list(
        (
            await session.execute(
                select(IssueCreator.issue_id, Creator.id, Creator.name, IssueCreator.role)
                .join(Creator, Creator.id == IssueCreator.creator_id)
                .where(IssueCreator.issue_id.in_(issue_ids))
                .order_by(IssueCreator.issue_id, Creator.id)
                .limit(len(issue_ids) * MAX_CREDITS + 1)
            )
        )
        .tuples()
        .all()
    )
    if len(rows) > len(issue_ids) * MAX_CREDITS:
        raise ValueError("Stored creator credits exceed the bounded metadata limit.")
    return rows


def _group(rows: Sequence[tuple[int, int, str, str]]) -> dict[int, tuple[MetadataCredit, ...]]:
    grouped: dict[int, list[MetadataCredit]] = {}
    for issue_id, _, name, role in rows:
        grouped.setdefault(issue_id, []).append(MetadataCredit(name=name, role=role))
    return {issue_id: parse_credits(values) for issue_id, values in grouped.items()}


async def read_issue_credits(
    session: AsyncSession, issue_ids: Sequence[int]
) -> dict[int, tuple[MetadataCredit, ...]]:
    result = {}
    for offset in range(0, len(issue_ids), _BATCH):
        result.update(_group(await _rows(session, issue_ids[offset : offset + _BATCH])))
    return result


async def write_issue_credits(
    session: AsyncSession, values: Mapping[int, tuple[MetadataCredit, ...] | None]
) -> None:
    """Persist assembled values, never assign foreign creator IDs by name.

    Existing issue links retain their creator IDs where possible. Generic
    descriptive names can be reused, but a name alone cannot adopt another
    issue's ComicVine-identified creator or rename a shared creator row.
    None means no evidence; an empty tuple is an intentional local clear.
    """
    if len(values) > _BATCH:
        raise ValueError("Write creator credits in batches of at most 200 issues.")
    incoming = {key: value for key, value in values.items() if value is not None}
    if not incoming:
        return
    rows = await _rows(session, list(incoming))
    current = _group(rows)
    changed = {key: value for key, value in incoming.items() if current.get(key, ()) != value}
    if not changed:
        return
    existing = {(issue_id, name): creator_id for issue_id, creator_id, name, _ in reversed(rows)}
    names = sorted({credit.name for credits in changed.values() for credit in credits})
    generic: dict[str, int] = {}
    for offset in range(0, len(names), _BATCH):
        generic.update(
            (
                await session.execute(
                    select(Creator.name, func.min(Creator.id))
                    .where(
                        Creator.name.in_(names[offset : offset + _BATCH]),
                        Creator.comicvine_id.is_(None),
                    )
                    .group_by(Creator.name)
                )
            )
            .tuples()
            .all()
        )
    needed = {
        credit.name
        for issue_id, credits in changed.items()
        for credit in credits
        if credit.name not in generic and (issue_id, credit.name) not in existing
    }
    pending = {name: Creator(name=name) for name in sorted(needed)}
    session.add_all(pending.values())
    await session.flush()
    generic.update({name: creator.id for name, creator in pending.items()})
    await session.execute(delete(IssueCreator).where(IssueCreator.issue_id.in_(changed)))
    session.add_all(
        [
            IssueCreator(
                issue_id=issue_id,
                creator_id=existing.get((issue_id, credit.name)) or generic[credit.name],
                role=credit.role,
            )
            for issue_id, credits in changed.items()
            for credit in credits
        ]
    )
    await session.flush()
