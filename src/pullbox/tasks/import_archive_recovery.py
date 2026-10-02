"""Bounded restart recovery for import-owned archive publications."""

from uuid import UUID

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pullbox.models.archive_metadata_publication import ArchiveMetadataPublication, PublicationState
from pullbox.services.archive_metadata_binding import ArchiveMetadataBindingError
from pullbox.services.archive_metadata_publication import (
    ArchivePublicationError,
    _clean_session,
    inspect_archive_publication,
    load_archive_publication,
)
from pullbox.services.import_archive_recovery import recover_import_archive_publication

logger = structlog.get_logger(__name__)
RECOVERY_PAGE_SIZE = 32


async def recover_import_archive_publications(
    session: AsyncSession, *, job_id: int | None = None
) -> int:
    """Own the supplied clean session across short recovery transactions.

    This is called only by startup/enrichment and rollback orchestration, which
    already own commits. Release all DB transactions before hashing; never use
    elapsed time or a missing worker as authority to republish a stage.
    """
    _clean_session(session)
    if session.in_nested_transaction():
        raise ArchivePublicationError("recovery_requires_owned_session")
    active = (PublicationState.INTENDED, PublicationState.PUBLISHED)
    ceiling = await session.scalar(
        select(func.max(ArchiveMetadataPublication.id)).where(
            ArchiveMetadataPublication.state.in_(active)
        )
    )
    if ceiling is None:
        return 0
    await session.commit()
    cursor = recovered = 0
    while cursor < ceiling:
        page = (
            await session.execute(
                select(ArchiveMetadataPublication.id, ArchiveMetadataPublication.operation_id)
                .where(
                    ArchiveMetadataPublication.id > cursor,
                    ArchiveMetadataPublication.id <= ceiling,
                    ArchiveMetadataPublication.state.in_(active),
                )
                .order_by(ArchiveMetadataPublication.id)
                .limit(RECOVERY_PAGE_SIZE)
            )
        ).all()
        await session.commit()
        if not page:
            break
        for row_id, operation in page:
            cursor = row_id
            try:
                try:
                    operation_id = UUID(operation)
                except ValueError:
                    raise ArchivePublicationError("invalid_journal") from None
                receipt = await load_archive_publication(session, operation_id)
                await session.commit()
                if receipt is None or receipt.plan.import_owner is None:
                    continue
                if job_id is not None and receipt.plan.import_owner.job_id != job_id:
                    continue
                inspection = await inspect_archive_publication(receipt)
                result = await recover_import_archive_publication(session, receipt, inspection)
                await session.commit()
                recovered += int(result != receipt)
                logger.info(
                    "import_archive_publication_recovered",
                    publication_id=row_id,
                    job_id=receipt.plan.import_owner.job_id,
                    outcome=result.state.value,
                )
            except (ArchivePublicationError, ArchiveMetadataBindingError, OSError) as exc:
                await session.rollback()
                reason = "filesystem_unavailable" if isinstance(exc, OSError) else exc.code
                logger.warning(
                    "import_archive_publication_recovery_deferred",
                    publication_id=row_id,
                    reason=reason,
                )
    return recovered
