"""Evidence work must stop before the owning stage can be cleaned up."""

import asyncio
import threading

import pytest

from pullbox.services import archive_metadata_publication as publication


async def test_cancelled_preparation_reaps_file_work_before_return(monkeypatch):
    started, stopped, release = threading.Event(), threading.Event(), threading.Event()

    def work(*args):
        started.set()
        stop = args[-1]
        try:
            if isinstance(stop, threading.Event):
                stop.wait(2)
                release.wait(2)
            else:
                release.wait(2)
        finally:
            stopped.set()

    monkeypatch.setattr(publication, "_prepare", work)
    task = asyncio.create_task(publication.prepare_archive_publication(None, None, None, None))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done(), "Cancellation must join file work before stage cleanup"
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped.is_set()
    finally:
        release.set()
        await asyncio.to_thread(stopped.wait, 2)


async def test_cancelled_inspection_reaps_file_work_before_return(monkeypatch):
    started, stopped, release = threading.Event(), threading.Event(), threading.Event()

    def work(*args):
        started.set()
        try:
            stop = args[-1]
            if isinstance(stop, threading.Event):
                stop.wait(2)
            release.wait(2)
        finally:
            stopped.set()

    monkeypatch.setattr(publication, "_inspect", work)
    task = asyncio.create_task(publication.inspect_archive_publication(None))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done(), "Inspection must not outlive its owning workflow"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stopped.is_set()
    finally:
        release.set()
        await asyncio.to_thread(stopped.wait, 2)
