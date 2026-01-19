import pytest

@pytest.mark.asyncio
class TestResumeLogic:

    async def test_resume_from_correct_chunk(
        self,
        ingestion_service,
        state_store,
        pim_core
    ):
        state_store.ack_chunk("ing-1", 2, 50)

        await ingestion_service.stream_and_push("ing-1", request)

        assert pim_core.received_chunks[0] == 3
