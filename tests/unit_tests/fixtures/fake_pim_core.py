import pytest 
from ..services.pim_core import FakePimCore

@pytest.fixture
def pim_core(monkeypatch):
    core = FakePimCore()

    async def fake_post(self, url, *args, **kwargs):
        payload = kwargs.get("json") or kwargs.get("content")
        if isinstance(payload, bytes):
            import orjson
            payload = orjson.loads(payload)

        class Resp:
            def json(self):
                return payload_response

        payload_response = await core.handle(payload)
        return Resp()

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    return core
