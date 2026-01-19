class FakePimCore:
    def __init__(self):
        self.received_chunks = []
        self.fail_on = set()

    def reject_chunk(self, n):
        self.fail_on.add(n)

    async def handle(self, payload):
        if payload.get("status") == "COMPLETED":
            return {"ack": True}

        if payload["chunk_number"] in self.fail_on:
            return {"ack": False, "error": "SIMULATED_FAILURE"}

        self.received_chunks.append(payload["chunk_number"])
        return {"ack": True}