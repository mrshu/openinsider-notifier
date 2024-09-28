from nio import AsyncClient
import asyncio
import os


async def matrix_send(msg):
    client = AsyncClient("https://matrix.org", "@argentaris:matrix.org")
    password = os.environ.get("ARGENTARIS_MATRIX_PASSWORD")
    if not password:
        raise Exception("No password provided")

    print(await client.login(password))

    await client.room_send(
        room_id="!HpzXJjByKYoKEcoQrD:matrix.org",
        message_type="m.room.message",
        content={"msgtype": "m.text", "body": msg},
    )

    await client.close()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(matrix_send("""See, this works!"""))
