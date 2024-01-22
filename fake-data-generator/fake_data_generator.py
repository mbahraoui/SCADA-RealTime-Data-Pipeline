import json
import random
import asyncio
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

# Create a generator function to simulate a stream of random data
async def generate_fake_data_stream():
    columns = ["GeneratorID", "PowerOutput", "FuelLevel", "Voltage", "FaultAlarms"]

    while True:
        fake_data = {
            "GeneratorID": f"G{random.randint(1, 10)}",
            "PowerOutput": f"{random.randint(80, 120)} MW",
            "FuelLevel": f"{random.randint(50, 90)}%",
            "Voltage": f"{random.randint(210, 250)} V",
            "FaultAlarms": random.choice(["No", "Yes:LowFuel", "Yes:HighVoltage"])
        }

        fake_data_json = json.dumps(fake_data).encode("utf-8")
        yield fake_data_json + b"\n\n"
        await asyncio.sleep(random.uniform(1, 5))  

# Create an endpoint to get a continuous stream of fake data
@app.get("/generator/stream")
async def stream_fake_data():
    response = StreamingResponse(generate_fake_data_stream(), media_type="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    return response

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


#uvicorn fake_data_generator:app --reload
