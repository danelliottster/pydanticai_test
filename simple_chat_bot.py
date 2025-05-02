from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider

with open("/home/dane/gemini_key.txt", "r") as fh:
    gemini_key = fh.read().strip()

model = GeminiModel(
    'gemini-2.0-flash', provider=GoogleGLAProvider(api_key=gemini_key)
    )
agent = Agent(model)

while True:

    try:
        user_input = input("User: ")
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        response = agent.run_sync(user_input)
        print("Assistant:", response.output)
    except Exception as e:
        print("Error:", e)