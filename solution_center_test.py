from pydantic import BaseModel

from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider

class CityLocation(BaseModel):
    city: str
    country: str


ollama_model = OpenAIModel(
    model_name='nvidia/llama-3.3-nemotron-super-49b-v1', provider=OpenAIProvider(base_url='http://10.151.99.50:8999/v1')
)
agent = Agent(ollama_model, output_type=CityLocation)

result = agent.run_sync('Where were the olympics held in 2012?')
print(result.output)
print(result.usage())