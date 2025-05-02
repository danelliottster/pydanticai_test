import random
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from pydantic_ai import Agent, RunContext

with open("/home/dane/gemini_key.txt", "r") as fh:
    gemini_key = fh.read().strip()

model = GeminiModel(
    'gemini-2.0-flash', provider=GoogleGLAProvider(api_key=gemini_key)
    )

agent = Agent(
    model,
    deps_type=str,  
    system_prompt=(
        "You're a dice game, you should roll the die and see if the number "
        "you get back matches the user's guess. If so, tell them they're a winner. "
        "Use the player's name in the response."
    ),
)


@agent.tool_plain  
def roll_die() -> str:
    """Roll a six-sided die and return the result."""
    return str(random.randint(1, 6))


@agent.tool  
def get_player_name(ctx: RunContext[str]) -> str:
    """Get the player's name."""
    return ctx.deps


dice_result = agent.run_sync('Is it 1', deps='Dan')  
print(dice_result.output)
print(dice_result.all_messages())
#> Congratulations Anne, you guessed correctly! You're a winner!