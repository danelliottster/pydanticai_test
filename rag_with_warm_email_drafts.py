import json
from pydantic_ai import Agent , Tool , ModelRetry
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from pydantic_ai import RunContext
import logfire

logfire.configure(token="pylf_v1_us_3VCs49q8Gxp5971kyVw9ygkhg4BFPws76KX05GsQhjGR" , scrubbing=False , environment="agentic_test")

with open("/home/dane/gemini_key.txt", "r") as fh:
    gemini_key = fh.read().strip()

gemini_deep = GeminiModel(
    'gemini-2.5-pro-preview-05-06', provider=GoogleGLAProvider(api_key=gemini_key)
    )

gemini_shallow = GeminiModel(
    'gemini-2.0-flash', provider=GoogleGLAProvider(api_key=gemini_key), retry=ModelRetry(
        max_retries=3,
        retry_on_error=True,
        retry_on_rate_limit=True,
        retry_on_timeout=True
    )
)


########################################################################################################################################
# define the main agent and available tools
main_agent = Agent(
    model,
    system_prompt=(
        "You are a sales assistant for Sterling Computer Corporation, a value added reseller which sells computer hardware and software."
        "You assist the account executive by: answering general questions about the services provided Sterling and drafting emails to customers."
        "You have access to a knowledge base of information about Sterling Computer Corporation, including descriptions of the services provided, call scripts provided by the marketing department, and a database of past sales for each customer."
    )
)
main_agent.instrument_all()

###############################################################################################################################
# define the warm email drafts agent and available tools
warm_email_drafts_agent = Agent(
    model,
    system_prompt=(
        "You are a sales assistant for Sterling Computer Corporation, a value added reseller which sells computer hardware and software."
        "You assist the account executive by drafting warm email drafts to customers."
        "You have access to a knowledge base of information about Sterling Computer Corporation, including descriptions of the services provided, call scripts provided by the marketing department, and a database of past sales for each customer."
    )
)