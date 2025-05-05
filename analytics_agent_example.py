"""
* Load the data
* Define a data catalog
* Define the following agents:
  * an agent to find one or more queries within a prompt which can be answered by the data tables
  * an agent which finds one or more requests for graphics generation within the prompt
  * an agent which generates sql for a given query
  * an agent which generates a graphic for a given request
* Define a main agent which uses the above agents to answer the prompt
* Define a main function which runs the main agent
"""

import pandas as pd
from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from pydantic_ai import RunContext
import json


###############################################################################
# Load the data
sales_by_agency_df = pd.read_csv("/home/dane/agentic_test/Sales by Agency.csv" , encoding="cp1252")
sales_by_agency_df.columns = sales_by_agency_df.columns.str.replace(" ", "_")
sales_by_agency_df.columns = sales_by_agency_df.columns.str.lower()
sales_by_agency_df.columns = sales_by_agency_df.columns.str.replace("-", "_")

opps_data_df = pd.read_csv("/home/dane/agentic_test/Opps Data.csv" , encoding="cp1252")
opps_data_df.columns = opps_data_df.columns.str.replace(" ", "_")
opps_data_df.columns = opps_data_df.columns.str.lower()
opps_data_df.columns = opps_data_df.columns.str.replace("-", "_")
opps_data_df.columns = opps_data_df.columns.str.replace(".", "_")

# drop duplicate agency names
agencies_df = sales_by_agency_df[["ordering_gvt_agency", "ordering_agency_owner", "govt_agency_roll_up_vertical"]]
agencies_df = agencies_df.drop_duplicates(subset=["ordering_gvt_agency"])
agencies_df = agencies_df.reset_index(drop=True)

# Split the Quarter-Year column into two columns: Quarter and Year
opps_data_df[['quarter', 'year']] = opps_data_df['quarter_year'].str.split('-', expand=True)
opps_data_df['quarter'] = opps_data_df['quarter'].str.replace('Q', '')
opps_data_df['quarter'] = opps_data_df['quarter'].astype(int)
opps_data_df['year'] = opps_data_df['year'].astype(int)

# drop some columns from opps_data_df
opps_data_df = opps_data_df.drop(columns=["quarter_year","deal_reg_y/n"])

#############################################################################
# Define a data catalog
data_catalog = {
    "agencies_df": {
        "dataframe": "agencies_df",
        "description": "Some basic information about the customers Sterling sells to.",
        "columns": {
            "ordering_gvt_agency": {
                "description": "The name of customer",
                "type": "string"
            },
            "ordering_agency_owner": {
                "description": "The name of the account executive associated with the customer.", 
                "type": "string"
            },
            "govt_agency_roll_up_vertical": {
                "description": "The business vertical of the customer.",
                "type": "string"
            }
        }
    },
    "opps_data": {
        "dataframe": "opps_data_df",
        "description": "Opportunities data",
        "columns": {
            "sales_order_number": {
                "description": "The sales order number",
                "type": "string"
            },
            "order_date": {
                "description": "The date of the sales order",
                "type": "datetime"
            },
            "quarter": {
                "description": "The quarter of the opportunity",
                "type": "integer"
            },
            "year": {
                "description": "The year of the opportunity",
                "type": "integer"
            },
            "opportunity_name": {
                "description": "The name of the opportunity",
                "type": "string"
            },
            "enterprise_product": {
                "description": "The name of the enterprise product associated with the opportunity if applicable",
                "type": "string"
            },
            "primary_product_category": {
                "description": "The first level category of the product(s) associated with the opportunity",
                "type": "string"
            },
            "revenue": {
                "description": "The revenue associated with the opportunity",
                "type": "float"
            },
            "margin": {
                "description": "The margin associated with the opportunity",
                "type": "float"
            },
            "ordering_gvt_agency": {
                "description": "The name of the customer associated with the opportunity",
                "type": "string"
            },
            "si_account": {
                "description": "The name of the system integrator associated with the opportunity",
                "type": "string"
            }
        }
    }
}

#############################################################################
# Define the agents
with open("/home/dane/gemini_key.txt", "r") as fh:
    gemini_key = fh.read().strip()

model = GeminiModel(
    'gemini-2.0-flash', provider=GoogleGLAProvider(api_key=gemini_key)
    )

# # define the main agent
# main_agent = Agent(
#     model,
#     system_prompt=(
#         "You are a data analyst and a friendly coworker."
#         "Use query_detection_tool to find one or more queries within the prompt."
#         "Return the queries to the user but do not answer them yourself."
#         "If the user is not asking about data, you can also answer general questions about life, the universe, and everything."
#     )
# )

# define the main agent
main_agent = Agent(
    model,
    system_prompt=(
        "You are a data analyst and a friendly coworker."
        "Use sql_generation_tool to find one or more queries within the prompt and return the associated SQL."
        "If there are multiple queries, return the SQL in different code blocks."
        "Return the queries to the user but do not execute them yourself."
        "If the user is not asking about data, you can also answer general questions about life, the universe, and everything."
    )
)

@main_agent.tool
async def sql_generation_tool(ctx: RunContext[str], prompt: str) -> str:
    """Generate SQL block or blocks for the given query if applicable."""
    r = await sql_generation_agent.run(
        user_prompt=prompt,
        deps=prompt
    )
    return r.output

sql_generation_agent = Agent( model , output_type=str ,
                              system_prompt=(
                                  "Use get_catalog_sql_gen to get the data catalog. "
                                  "Use the query_detection_tool to find one or more queries within the prompt which can be answered by the data tables described in the data catalog. "
                                  "Your job is to convert the queries into SQL statements. "
                                  "If you cannot find any queries, return an empty list. "
                                ),
)

@sql_generation_agent.tool
async def query_detection_tool( ctx: RunContext[str] , prompt: str ) -> list[str]:
    r = await query_detection_agent.run(
        user_prompt=prompt,
        deps=prompt
    )
    return r.output

@sql_generation_agent.tool
async def get_catalog_sql_gen(ctx: RunContext[None]) -> str:
    """Get the data catalog."""
    return json.dumps(data_catalog)

# Define the agent to find one or more queries within a prompt which can be answered by the data tables
query_detection_agent = Agent( model , output_type=list[str] , 
                              system_prompt=(
                                  "Use get_catalog_detect to get the data catalog. "                                
                                  "Your job is to find one or more queries within the prompt which can be answered by the data tables described in the data catalog. "
                                  "You should return the queries in a list format. "
                                  "Your response should only include the queries, not any other text. "
                                  "If you cannot find any queries, return an empty list. "
                                  "The queries should be in the form of a question. "
                                  "Ensure that the queries are clear and concise."
                                ),
)

@query_detection_agent.tool
async def get_catalog_detect(ctx: RunContext[None]) -> str:
    """Get the data catalog."""
    return json.dumps(data_catalog)

# result = main_agent.run_sync( "When was the USA founded?" )
# result = main_agent.run_sync( "Who is the account executive for the customer 557th Weather Wing?" )
# result = main_agent.run_sync( "What is the revenue for the customer 557th Weather Wing in 2023?" )
result = main_agent.run_sync( "What is the revenue for the customer 557th Weather Wing in 2023 and who is the account executive?" )
print(result.output)
print(result.all_messages())