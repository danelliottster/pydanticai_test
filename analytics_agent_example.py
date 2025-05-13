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
import duckdb
from dataclasses import dataclass
import json
from tabulate import tabulate
from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from pydantic_ai.providers.google_gla import GoogleGLAProvider
from pydantic_ai import RunContext


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
opps_data_df["revenue"] = opps_data_df["revenue"].str.replace("$", "", regex=False)  # Remove dollar sign
opps_data_df["revenue"] = opps_data_df["revenue"].str.replace(",", "", regex=False)  # Remove commas
opps_data_df["revenue"] = opps_data_df["revenue"].astype(float)  # Convert to float


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
        "description": "Some basic information about the customers Sterling sells to. This includes the name of the customer, the name of the account executive associated with the customer, and the business vertical of the customer.",
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
    "opps_data_df": {
        "dataframe": "opps_data_df",
        "description": "Information about the opportunities Sterling has sold to customers.  This includes dates, the sales order number, the date of the sales order, the quarter and year of the opportunity, the name of the opportunity, the enterprise product associated with the opportunity if applicable, the first level category of the product(s) associated with the opportunity, the revenue associated with the opportunity, and the margin associated with the opportunity.",
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

# define the main agent
main_agent = Agent(
    model,
    system_prompt=(
        "You are a data analyst and a friendly coworker."
        "Use query_detection_tool to find one or more queries which can be answered using our data lakehouse and return the associated context."
        "If the user is not asking about data, you can also answer general questions about life, the universe, and everything."
    )
)
import logfire
logfire.configure(token="pylf_v1_us_3VCs49q8Gxp5971kyVw9ygkhg4BFPws76KX05GsQhjGR")
main_agent.instrument_all()

sql_generation_agent = Agent( model , output_type=str ,
                              system_prompt=(
                                  "Your job is to convert the queries into SQL statements which could be used to query the data tables described in the data catalog."
                                  "You will be given a query and you should return the SQL statement which could be used to answer the query."
                                  "Your response should only include the SQL statement and should not include any text formatting."
                                  "You will also be given the data catalog which describes the data tables and their columns."
                                  "If you cannot find any queries, return an empty list. "
                                )
)

@dataclass
class QueryResult:
    query: str
    sql: str
    output: pd.DataFrame = None
    summary: str = None

def prep_sql_string( sql_in ) :
    """Prepare the SQL string for execution."""
    # remove newlines and extra spaces
    sql_out = sql_in.replace("\n", " ")
    sql_out = sql_out.replace("```", " ")
    sql_out = sql_out.replace("sql", " ")
    sql_out = sql_out.strip()
    sql_out = " ".join(sql_out.split())
    return sql_out

@main_agent.tool
async def query_detection_tool( ctx: RunContext[str] , prompt: str ) -> list[dict[str,str]]:
    """Find one or more queries within the prompt which can be answered by the data tables.  Answer the queries using the data tables and return the results."""
    r = await query_detection_agent.run(
        user_prompt=prompt,
        deps=prompt
    )

    tool_response = []
    ### iterate over the discovered queries
    for query_i , query in enumerate(r.output):
        if query_i > 0:
            tool_response += "\n\n"
        # Generate SQL for the query
        print(f"Generating SQL for query: {query}")
        sql = await sql_generation_agent.run(
            user_prompt="Using the following data catalog:\n" + json.dumps(data_catalog) + "\n\n return the SQL needed to answer the following query: " + query,
            deps=query
        )
        prepped_sql = prep_sql_string(sql.output)
        print(f"Generated SQL: {sql.output}")
        print(f"Prepped SQL: {prepped_sql}")
        # execute the SQL and get the resulting table
        result = duckdb.query(prepped_sql).to_df()
        # summarize the table
        # if the result has one cell, return the cell's value
        if result.shape == (1, 1):
            summary = str( result.iloc[0, 0] )
        # if the result has one row, return only the summary which is each column header followed by the value
        elif result.shape[0] == 1:
            summary = ""
            for coli , col in enumerate(result.columns):
                summary += str( col + ": " + str(result.iloc[0, coli]) )
        # if the result has more than one row, return the table and the summary
        elif result.shape[0] > 1:
            # summary = tabulate(result, headers="keys", tablefmt="html")
            summary = result.to_html()
        
        tool_response += [{"query": query, "summary": summary}]

    return tool_response

# Define the agent to find one or more queries within a prompt which can be answered by the data tables
query_detection_agent = Agent( model , output_type=list[str] , 
                              system_prompt=(
                                "Use get_catalog_detect to get the data catalog for use in determining what queries can be answered. Only call this function if you need to and never more than once. "
                                  "Your job is to find and refine one or more natural language queries within the prompt which can be answered by the data tables described in the data catalog. "
                                  "You should return the queries in a list format. "
                                  "Your response should only include the queries, not any other text. "
                                  "If you cannot find any queries, return an empty list. "
                                  "The queries should be in the form of a question."
                                  "Ensure that the queries are clear and concise."
                                ),
)

@query_detection_agent.tool_plain
async def get_catalog_detect(ctx: RunContext[None]) -> str:
    """Get the data catalog."""
    return json.dumps(data_catalog)

# result = main_agent.run_sync( "When was the USA founded?" )
# result = main_agent.run_sync( "Who is the account executive for the customer 557th Weather Wing?" )
# result = main_agent.run_sync( "What is the revenue for the customer 557th Weather Wing in 2023?" )
# result = main_agent.run_sync( "What is the revenue for the customer 557th Weather Wing in 2023 and who is the account executive?" )
result = main_agent.run_sync( "What is the total revenue for all customers in the army vertical in 2024?" ) #this one is whack!
# result = main_agent.run_sync( "What is the total revenue for all customers in the army vertical?" )
print(result.output)
print(result.all_messages())