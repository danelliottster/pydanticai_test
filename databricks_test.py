from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for query in w.queries.list():
    print(f'query {query.display_name} was created at {query.create_time}')

for cat in w.catalogs.list():
    print(f'catalog {cat.full_name} owned by {cat.owner}')

for tables in w.tables.list(catalog_name="gold" , schema_name="default"):
    print(f'table {tables.full_name} owned by {tables.owner}')