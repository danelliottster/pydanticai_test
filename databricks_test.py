from databricks.sdk import WorkspaceClient, service
# from databricks.sdk.workspace.dashboards.genie import Genie
w = WorkspaceClient()
for query in w.queries.list():
    print(f'query {query.display_name} was created at {query.create_time}')

for cat in w.catalogs.list():
    print(f'catalog {cat.full_name} owned by {cat.owner}')

for tables in w.tables.list(catalog_name="gold" , schema_name="default"):
    print(f'table {tables.full_name} owned by {tables.owner}')

for col in w.tables.get("gold.default.opportunities_closed_won").columns:
    print(f'column {col.name} of type {col.type_text} is {col.comment}')

for genies in w.genie.list_spaces().spaces:
    print(f'genie space {genies.title} with id {genies.space_id}')


blah = w.genie.start_conversation_and_wait(space_id="01f046f0ad65171c81f6682c6b9359aa" , content="what opportunities has  justin.west@sterling.com sold to diamond plastics?")
blah_result = w.genie.get_message_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb',message_id="01f046f93f4719d7bd889bab3e351c19")
blah_attachment = w.genie.get_message_attachment_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb',message_id="01f046f93f4719d7bd889bab3e351c19", attachment_id="01f046f9407b148f897b34eb54d1595a")

blah2 = w.genie.create_message_and_wait(space_id="01f046f0ad65171c81f6682c6b9359aa",  conversation_id='01f046f93f37123e810b7b1e60bb92eb', content="what opportunities has  justin.west@sterling.com sold to diamond plastics?")
blah2_attachment = w.genie.get_message_attachment_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb', message_id="01f047b682da1fafa5d01fbe7bbb1895", attachment_id="01f047b683fc170390fe5160603b26bd")

blah3 = w.genie.create_message_and_wait(space_id="01f046f0ad65171c81f6682c6b9359aa",  conversation_id='01f046f93f37123e810b7b1e60bb92eb', content="how many opportunities has are there?")
blah3_attachment = w.genie.get_message_attachment_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb', message_id="01f047b6c3221e62b6ce1966cf7ab327", attachment_id="01f047b6c4801cd284e89e6698b7d268")

blah4 = w.genie.create_message_and_wait(space_id="01f046f0ad65171c81f6682c6b9359aa",  conversation_id='01f046f93f37123e810b7b1e60bb92eb', content="how many opportunities has  justin.west@sterling.com sold to diamond plastics?")
blah4_attachment = w.genie.get_message_attachment_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb', message_id="01f047b785a5124ea90fc1794132b255", attachment_id="01f047b7875019f0b1f89966076da878")

blah5 = w.genie.create_message_and_wait(space_id="01f046f0ad65171c81f6682c6b9359aa",  conversation_id='01f046f93f37123e810b7b1e60bb92eb', content="what is the total revenue from opportunities owned by  justin.west@sterling.com sold to diamond plastics?")
blah5_attachment = w.genie.get_message_attachment_query_result(space_id="01f046f0ad65171c81f6682c6b9359aa", conversation_id='01f046f93f37123e810b7b1e60bb92eb', message_id="01f047c9f39e1cad9f50c59154b387c1", attachment_id="01f047c9f57119018f33f470045ffbd9")