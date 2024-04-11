import kuzu
import shutil


shutil.rmtree("./policyknowledgebase", ignore_errors=True)
db = kuzu.Database('./policyknowledgebase', buffer_pool_size=1024**3)
conn = kuzu.Connection(db)

#Define PolicyInsights Knowledge Graph Schema entities
conn.execute("CREATE NODE TABLE Policy(policy_id STRING, policy_title STRING, policy_description STRING, policy_state STRING, policy_legislation_type STRING, policy_content STRING, policy_rules STRING, policy_summary STRING, policy_obligations STRING, policy_risks STRING, policy_issue_date DATE, policy_approved_date DATE, policy_effective_date DATE, PRIMARY KEY (policy_id))")
conn.execute("CREATE NODE TABLE PolicyMaker(policy_maker_id STRING, policy_maker_name STRING, policy_maker_description STRING, policy_maker_type STRING, policy_jurisdiction STRING, PRIMARY KEY (policy_maker_id))")
conn.execute("CREATE NODE TABLE PolicyArea(policy_area_id STRING, policy_area_name STRING, policy_area_description STRING, PRIMARY KEY (policy_area_id))")
conn.execute("CREATE NODE TABLE PolicyStakeholder(policy_stakeholder_id STRING, policy_stakeholder_name STRING, policy_stakeholder_description STRING, policy_stakeholder_type STRING, PRIMARY KEY (policy_stakeholder_id))")


#Define PolicyInsights Knowledge Graph Schema relationships

conn.execute("CREATE REL TABLE PolicyAreaMapping(FROM Policy TO PolicyArea, mapping_strenght STRING)")
conn.execute("CREATE REL TABLE PolicyStakeholderAreaMapping(FROM PolicyStakeholder TO PolicyArea, mapping_strenght STRING)")
conn.execute("CREATE REL TABLE PolicyMakerToPolicyMapping(FROM PolicyMaker TO Policy)")