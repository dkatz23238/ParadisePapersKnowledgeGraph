import pandas as pd
from grakn.client import GraknClient


def clean_table(df):
    df["n.name"] = df["n.name"].str.replace('"', '')
    return df


def relationship_template(rel):
    '''Creates gql relationship insert'''
    insert_gql = "match $n1 isa thing, has node-id " + rel["node_1"] + ";"
    insert_gql += "$n2 isa thing, has node-id " + rel["node_2"] + ";"
    return insert_gql


REL_TYPES = [
    'registered_address', 'officer_of', 'connected_to', 'intermediary_of',
    'same_name_as', 'same_id_as'
]

relationships_df = pd.read_csv("./data/paradise_papers.edges.csv").astype(str)

with GraknClient(uri='127.0.0.1:48555') as client:
    with client.session(keyspace="paradise") as session:
        for rel_type in REL_TYPES:

            df = relationships_df[relationships_df["rel_type"] == rel_type]

            if rel_type == "registered_address":
                for row in df.iterrows():
                    with session.transaction().write() as transaction:
                        rel = row[1]
                        query = relationship_template(rel)
                        query += "insert $registration (registrant: $n1, registered: $n2) isa registration;"
                        print("Inserting Query:")
                        print(query)
                        transaction.query(query)
                        transaction.commit()
                        print("\nInsert Complete!\n")

            if rel_type == "officer_of":
                for row in df.iterrows():
                    with session.transaction().write() as transaction:
                        rel = row[1]
                        query = relationship_template(rel)
                        query += "insert $employement (employer: $n1, employee: $n2) isa employement;"
                        print("Inserting Query:")
                        print(query)
                        transaction.query(query)
                        transaction.commit()
                        print("\nInsert Complete!\n")

            if rel_type == "connected_to":
                for row in df.iterrows():
                    with session.transaction().write() as transaction:
                        rel = row[1]
                        query = relationship_template(rel)
                        query += "insert $connection (x: $n1, y: $n2) isa connection;"
                        print("Inserting Query:")
                        print(query)
                        transaction.query(query)
                        transaction.commit()
                        print("\nInsert Complete!\n")

            if rel_type == "intermediary_of":
                for row in df.iterrows():
                    with session.transaction().write() as transaction:
                        rel = row[1]
                        query = relationship_template(rel)
                        query += "insert $intermediaryship (x: $n1, y: $n2) isa intermediaryship;"
                        print("Inserting Query:")
                        print(query)
                        transaction.query(query)
                        transaction.commit()
                        print("\nInsert Complete!\n")
