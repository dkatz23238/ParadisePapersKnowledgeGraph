import pandas as pd
from grakn.client import GraknClient


def clean_table(df):
    df["n.name"] = df["n.name"].str.replace('"', '')
    return df


def financial_entity_template(entity, insert_type="financial-entity"):
    '''Creates gql query for inserting a financial-entity.'''

    insert_gql = "insert $" + insert_type + " isa " + insert_type
    insert_gql += ", has node-id " + str(entity["n.node_id"])
    insert_gql += ', has name "' + entity["n.name"] + '"'

    if not pd.isnull(entity["n.country_codes"]):
        insert_gql += ', has country-code "' + entity["n.country_codes"] + '"'

    if not pd.isnull(entity["n.jurisdiction_description"]):
        insert_gql += ', has jurisdiction "' + entity[
            "n.jurisdiction_description"] + '"'

    dt = entity["n.incorporation_date"]
    if not pd.isnull(dt):
        insert_gql += ", has start-date " + dt.replace(
            microsecond=0).isoformat()

    insert_gql += ';'

    return insert_gql


def address_entity_template(entity):
    '''Creates gql query for inserting an address-entity.'''
    insert_gql = "insert $address-entity isa address-entity"
    insert_gql += ", has node-id " + str(entity["n.node_id"])
    insert_gql += ', has name "' + entity["n.name"] + '"'

    if not pd.isnull(entity["n.country_codes"]):
        insert_gql += ', has country-code "' + entity["n.country_codes"] + '"'

    if not pd.isnull(entity["n.jurisdiction_description"]):
        insert_gql += ', has jurisdiction "' + entity[
            "n.jurisdiction_description"] + '"'
    insert_gql += ';'

    return insert_gql


def officer_entity_template(entity):
    '''Creates gql query for inserting an address-entity.'''
    insert_gql = "insert $officer isa officer"
    insert_gql += ", has node-id " + str(entity["n.node_id"])
    insert_gql += ', has name "' + entity["n.name"] + '"'

    if not pd.isnull(entity["n.country_codes"]):
        insert_gql += ', has country-code "' + entity["n.country_codes"] + '"'

    if not pd.isnull(entity["n.jurisdiction_description"]):
        insert_gql += ', has jurisdiction "' + entity[
            "n.jurisdiction_description"] + '"'
    insert_gql += ';'

    return insert_gql


# Import data
entities_df = clean_table(
    pd.read_csv("./data/paradise_papers.nodes.entity.csv"))
address_df = clean_table(
    pd.read_csv("./data/paradise_papers.nodes.address.csv").dropna(
        subset=['n.name']))
intermediary_df = clean_table(
    pd.read_csv("./data/paradise_papers.nodes.intermediary.csv"))
officers_df = clean_table(
    pd.read_csv("./data/paradise_papers.nodes.officer.csv"))
others_df = clean_table(pd.read_csv("./data/paradise_papers.nodes.other.csv"))

entities_df["n.incorporation_date"] = pd.to_datetime(
    entities_df["n.incorporation_date"], errors="coerce")

with GraknClient(uri='127.0.0.1:48555') as client:
    with client.session(keyspace="paradise") as session:
        # Entities
        for row in entities_df.iterrows():
            with session.transaction().write() as transaction:
                ent = row[1]
                query = financial_entity_template(ent)
                print("Inserting Query:")
                print(query)
                transaction.query(query)
                transaction.commit()
                print("\nInsert Complete!\n")

        # Intermediary
        for row in intermediary_df.iterrows():
            with session.transaction().write() as transaction:
                ent = row[1]
                query = financial_entity_template(ent,
                                                  insert_type="intermediary")
                print("Inserting Query:")
                print(query)
                transaction.query(query)
                transaction.commit()
                print("\nInsert Complete!\n")
        # Others
        for row in others_df.iterrows():
            with session.transaction().write() as transaction:
                ent = row[1]
                query = financial_entity_template(ent, insert_type="other")
                print("Inserting Query:")
                print(query)
                transaction.query(query)
                transaction.commit()
                print("\nInsert Complete!\n")
        # Addresses
        for row in address_df.iterrows():
            with session.transaction().write() as transaction:
                ent = row[1]
                query = address_entity_template(ent)
                print("Inserting Query:")
                print(query)
                transaction.query(query)
                transaction.commit()
                print("\nInsert Complete!\n")
        # Officers
        for row in officers_df.iterrows():
            with session.transaction().write() as transaction:
                ent = row[1]
                query = officer_entity_template(ent)
                print("Inserting Query: ")
                print(query)
                transaction.query(query)
                transaction.commit()
                print("\nInsert Complete!\n")
