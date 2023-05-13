#!/usr/bin/python3

import json
import os
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
from azure.cosmos import cosmos_client, exceptions
import boto3
from botocore.exceptions import ClientError


def get_aws_secret(secret_name: str, region_name: str):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)


def extract_from_cosmosdb(param: dict) -> pd.DataFrame:
    # Connect to CosmosDB / TruckTicketing
    secret = get_aws_secret(secret_name='prod/cosmos-db/TruckTicketing', region_name='ca-central-1')
    client = cosmos_client.CosmosClient(url=secret['endpoint'], credential=secret['credential'], consistency_level='Eventual')
    database = client.get_database_client(secret['database'])

    container = get_container(database, param["SourceContainer"])
    print( f"SourceContainer/Entity: {param['SourceContainer']}/{param['SourceEntity']}")
    entity_items = query_items(
        container,
        f"SELECT * from {param['SourceContainer']} c "
        f"WHERE c.EntityType = '{param['SourceEntity']}' "
        f"AND c.{param['HWMColumn']} > {param['HWMValue']}",
    )

    #entity_df = pd.DataFrame(entity_items)
    return entity_items



def get_container(db, container_name) -> None:
    try:
        container = db.get_container_client(container_name)
        return container
    except exceptions.CosmosResourceNotFoundError:
        logger.error(f"Container {container_name} not found!")
        raise
    except exceptions.CosmosHttpResponseError:
        raise



def query_items(container, query_text: str) -> list:
    """
    Retrieve items from the provided CosmosDB container that match the query.
    Returns list of items.
    Side Effect: Logs the cost of the query.
    """
    try:
        response = container.query_items(
            query=query_text, enable_cross_partition_query=True
        )
    except exceptions.CosmosHttpResponseError:
        print(f"query_items failed: {container}\n{query_text}\n")
        raise
    items = list(response)
    request_charge = container.client_connection.last_response_headers[
        "x-ms-request-charge"
    ]
    print(f"Returned {len(items):10} rows @ {request_charge:10} request units.")
    return items




if __name__ == '__main__':
    source_entities_list = [
        {"Container": "Operations", "Entity": "Account"},
        {"Container": "Operations", "Entity": "AccountContactIndex"},
        {"Container": "Operations", "Entity": "AccountContactReferenceIndex"},
        {"Container": "Operations", "Entity": "BusinessStream"},
        {"Container": "Operations", "Entity": "TicketType"},
        {"Container": "Operations", "Entity": "AdditionalServicesConfiguration"},
        {"Container": "Operations", "Entity": "ChangeTracking"},
        {"Container": "Operations", "Entity": "EmailTemplateEvent"},
        {"Container": "Operations", "Entity": "FacilityService"},
        {"Container": "Operations", "Entity": "FacilityServiceSubstanceIndex"},
        {"Container": "Operations", "Entity": "Note"},
        {"Container": "Operations", "Entity": "EmailTemplate"},
#        {"Container": "Operations", "Entity": "Facility"},
        {"Container": "Operations", "Entity": "LandfillSampling"},
        {"Container": "Operations", "Entity": "LandfillSamplingRule"},
        {"Container": "Operations", "Entity": "Invoice"},
        {"Container": "Operations", "Entity": "LegalEntity"},
        {"Container": "Operations", "Entity": "LoadConfirmation"},
        {"Container": "Operations", "Entity": "TruckTicketHoldReason"},
        {"Container": "Operations", "Entity": "Sequence"},
        {"Container": "Operations", "Entity": "TruckTicketTareWeightIndex"},
        {"Container": "Operations", "Entity": "MaterialApproval"},
        {"Container": "Operations", "Entity": "SalesLine"},
        {"Container": "Operations", "Entity": "TruckTicketVoidReason"},
        {"Container": "Operations", "Entity": "ServiceType"},
        {"Container": "Operations", "Entity": "SourceLocation"},
        {"Container": "Operations", "Entity": "SourceLocationType"},
        {"Container": "Operations", "Entity": "TradeAgreementUpload"},
        {"Container": "Operations", "Entity": "TruckTicket"},
        {"Container": "Operations", "Entity": "TruckTicketWellClassification"},
        {"Container": "Operations", "Entity": "VolumeChange"},
        {"Container": "Accounts", "Entity": "Permission"},
        {"Container": "Accounts", "Entity": "Role"},
        {"Container": "Accounts", "Entity": "NavigationConfiguration"},
        {"Container": "Accounts", "Entity": "UserProfile"},
        {"Container": "Billing", "Entity": "InvoiceConfigurationPermutation"},
        {"Container": "Billing", "Entity": "EDIFieldDefinition"},
        {"Container": "Billing", "Entity": "EDIFieldLookup"},
        {"Container": "Billing", "Entity": "EDIValidationPatternLookup"},
        {"Container": "Billing", "Entity": "BillingConfiguration"},
        {"Container": "Billing", "Entity": "InvoiceConfiguration"},
        {"Container": "Pricing", "Entity": "PricingRule"},
        {"Container": "Products", "Entity": "Product"},
        {"Container": "Products", "Entity": "SpartanProductParameter"},
        {"Container": "Products", "Entity": "Substance"},
    ]

    for entity in source_entities_list:
        data = {
            "SourceContainer": entity['Container'],
            "SourceEntity": entity['Entity'],
            "HWMColumn": "_ts",
            "HWMValue": "0"
        }

        entity_items = extract_from_cosmosdb(param=data)
        df = pd.DataFrame.from_dict(entity_items)

        df.to_csv(f"data/{data['SourceEntity']}.csv", index=False)
        df.to_parquet(f"data/{data['SourceEntity']}.parquet", compression='gzip', index=False)


