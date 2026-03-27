"""
XOMPER DynamoDB Helpers
=======================
Common DynamoDB operations with proper error handling and type hints.
"""

import boto3
from datetime import datetime, timezone
from typing import Any, Optional

from lambdas.common.constants import AWS_DEFAULT_REGION, DYNAMODB_KMS_ALIAS
from lambdas.common.errors import DynamoDBError
from lambdas.common.logger import get_logger

log = get_logger(__file__)

dynamodb_res = boto3.resource("dynamodb", region_name=AWS_DEFAULT_REGION)
dynamodb_client = boto3.client("dynamodb", region_name=AWS_DEFAULT_REGION)
kms_res = boto3.client("kms")

HANDLER = 'dynamo_helpers'


def full_table_scan(
    table_name: str,
    *,
    attribute_name_to_sort_by: Optional[str] = None,
    is_reverse: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Perform a full table scan, fetching all pages.

    Args:
        table_name: DynamoDB table name
        attribute_name_to_sort_by: Optional attribute to sort results by
        is_reverse: Sort descending if True

    Returns:
        List of all items in the table
    """
    try:
        table = dynamodb_res.Table(table_name)
        response = table.scan()
        data: list[dict[str, Any]] = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        # Support legacy kwargs pattern
        sort_key = attribute_name_to_sort_by or kwargs.get('attribute_name_to_sort_by')
        if sort_key:
            reverse = is_reverse or kwargs.get('is_reverse', False)
            data = sorted(data, key=lambda i: i[sort_key], reverse=reverse)

        return data
    except Exception as err:
        raise DynamoDBError(
            message=f"Full table scan failed: {err}",
            function="full_table_scan",
            table=table_name,
        ) from err


def table_scan_by_ids(
    table_name: str,
    key: str,
    ids: list[str],
    goal_filter: str,
    *,
    attribute_name_to_sort_by: Optional[str] = None,
    is_reverse: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Batch get items by IDs with rank extraction."""
    try:
        table = dynamodb_res.Table(table_name)
        keys = {
            table.name: {
                'Keys': [{key: id_val} for id_val in ids]
            }
        }

        response = dynamodb_res.batch_get_item(RequestItems=keys)
        data: list[dict[str, Any]] = response['Responses'][table.name]

        for offering in data:
            if len(offering.get('rank_dict', {})) > 0:
                offering['rank'] = offering['rank_dict'][goal_filter]

        sort_key = attribute_name_to_sort_by or kwargs.get('attribute_name_to_sort_by')
        if sort_key:
            reverse = is_reverse or kwargs.get('is_reverse', False)
            data = sorted(data, key=lambda i: i[sort_key], reverse=reverse)

        return data
    except Exception as err:
        raise DynamoDBError(
            message=f"Table scan by IDs failed: {err}",
            function="table_scan_by_ids",
            table=table_name,
        ) from err


def delete_table_item(
    table_name: str,
    primary_key: str,
    primary_key_value: str,
) -> dict[str, Any]:
    """Delete a single item from a table."""
    try:
        check_if_item_exist(table_name, primary_key, primary_key_value)
        table = dynamodb_res.Table(table_name)
        return table.delete_item(Key={primary_key: primary_key_value})
    except Exception as err:
        raise DynamoDBError(
            message=f"Delete table item failed: {err}",
            function="delete_table_item",
            table=table_name,
        ) from err


def update_table_item(
    table_name: str,
    table_item: dict[str, Any],
) -> dict[str, Any]:
    """Put (upsert) an entire item into a table."""
    try:
        table = dynamodb_res.Table(table_name)
        return table.put_item(Item=table_item)
    except Exception as err:
        raise DynamoDBError(
            message=f"Update table item failed: {err}",
            function="update_table_item",
            table=table_name,
        ) from err


def update_table_item_field(
    table_name: str,
    primary_key: str,
    primary_key_value: str,
    attr_key: str,
    attr_val: Any,
) -> dict[str, Any]:
    """Update a single field on an existing item."""
    try:
        check_if_item_exist(table_name, primary_key, primary_key_value)
        table = dynamodb_res.Table(table_name)
        return table.update_item(
            Key={primary_key: primary_key_value},
            UpdateExpression="set #attr_key = :attr_val",
            ExpressionAttributeValues={':attr_val': attr_val},
            ExpressionAttributeNames={'#attr_key': attr_key},
            ReturnValues="UPDATED_NEW",
        )
    except Exception as err:
        raise DynamoDBError(
            message=f"Update table item field failed: {err}",
            function="update_table_item_field",
            table=table_name,
        ) from err


def check_if_item_exist(
    table_name: str,
    id_key: str,
    id_val: str,
    override: bool = False,
) -> bool:
    """
    Check if an item exists in a table.

    Args:
        table_name: DynamoDB table name
        id_key: Partition key name
        id_val: Partition key value
        override: If True, return False instead of raising when item not found

    Returns:
        True if item exists

    Raises:
        DynamoDBError: If item does not exist and override is False
    """
    try:
        table = dynamodb_res.Table(table_name)
        response = table.get_item(Key={id_key: id_val})
        if 'Item' in response:
            return True
        elif override:
            return False
        else:
            raise DynamoDBError(
                message=f"Invalid ID ({id_val}): Item does not exist.",
                function="check_if_item_exist",
                table=table_name,
            )
    except DynamoDBError:
        raise
    except Exception as err:
        raise DynamoDBError(
            message=f"Check if item exists failed: {err}",
            function="check_if_item_exist",
            table=table_name,
        ) from err


def get_item_by_key(
    table_name: str,
    id_key: str,
    id_val: str,
) -> dict[str, Any]:
    """Get a single item by its partition key."""
    try:
        table = dynamodb_res.Table(table_name)
        response = table.get_item(Key={id_key: id_val})
        if 'Item' in response:
            return response['Item']
        raise DynamoDBError(
            message=f"Invalid ID ({id_val}): Item does not exist.",
            function="get_item_by_key",
            table=table_name,
        )
    except DynamoDBError:
        raise
    except Exception as err:
        raise DynamoDBError(
            message=f"Get item by key failed: {err}",
            function="get_item_by_key",
            table=table_name,
        ) from err


def get_item_by_multiple_keys(
    table_name: str,
    id_partition_key: str,
    id_partition_val: str,
    id_sort_key: str,
    id_sort_val: str,
) -> dict[str, Any]:
    """Get a single item by partition key + sort key."""
    try:
        table = dynamodb_res.Table(table_name)
        response = table.get_item(
            Key={
                id_partition_key: id_partition_val,
                id_sort_key: id_sort_val,
            }
        )
        item = response.get('Item')
        if item:
            log.info("Item found in table.")
            return item
        log.warning(f"Invalid IDs ({id_partition_key} - {id_sort_key}): Item does not exist.")
        return {}
    except Exception as err:
        raise DynamoDBError(
            message=f"Get item by multiple keys failed: {err}",
            function="get_item_by_multiple_keys",
            table=table_name,
        ) from err


def query_table_by_key(
    table_name: str,
    id_key: str,
    id_val: str,
    ascending: bool = False,
) -> dict[str, Any]:
    """Query a table by partition key."""
    try:
        table = dynamodb_res.Table(table_name)
        return table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key(id_key).eq(id_val),
            ScanIndexForward=ascending,
        )
    except Exception as err:
        raise DynamoDBError(
            message=f"Query table by key failed: {err}",
            function="query_table_by_key",
            table=table_name,
        ) from err


def item_has_property(item: dict[str, Any], property_name: str) -> bool:
    """Check if an item dict contains a given key."""
    return property_name in item


def empty_table(
    table_name: str,
    hash_key: str,
    hash_key_type: str,
    *,
    confirm: bool = False,
) -> dict[str, Any]:
    """
    Delete and recreate a table (destructive).

    Args:
        table_name: DynamoDB table name
        hash_key: Partition key name
        hash_key_type: Partition key type (S, N, B)
        confirm: Must be True to proceed -- safety guard against accidental calls

    Returns:
        Create table response

    Raises:
        DynamoDBError: If confirm is not True
    """
    if not confirm:
        raise DynamoDBError(
            message="empty_table requires confirm=True to execute. This is a destructive operation.",
            function="empty_table",
            table=table_name,
        )
    try:
        delete_table(table_name, confirm=True)
        return create_table(table_name, hash_key, hash_key_type)
    except Exception as err:
        raise DynamoDBError(
            message=f"Empty table failed: {err}",
            function="empty_table",
            table=table_name,
        ) from err


def delete_table(
    table_name: str,
    *,
    confirm: bool = False,
) -> dict[str, Any]:
    """
    Delete a DynamoDB table (destructive).

    Args:
        table_name: DynamoDB table name
        confirm: Must be True to proceed -- safety guard

    Raises:
        DynamoDBError: If confirm is not True
    """
    if not confirm:
        raise DynamoDBError(
            message="delete_table requires confirm=True to execute. This is a destructive operation.",
            function="delete_table",
            table=table_name,
        )
    try:
        return dynamodb_client.delete_table(TableName=table_name)
    except Exception as err:
        raise DynamoDBError(
            message=f"Delete table failed: {err}",
            function="delete_table",
            table=table_name,
        ) from err


def create_table(
    table_name: str,
    hash_key: str,
    hash_key_type: str,
) -> dict[str, Any]:
    """Create a DynamoDB table with KMS encryption and streams enabled."""
    try:
        waiter = dynamodb_client.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)

        kms_key = kms_res.describe_key(KeyId=DYNAMODB_KMS_ALIAS)

        table = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[{'AttributeName': hash_key, 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': hash_key, 'AttributeType': hash_key_type}],
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES',
            },
            SSESpecification={
                'Enabled': True,
                'SSEType': 'KMS',
                'KMSMasterKeyId': kms_key['KeyMetadata']['Arn'],
            },
            BillingMode='PAY_PER_REQUEST',
        )

        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        return table
    except Exception as err:
        raise DynamoDBError(
            message=f"Create table failed: {err}",
            function="create_table",
            table=table_name,
        ) from err


def batch_write_table_items(table_name: str, db_items: dict[str, Any]) -> str:
    """Batch write items to a DynamoDB table."""
    try:
        table = dynamodb_res.Table(table_name)
        with table.batch_writer() as batch:
            for player_id, player_data in db_items.items():
                batch.put_item(
                    Item={
                        'player_id': player_id,
                        'data': player_data,
                        'last_updated': datetime.now(timezone.utc).isoformat(),
                    }
                )
        msg = f"Updated {len(db_items)} items in DynamoDB table {table_name}."
        log.info(msg)
        return msg
    except Exception as err:
        raise DynamoDBError(
            message=f"Batch write table items failed: {err}",
            function="batch_write_table_items",
            table=table_name,
        ) from err


# ---------------------------------------------------------------------------
# Backward Compatibility Aliases (deprecated -- use new names)
# ---------------------------------------------------------------------------
emptyTable = empty_table
deleteTable = delete_table
createTable = create_table
