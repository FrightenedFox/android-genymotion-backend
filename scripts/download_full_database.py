import csv
import decimal
import json

import boto3


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON Encoder that converts Decimal types to float."""

    def default(self, o) -> float:
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def download_dynamodb_data(table_name):
    """
    Connects to DynamoDB and downloads all data from the specified table.
    Handles pagination using LastEvaluatedKey.
    """
    # Initialize DynamoDB resource
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    data = []
    response = table.scan()
    data.extend(response.get("Items", []))

    # Continue scanning if there are more items (pagination)
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response.get("Items", []))

    return data


def save_as_json(data, filename):
    """Saves the data to a JSON file."""
    with open(filename, "w") as f:
        json.dump(data, f, indent=4, cls=DecimalEncoder)
    print(f"Data saved as JSON in {filename}")


def save_as_csv(data, filename):
    """Saves the data to a CSV file.
    This assumes that all items have the same set of keys.
    """
    if not data:
        print("No data found!")
        return

    # Compute the union of all keys in the dataset
    headers = set()
    for item in data:
        headers.update(item.keys())
    headers = list(headers)

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved as CSV in {filename}")


def main():
    table_name = "android-project-db"

    # Download the data from DynamoDB
    data = download_dynamodb_data(table_name)

    # Save data in your preferred format(s)
    save_as_json(data, "dynamodb_data.json")
    save_as_csv(data, "dynamodb_data.csv")


if __name__ == "__main__":
    main()
