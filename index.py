from boto3 import resource
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime

demo_table = resource('dynamodb').Table('demo-dynamo-python')

def insert():
    print(f'demo-insert')
    response = demo_table.put_item(
        Item={
            'customer_id':'cus-08', # partition key (primary key-mandatory)
            'order_id':'ord-4', # sort key (mandatory)
            'status':'pending',
            'create_date': datetime.now().isoformat()
        }
    )
    print(f' Insert response: {response}')


def select_scan():
    print(f'demo_select_scan')
    filter_expression = Attr('status').eq('pending')

    item_list = []
    dynamo_response = {'LastEvaluatedKey':False}
    while 'LastEvaluatedKey' in dynamo_response:
        if dynamo_response['LastEvaluatedKey']:
            dynamo_response=demo_table.scan(
                FilterExpression=filter_expression,
                ExclusiveStartKey=dynamo_response['LastEvaluatedKey']
                )
            print(f'response-if:{dynamo_response}')
        else:
            dynamo_response = demo_table.scan(
                FilterExpression=filter_expression,
            )
            print(f'response-else{dynamo_response}')

        for item in dynamo_response['Items']:
            item_list.append(item)

    print(f'Number of input task to process: {len(item_list)}')
    for item in item_list:
        print(f'Item: {item}')


select_scan()


# insert()